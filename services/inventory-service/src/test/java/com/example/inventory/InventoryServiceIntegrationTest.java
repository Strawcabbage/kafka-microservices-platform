package com.example.inventory;

import com.example.events.OrderCreatedEvent;
import com.example.inventory.config.KafkaConsumerConfig;
import com.example.inventory.domain.InventoryItem;
import com.example.inventory.repository.InventoryItemRepository;
import com.example.inventory.repository.InventoryReservationRepository;
import com.example.inventory.repository.ProcessedEventRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@SpringBootTest
@ActiveProfiles("test")
@Testcontainers
class InventoryServiceIntegrationTest {

    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>(DockerImageName.parse("postgres:15"))
            .withDatabaseName("inventory")
            .withUsername("test")
            .withPassword("test");

    @Container
    static KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.5.0"));

    @Autowired
    private InventoryItemRepository inventoryItemRepository;

    @Autowired
    private InventoryReservationRepository reservationRepository;

    @Autowired
    private ProcessedEventRepository processedEventRepository;

    @Autowired
    private ObjectMapper objectMapper;

    private KafkaTemplate<String, Object> kafkaTemplate;

    @DynamicPropertySource
    static void configureProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
        registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
    }

    @BeforeAll
    static void setUpTopics() {
        Map<String, Object> adminProps = new HashMap<>();
        adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());

        try (AdminClient adminClient = AdminClient.create(adminProps)) {
            List<NewTopic> topics = List.of(
                    new NewTopic(KafkaConsumerConfig.ORDERS_TOPIC, 3, (short) 1),
                    new NewTopic(KafkaConsumerConfig.ORDERS_DLQ_TOPIC, 3, (short) 1),
                    new NewTopic("inventory.v1", 3, (short) 1)
            );
            adminClient.createTopics(topics);
        }
    }

    @BeforeEach
    void setUp() {
        // Clear test data
        processedEventRepository.deleteAll();
        reservationRepository.deleteAll();

        // Initialize or reset inventory item
        InventoryItem item = inventoryItemRepository.findByProductId("PROD-001")
                .orElseGet(() -> InventoryItem.builder()
                        .productId("PROD-001")
                        .availableQuantity(100)
                        .reservedQuantity(0)
                        .build());
        item.setAvailableQuantity(100);
        item.setReservedQuantity(0);
        inventoryItemRepository.save(item);

        // Set up test producer
        Map<String, Object> producerProps = new HashMap<>();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);

        DefaultKafkaProducerFactory<String, Object> producerFactory =
                new DefaultKafkaProducerFactory<>(producerProps, new StringSerializer(), new JsonSerializer<>(objectMapper));
        kafkaTemplate = new KafkaTemplate<>(producerFactory);
    }

    @Test
    @DisplayName("Should process OrderCreatedEvent and create reservation")
    void shouldProcessOrderCreatedEvent() {
        // Given
        UUID eventId = UUID.randomUUID();
        UUID orderId = UUID.randomUUID();
        OrderCreatedEvent event = createOrderCreatedEvent(eventId, orderId, "PROD-001", 10);

        // When
        kafkaTemplate.send(KafkaConsumerConfig.ORDERS_TOPIC, orderId.toString(), event);

        // Then
        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            assertThat(reservationRepository.existsByOrderId(orderId)).isTrue();
            assertThat(processedEventRepository.existsByEventId(eventId)).isTrue();

            InventoryItem item = inventoryItemRepository.findByProductId("PROD-001").orElseThrow();
            assertThat(item.getAvailableQuantity()).isEqualTo(90);
            assertThat(item.getReservedQuantity()).isEqualTo(10);
        });
    }

    @Test
    @DisplayName("Should be idempotent - replaying same event does not create duplicate reservation")
    void shouldBeIdempotent_replayedEventDoesNotDuplicate() {
        // Given
        UUID eventId = UUID.randomUUID();
        UUID orderId = UUID.randomUUID();
        OrderCreatedEvent event = createOrderCreatedEvent(eventId, orderId, "PROD-001", 10);

        // When - send the same event twice
        kafkaTemplate.send(KafkaConsumerConfig.ORDERS_TOPIC, orderId.toString(), event);

        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            assertThat(reservationRepository.existsByOrderId(orderId)).isTrue();
        });

        // Send the same event again (simulating replay)
        kafkaTemplate.send(KafkaConsumerConfig.ORDERS_TOPIC, orderId.toString(), event);

        // Wait for potential processing
        await().pollDelay(Duration.ofSeconds(2)).atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
            // Then - should still have only one reservation
            long reservationCount = reservationRepository.findAll().stream()
                    .filter(r -> r.getOrderId().equals(orderId))
                    .count();
            assertThat(reservationCount).isEqualTo(1);

            // Inventory should only be decremented once
            InventoryItem item = inventoryItemRepository.findByProductId("PROD-001").orElseThrow();
            assertThat(item.getAvailableQuantity()).isEqualTo(90);
            assertThat(item.getReservedQuantity()).isEqualTo(10);

            // Processed event should exist
            assertThat(processedEventRepository.existsByEventId(eventId)).isTrue();
        });
    }

    @Test
    @DisplayName("Should be idempotent - multiple replays do not duplicate state")
    void shouldBeIdempotent_multipleReplaysDoNotDuplicate() {
        // Given
        UUID eventId = UUID.randomUUID();
        UUID orderId = UUID.randomUUID();
        OrderCreatedEvent event = createOrderCreatedEvent(eventId, orderId, "PROD-001", 5);

        // When - send the same event multiple times
        for (int i = 0; i < 5; i++) {
            kafkaTemplate.send(KafkaConsumerConfig.ORDERS_TOPIC, orderId.toString(), event);
        }

        // Then
        await().atMost(15, TimeUnit.SECONDS).untilAsserted(() -> {
            // Should have exactly one reservation
            long reservationCount = reservationRepository.findAll().stream()
                    .filter(r -> r.getOrderId().equals(orderId))
                    .count();
            assertThat(reservationCount).isEqualTo(1);

            // Inventory decremented only once
            InventoryItem item = inventoryItemRepository.findByProductId("PROD-001").orElseThrow();
            assertThat(item.getAvailableQuantity()).isEqualTo(95);
            assertThat(item.getReservedQuantity()).isEqualTo(5);
        });
    }

    @Test
    @DisplayName("Different events for different orders should be processed independently")
    void shouldProcessDifferentEventsIndependently() {
        // Given
        UUID eventId1 = UUID.randomUUID();
        UUID orderId1 = UUID.randomUUID();
        OrderCreatedEvent event1 = createOrderCreatedEvent(eventId1, orderId1, "PROD-001", 10);

        UUID eventId2 = UUID.randomUUID();
        UUID orderId2 = UUID.randomUUID();
        OrderCreatedEvent event2 = createOrderCreatedEvent(eventId2, orderId2, "PROD-001", 20);

        // When
        kafkaTemplate.send(KafkaConsumerConfig.ORDERS_TOPIC, orderId1.toString(), event1);
        kafkaTemplate.send(KafkaConsumerConfig.ORDERS_TOPIC, orderId2.toString(), event2);

        // Then
        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            assertThat(reservationRepository.existsByOrderId(orderId1)).isTrue();
            assertThat(reservationRepository.existsByOrderId(orderId2)).isTrue();

            InventoryItem item = inventoryItemRepository.findByProductId("PROD-001").orElseThrow();
            assertThat(item.getAvailableQuantity()).isEqualTo(70); // 100 - 10 - 20
            assertThat(item.getReservedQuantity()).isEqualTo(30); // 10 + 20
        });
    }

    @Test
    @DisplayName("Should handle non-existent product gracefully (publishes failure event)")
    void shouldHandleNonExistentProduct() {
        // Given - an event with a non-existent productId
        UUID eventId = UUID.randomUUID();
        UUID orderId = UUID.randomUUID();
        OrderCreatedEvent event = createOrderCreatedEvent(eventId, orderId, "NONEXISTENT-PRODUCT", 10);

        // When
        kafkaTemplate.send(KafkaConsumerConfig.ORDERS_TOPIC, orderId.toString(), event);

        // Then - the event should be processed (marked as processed) even though it results in failure event
        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            assertThat(processedEventRepository.existsByEventId(eventId)).isTrue();
            // No reservation should be created for non-existent product
            assertThat(reservationRepository.existsByOrderId(orderId)).isFalse();
        });
    }

    @Test
    @DisplayName("Should handle insufficient inventory gracefully (publishes failure event)")
    void shouldHandleInsufficientInventory() {
        // Given - an event requesting more inventory than available
        UUID eventId = UUID.randomUUID();
        UUID orderId = UUID.randomUUID();
        OrderCreatedEvent event = createOrderCreatedEvent(eventId, orderId, "PROD-001", 1000); // More than available

        // When
        kafkaTemplate.send(KafkaConsumerConfig.ORDERS_TOPIC, orderId.toString(), event);

        // Then - the event should be processed (marked as processed) even though it results in failure event
        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            assertThat(processedEventRepository.existsByEventId(eventId)).isTrue();
            // No reservation should be created for insufficient inventory
            assertThat(reservationRepository.existsByOrderId(orderId)).isFalse();

            // Inventory should remain unchanged
            InventoryItem item = inventoryItemRepository.findByProductId("PROD-001").orElseThrow();
            assertThat(item.getAvailableQuantity()).isEqualTo(100);
            assertThat(item.getReservedQuantity()).isEqualTo(0);
        });
    }

    private OrderCreatedEvent createOrderCreatedEvent(UUID eventId, UUID orderId, String productId, int quantity) {
        return OrderCreatedEvent.builder()
                .eventId(eventId)
                .orderId(orderId)
                .customerId("CUST-001")
                .productId(productId)
                .quantity(quantity)
                .unitPrice(BigDecimal.valueOf(99.99))
                .totalAmount(BigDecimal.valueOf(99.99 * quantity))
                .createdAt(Instant.now())
                .eventTimestamp(Instant.now())
                .build();
    }
}
