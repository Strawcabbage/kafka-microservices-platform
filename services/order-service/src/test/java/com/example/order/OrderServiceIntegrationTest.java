package com.example.order;

import com.example.events.InventoryFailedEvent;
import com.example.events.InventoryReservedEvent;
import com.example.order.config.KafkaConfig;
import com.example.order.domain.Order;
import com.example.order.domain.OrderStatus;
import com.example.order.repository.OrderRepository;
import com.example.order.repository.ProcessedEventRepository;
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
class OrderServiceIntegrationTest {

    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>(DockerImageName.parse("postgres:15"))
            .withDatabaseName("orders")
            .withUsername("test")
            .withPassword("test");

    @Container
    static KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.5.0"));

    @Autowired
    private OrderRepository orderRepository;

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
                    new NewTopic(KafkaConfig.ORDERS_TOPIC, 3, (short) 1),
                    new NewTopic(KafkaConfig.INVENTORY_TOPIC, 3, (short) 1),
                    new NewTopic(KafkaConfig.INVENTORY_DLQ_TOPIC, 3, (short) 1)
            );
            adminClient.createTopics(topics);
        }
    }

    @BeforeEach
    void setUp() {
        // Clear test data
        processedEventRepository.deleteAll();
        orderRepository.deleteAll();

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
    @DisplayName("Should update order to INVENTORY_RESERVED when InventoryReservedEvent is received")
    void shouldUpdateOrderToInventoryReserved() {
        // Given - create an order in PENDING status
        Order order = createAndSaveOrder(OrderStatus.PENDING);
        UUID reservationId = UUID.randomUUID();

        InventoryReservedEvent event = InventoryReservedEvent.builder()
                .eventId(UUID.randomUUID())
                .orderId(order.getId())
                .productId("PROD-001")
                .quantityReserved(10)
                .reservationId(reservationId)
                .eventTimestamp(Instant.now())
                .build();

        // When
        kafkaTemplate.send(KafkaConfig.INVENTORY_TOPIC, order.getId().toString(), event);

        // Then
        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            Order updatedOrder = orderRepository.findById(order.getId()).orElseThrow();
            assertThat(updatedOrder.getStatus()).isEqualTo(OrderStatus.INVENTORY_RESERVED);
            assertThat(updatedOrder.getInventoryReservationId()).isEqualTo(reservationId);
        });
    }

    @Test
    @DisplayName("Should update order to FAILED when InventoryFailedEvent is received")
    void shouldUpdateOrderToFailed() {
        // Given - create an order in PENDING status
        Order order = createAndSaveOrder(OrderStatus.PENDING);

        InventoryFailedEvent event = InventoryFailedEvent.builder()
                .eventId(UUID.randomUUID())
                .orderId(order.getId())
                .productId("PROD-001")
                .quantityRequested(100)
                .quantityAvailable(10)
                .failureReason("Insufficient inventory")
                .eventTimestamp(Instant.now())
                .build();

        // When
        kafkaTemplate.send(KafkaConfig.INVENTORY_TOPIC, order.getId().toString(), event);

        // Then
        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            Order updatedOrder = orderRepository.findById(order.getId()).orElseThrow();
            assertThat(updatedOrder.getStatus()).isEqualTo(OrderStatus.FAILED);
            assertThat(updatedOrder.getFailureReason()).isEqualTo("Insufficient inventory");
        });
    }

    @Test
    @DisplayName("Should be idempotent - replaying InventoryReservedEvent does not change already updated order")
    void shouldBeIdempotent_inventoryReservedEvent() {
        // Given
        Order order = createAndSaveOrder(OrderStatus.PENDING);
        UUID eventId = UUID.randomUUID();
        UUID reservationId = UUID.randomUUID();

        InventoryReservedEvent event = InventoryReservedEvent.builder()
                .eventId(eventId)
                .orderId(order.getId())
                .productId("PROD-001")
                .quantityReserved(10)
                .reservationId(reservationId)
                .eventTimestamp(Instant.now())
                .build();

        // When - send the same event twice
        kafkaTemplate.send(KafkaConfig.INVENTORY_TOPIC, order.getId().toString(), event);

        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            Order updatedOrder = orderRepository.findById(order.getId()).orElseThrow();
            assertThat(updatedOrder.getStatus()).isEqualTo(OrderStatus.INVENTORY_RESERVED);
        });

        // Send the same event again (simulating replay)
        kafkaTemplate.send(KafkaConfig.INVENTORY_TOPIC, order.getId().toString(), event);

        // Then - wait and verify state hasn't changed incorrectly
        await().pollDelay(Duration.ofSeconds(2)).atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
            Order finalOrder = orderRepository.findById(order.getId()).orElseThrow();
            assertThat(finalOrder.getStatus()).isEqualTo(OrderStatus.INVENTORY_RESERVED);
            assertThat(finalOrder.getInventoryReservationId()).isEqualTo(reservationId);
            assertThat(processedEventRepository.existsByEventId(eventId)).isTrue();
        });
    }

    @Test
    @DisplayName("Should be idempotent - replaying InventoryFailedEvent does not change already updated order")
    void shouldBeIdempotent_inventoryFailedEvent() {
        // Given
        Order order = createAndSaveOrder(OrderStatus.PENDING);
        UUID eventId = UUID.randomUUID();

        InventoryFailedEvent event = InventoryFailedEvent.builder()
                .eventId(eventId)
                .orderId(order.getId())
                .productId("PROD-001")
                .quantityRequested(100)
                .quantityAvailable(10)
                .failureReason("Insufficient inventory")
                .eventTimestamp(Instant.now())
                .build();

        // When - send the same event twice
        kafkaTemplate.send(KafkaConfig.INVENTORY_TOPIC, order.getId().toString(), event);

        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            Order updatedOrder = orderRepository.findById(order.getId()).orElseThrow();
            assertThat(updatedOrder.getStatus()).isEqualTo(OrderStatus.FAILED);
        });

        // Send the same event again
        kafkaTemplate.send(KafkaConfig.INVENTORY_TOPIC, order.getId().toString(), event);

        // Then
        await().pollDelay(Duration.ofSeconds(2)).atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
            Order finalOrder = orderRepository.findById(order.getId()).orElseThrow();
            assertThat(finalOrder.getStatus()).isEqualTo(OrderStatus.FAILED);
            assertThat(finalOrder.getFailureReason()).isEqualTo("Insufficient inventory");
            assertThat(processedEventRepository.existsByEventId(eventId)).isTrue();
        });
    }

    @Test
    @DisplayName("Should be idempotent - multiple replays do not corrupt order state")
    void shouldBeIdempotent_multipleReplays() {
        // Given
        Order order = createAndSaveOrder(OrderStatus.PENDING);
        UUID eventId = UUID.randomUUID();
        UUID reservationId = UUID.randomUUID();

        InventoryReservedEvent event = InventoryReservedEvent.builder()
                .eventId(eventId)
                .orderId(order.getId())
                .productId("PROD-001")
                .quantityReserved(10)
                .reservationId(reservationId)
                .eventTimestamp(Instant.now())
                .build();

        // When - send the same event multiple times
        for (int i = 0; i < 5; i++) {
            kafkaTemplate.send(KafkaConfig.INVENTORY_TOPIC, order.getId().toString(), event);
        }

        // Then
        await().atMost(15, TimeUnit.SECONDS).untilAsserted(() -> {
            Order finalOrder = orderRepository.findById(order.getId()).orElseThrow();
            assertThat(finalOrder.getStatus()).isEqualTo(OrderStatus.INVENTORY_RESERVED);
            assertThat(finalOrder.getInventoryReservationId()).isEqualTo(reservationId);

            // Should only have one processed event record
            long processedCount = processedEventRepository.findAll().stream()
                    .filter(pe -> pe.getEventId().equals(eventId))
                    .count();
            assertThat(processedCount).isEqualTo(1);
        });
    }

    @Test
    @DisplayName("Should process different events for different orders independently")
    void shouldProcessDifferentEventsIndependently() {
        // Given
        Order order1 = createAndSaveOrder(OrderStatus.PENDING);
        Order order2 = createAndSaveOrder(OrderStatus.PENDING);

        InventoryReservedEvent event1 = InventoryReservedEvent.builder()
                .eventId(UUID.randomUUID())
                .orderId(order1.getId())
                .productId("PROD-001")
                .quantityReserved(10)
                .reservationId(UUID.randomUUID())
                .eventTimestamp(Instant.now())
                .build();

        InventoryFailedEvent event2 = InventoryFailedEvent.builder()
                .eventId(UUID.randomUUID())
                .orderId(order2.getId())
                .productId("PROD-002")
                .quantityRequested(100)
                .quantityAvailable(5)
                .failureReason("Insufficient inventory")
                .eventTimestamp(Instant.now())
                .build();

        // When
        kafkaTemplate.send(KafkaConfig.INVENTORY_TOPIC, order1.getId().toString(), event1);
        kafkaTemplate.send(KafkaConfig.INVENTORY_TOPIC, order2.getId().toString(), event2);

        // Then
        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            Order updatedOrder1 = orderRepository.findById(order1.getId()).orElseThrow();
            Order updatedOrder2 = orderRepository.findById(order2.getId()).orElseThrow();

            assertThat(updatedOrder1.getStatus()).isEqualTo(OrderStatus.INVENTORY_RESERVED);
            assertThat(updatedOrder2.getStatus()).isEqualTo(OrderStatus.FAILED);
        });
    }

    @Test
    @DisplayName("Should send message to DLQ when order not found after retries")
    void shouldSendToDlqWhenOrderNotFound() {
        // Given - event for non-existent order
        UUID nonExistentOrderId = UUID.randomUUID();

        InventoryReservedEvent event = InventoryReservedEvent.builder()
                .eventId(UUID.randomUUID())
                .orderId(nonExistentOrderId)
                .productId("PROD-001")
                .quantityReserved(10)
                .reservationId(UUID.randomUUID())
                .eventTimestamp(Instant.now())
                .build();

        // Set up DLQ consumer
        Map<String, Object> consumerProps = new HashMap<>();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "dlq-test-group-" + UUID.randomUUID());
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        DefaultKafkaConsumerFactory<String, String> consumerFactory = new DefaultKafkaConsumerFactory<>(consumerProps);
        Consumer<String, String> dlqConsumer = consumerFactory.createConsumer();
        dlqConsumer.subscribe(Collections.singletonList(KafkaConfig.INVENTORY_DLQ_TOPIC));

        // When
        kafkaTemplate.send(KafkaConfig.INVENTORY_TOPIC, nonExistentOrderId.toString(), event);

        // Then - verify message lands in DLQ after retries
        await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> {
            ConsumerRecords<String, String> records = dlqConsumer.poll(Duration.ofMillis(500));
            assertThat(records.count()).isGreaterThan(0);

            // Verify the DLQ message contains our order ID
            boolean foundMessage = false;
            for (var record : records) {
                if (record.key().equals(nonExistentOrderId.toString())) {
                    foundMessage = true;
                    break;
                }
            }
            assertThat(foundMessage).isTrue();
        });

        dlqConsumer.close();
    }

    @Test
    @DisplayName("Should not update order that is not in PENDING status")
    void shouldNotUpdateOrderNotInPendingStatus() {
        // Given - create an order already in INVENTORY_RESERVED status
        Order order = Order.builder()
                .customerId("CUST-001")
                .productId("PROD-001")
                .quantity(10)
                .unitPrice(BigDecimal.valueOf(99.99))
                .status(OrderStatus.INVENTORY_RESERVED)
                .inventoryReservationId(UUID.randomUUID())
                .build();
        order = orderRepository.save(order);

        UUID originalReservationId = order.getInventoryReservationId();
        UUID newReservationId = UUID.randomUUID();

        InventoryReservedEvent event = InventoryReservedEvent.builder()
                .eventId(UUID.randomUUID())
                .orderId(order.getId())
                .productId("PROD-001")
                .quantityReserved(10)
                .reservationId(newReservationId)
                .eventTimestamp(Instant.now())
                .build();

        // When
        UUID orderId = order.getId();
        kafkaTemplate.send(KafkaConfig.INVENTORY_TOPIC, orderId.toString(), event);

        // Then - order should remain unchanged (business logic prevents update)
        await().pollDelay(Duration.ofSeconds(2)).atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
            Order finalOrder = orderRepository.findById(orderId).orElseThrow();
            assertThat(finalOrder.getStatus()).isEqualTo(OrderStatus.INVENTORY_RESERVED);
            // Original reservation ID should be preserved
            assertThat(finalOrder.getInventoryReservationId()).isEqualTo(originalReservationId);
        });
    }

    private Order createAndSaveOrder(OrderStatus status) {
        Order order = Order.builder()
                .customerId("CUST-001")
                .productId("PROD-001")
                .quantity(10)
                .unitPrice(BigDecimal.valueOf(99.99))
                .status(status)
                .build();
        return orderRepository.save(order);
    }
}
