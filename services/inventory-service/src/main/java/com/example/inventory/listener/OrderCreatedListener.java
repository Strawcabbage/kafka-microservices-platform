package com.example.inventory.listener;

import com.example.inventory.config.KafkaConsumerConfig;
import com.example.inventory.domain.ProcessedEvent;
import com.example.inventory.repository.ProcessedEventRepository;
import com.example.inventory.service.InventoryService;
import com.example.events.OrderCreatedEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Component
@RequiredArgsConstructor
@Slf4j
public class OrderCreatedListener {

    private final InventoryService inventoryService;
    private final ProcessedEventRepository processedEventRepository;

    @KafkaListener(
            topics = KafkaConsumerConfig.ORDERS_TOPIC,
            groupId = KafkaConsumerConfig.CONSUMER_GROUP
    )
    @Transactional
    public void handleOrderCreated(OrderCreatedEvent event) {
        log.info("Received OrderCreated event: eventId={}, orderId={}", event.getEventId(), event.getOrderId());

        // Idempotency check: skip if event was already processed
        if (processedEventRepository.existsByEventId(event.getEventId())) {
            log.info("Event already processed, skipping: eventId={}", event.getEventId());
            return;
        }

        log.info("Order details: customerId={}, productId={}, quantity={}",
                event.getCustomerId(), event.getProductId(), event.getQuantity());

        inventoryService.reserveInventory(
                event.getOrderId(),
                event.getProductId(),
                event.getQuantity()
        );

        // Record event as processed after successful handling
        ProcessedEvent processedEvent = ProcessedEvent.builder()
                .eventId(event.getEventId())
                .eventType("OrderCreatedEvent")
                .topic(KafkaConsumerConfig.ORDERS_TOPIC)
                .build();
        processedEventRepository.save(processedEvent);

        log.info("Event processing completed and recorded: eventId={}", event.getEventId());
    }
}
