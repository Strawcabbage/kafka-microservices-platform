package com.example.inventory.listener;

import com.example.inventory.config.KafkaConsumerConfig;
import com.example.inventory.service.InventoryService;
import com.example.events.OrderCreatedEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class OrderCreatedListener {

    private final InventoryService inventoryService;

    @KafkaListener(
            topics = KafkaConsumerConfig.ORDERS_TOPIC,
            groupId = KafkaConsumerConfig.CONSUMER_GROUP
    )
    public void handleOrderCreated(OrderCreatedEvent event) {
        log.info("Received OrderCreated event: eventId={}, orderId={}", event.getEventId(), event.getOrderId());
        log.info("Order details: customerId={}, productId={}, quantity={}",
                event.getCustomerId(), event.getProductId(), event.getQuantity());

        inventoryService.reserveInventory(
                event.getOrderId(),
                event.getProductId(),
                event.getQuantity()
        );
    }
}
