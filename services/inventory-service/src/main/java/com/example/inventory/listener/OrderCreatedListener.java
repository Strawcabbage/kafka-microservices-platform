package com.example.inventory.listener;

import com.example.inventory.config.KafkaConsumerConfig;
import com.example.events.OrderCreatedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class OrderCreatedListener {

    private static final Logger log = LoggerFactory.getLogger(OrderCreatedListener.class);

    @KafkaListener(
            topics = KafkaConsumerConfig.ORDERS_TOPIC,
            groupId = KafkaConsumerConfig.CONSUMER_GROUP
    )
    public void handleOrderCreated(OrderCreatedEvent event) {
        log.info("Received OrderCreated event: eventId={}, orderId={}", event.getEventId(), event.getOrderId());
        log.info("Order details: customerId={}, productId={}, quantity={}",
                event.getCustomerId(), event.getProductId(), event.getQuantity());

        // Log inventory reservation behavior (actual reservation logic to be implemented later)
        log.info("Processing inventory reservation for orderId={}", event.getOrderId());
        log.info("Attempting to reserve {} units of product {} for order {}",
                event.getQuantity(), event.getProductId(), event.getOrderId());

        // Simulate reservation check
        log.info("Inventory reservation check completed for orderId={}, productId={}, quantity={}",
                event.getOrderId(), event.getProductId(), event.getQuantity());
    }
}
