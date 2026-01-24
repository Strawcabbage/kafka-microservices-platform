package com.example.inventory.service;

import com.example.events.InventoryFailedEvent;
import com.example.events.InventoryReservedEvent;
import com.example.inventory.config.KafkaProducerConfig;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.UUID;

@Service
@RequiredArgsConstructor
@Slf4j
public class InventoryEventPublisher {

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public void publishInventoryReserved(UUID orderId, String productId, Integer quantityReserved, UUID reservationId) {
        InventoryReservedEvent event = InventoryReservedEvent.builder()
                .eventId(UUID.randomUUID())
                .orderId(orderId)
                .productId(productId)
                .quantityReserved(quantityReserved)
                .reservationId(reservationId)
                .eventTimestamp(Instant.now())
                .build();

        kafkaTemplate.send(KafkaProducerConfig.INVENTORY_TOPIC, orderId.toString(), event)
                .whenComplete((result, ex) -> {
                    if (ex == null) {
                        log.info("InventoryReservedEvent published for order: {} to partition: {} with offset: {}",
                                orderId,
                                result.getRecordMetadata().partition(),
                                result.getRecordMetadata().offset());
                    } else {
                        log.error("Failed to publish InventoryReservedEvent for order: {}", orderId, ex);
                    }
                });
    }

    public void publishInventoryFailed(UUID orderId, String productId, Integer quantityRequested,
                                        Integer quantityAvailable, String failureReason) {
        InventoryFailedEvent event = InventoryFailedEvent.builder()
                .eventId(UUID.randomUUID())
                .orderId(orderId)
                .productId(productId)
                .quantityRequested(quantityRequested)
                .quantityAvailable(quantityAvailable)
                .failureReason(failureReason)
                .eventTimestamp(Instant.now())
                .build();

        kafkaTemplate.send(KafkaProducerConfig.INVENTORY_TOPIC, orderId.toString(), event)
                .whenComplete((result, ex) -> {
                    if (ex == null) {
                        log.info("InventoryFailedEvent published for order: {} to partition: {} with offset: {}",
                                orderId,
                                result.getRecordMetadata().partition(),
                                result.getRecordMetadata().offset());
                    } else {
                        log.error("Failed to publish InventoryFailedEvent for order: {}", orderId, ex);
                    }
                });
    }
}
