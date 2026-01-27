package com.example.order.listener;

import com.example.events.InventoryFailedEvent;
import com.example.events.InventoryReservedEvent;
import com.example.order.config.KafkaConfig;
import com.example.order.domain.ProcessedEvent;
import com.example.order.repository.ProcessedEventRepository;
import com.example.order.service.OrderService;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.Map;
import java.util.UUID;

@Component
@RequiredArgsConstructor
@Slf4j
public class InventoryEventListener {

    private final OrderService orderService;
    private final ObjectMapper objectMapper;
    private final ProcessedEventRepository processedEventRepository;

    @KafkaListener(
            topics = KafkaConfig.INVENTORY_TOPIC,
            groupId = KafkaConfig.CONSUMER_GROUP
    )
    @Transactional
    public void handleInventoryEvent(ConsumerRecord<String, Object> record) {
        Object value = record.value();
        log.info("Received inventory event for key: {}", record.key());

        try {
            // The value comes as a LinkedHashMap since we're using Object type deserializer
            if (value instanceof Map) {
                @SuppressWarnings("unchecked")
                Map<String, Object> eventMap = (Map<String, Object>) value;

                // Determine event type by checking for distinguishing fields
                if (eventMap.containsKey("reservationId")) {
                    InventoryReservedEvent event = objectMapper.convertValue(eventMap, InventoryReservedEvent.class);
                    handleInventoryReserved(event);
                } else if (eventMap.containsKey("failureReason")) {
                    InventoryFailedEvent event = objectMapper.convertValue(eventMap, InventoryFailedEvent.class);
                    handleInventoryFailed(event);
                } else {
                    log.warn("Unknown inventory event type: {}", eventMap);
                }
            }
        } catch (Exception e) {
            log.error("Error processing inventory event", e);
            throw e;
        }
    }

    private void handleInventoryReserved(InventoryReservedEvent event) {
        log.info("Processing InventoryReservedEvent: eventId={}, orderId={}, reservationId={}",
                event.getEventId(), event.getOrderId(), event.getReservationId());

        // Idempotency check: skip if event was already processed
        if (processedEventRepository.existsByEventId(event.getEventId())) {
            log.info("Event already processed, skipping: eventId={}", event.getEventId());
            return;
        }

        orderService.markInventoryReserved(event.getOrderId(), event.getReservationId());

        // Record event as processed after successful handling
        recordProcessedEvent(event.getEventId(), "InventoryReservedEvent");
        log.info("Event processing completed and recorded: eventId={}", event.getEventId());
    }

    private void handleInventoryFailed(InventoryFailedEvent event) {
        log.info("Processing InventoryFailedEvent: eventId={}, orderId={}, reason={}",
                event.getEventId(), event.getOrderId(), event.getFailureReason());

        // Idempotency check: skip if event was already processed
        if (processedEventRepository.existsByEventId(event.getEventId())) {
            log.info("Event already processed, skipping: eventId={}", event.getEventId());
            return;
        }

        orderService.markOrderFailed(event.getOrderId(), event.getFailureReason());

        // Record event as processed after successful handling
        recordProcessedEvent(event.getEventId(), "InventoryFailedEvent");
        log.info("Event processing completed and recorded: eventId={}", event.getEventId());
    }

    private void recordProcessedEvent(UUID eventId, String eventType) {
        ProcessedEvent processedEvent = ProcessedEvent.builder()
                .eventId(eventId)
                .eventType(eventType)
                .topic(KafkaConfig.INVENTORY_TOPIC)
                .build();
        processedEventRepository.save(processedEvent);
    }
}
