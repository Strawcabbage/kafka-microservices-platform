package com.example.order.service;

import com.example.order.config.KafkaConfig;
import com.example.order.domain.Order;
import com.example.order.domain.OrderStatus;
import com.example.order.dto.CreateOrderRequest;
import com.example.order.dto.OrderResponse;
import com.example.events.OrderCreatedEvent;
import com.example.order.repository.OrderRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.UUID;

@Service
@RequiredArgsConstructor
@Slf4j
public class OrderService {

    private final OrderRepository orderRepository;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    @Transactional
    public OrderResponse createOrder(CreateOrderRequest request) {
        Order order = Order.builder()
                .customerId(request.getCustomerId())
                .productId(request.getProductId())
                .quantity(request.getQuantity())
                .unitPrice(request.getUnitPrice())
                .status(OrderStatus.PENDING)
                .build();

        Order savedOrder = orderRepository.save(order);
        log.info("Order created with id: {}", savedOrder.getId());

        publishOrderCreatedEvent(savedOrder);

        return toResponse(savedOrder);
    }

    private void publishOrderCreatedEvent(Order order) {
        OrderCreatedEvent event = OrderCreatedEvent.builder()
                .eventId(UUID.randomUUID())
                .orderId(order.getId())
                .customerId(order.getCustomerId())
                .productId(order.getProductId())
                .quantity(order.getQuantity())
                .unitPrice(order.getUnitPrice())
                .totalAmount(order.getTotalAmount())
                .createdAt(order.getCreatedAt())
                .eventTimestamp(Instant.now())
                .build();

        kafkaTemplate.send(KafkaConfig.ORDERS_TOPIC, order.getId().toString(), event)
                .whenComplete((result, ex) -> {
                    if (ex == null) {
                        log.info("OrderCreatedEvent published for order: {} to partition: {} with offset: {}",
                                order.getId(),
                                result.getRecordMetadata().partition(),
                                result.getRecordMetadata().offset());
                    } else {
                        log.error("Failed to publish OrderCreatedEvent for order: {}", order.getId(), ex);
                    }
                });
    }

    @Transactional
    public void markInventoryReserved(UUID orderId, UUID reservationId) {
        Order order = orderRepository.findById(orderId)
                .orElseThrow(() -> new IllegalArgumentException("Order not found: " + orderId));

        if (order.getStatus() != OrderStatus.PENDING) {
            log.warn("Order {} is not in PENDING status, current status: {}", orderId, order.getStatus());
            return;
        }

        order.setStatus(OrderStatus.INVENTORY_RESERVED);
        order.setInventoryReservationId(reservationId);
        orderRepository.save(order);

        log.info("Order {} marked as INVENTORY_RESERVED with reservationId: {}", orderId, reservationId);
    }

    @Transactional
    public void markOrderFailed(UUID orderId, String failureReason) {
        Order order = orderRepository.findById(orderId)
                .orElseThrow(() -> new IllegalArgumentException("Order not found: " + orderId));

        if (order.getStatus() != OrderStatus.PENDING) {
            log.warn("Order {} is not in PENDING status, current status: {}", orderId, order.getStatus());
            return;
        }

        order.setStatus(OrderStatus.FAILED);
        order.setFailureReason(failureReason);
        orderRepository.save(order);

        log.info("Order {} marked as FAILED with reason: {}", orderId, failureReason);
    }

    private OrderResponse toResponse(Order order) {
        return OrderResponse.builder()
                .id(order.getId())
                .customerId(order.getCustomerId())
                .productId(order.getProductId())
                .quantity(order.getQuantity())
                .unitPrice(order.getUnitPrice())
                .totalAmount(order.getTotalAmount())
                .status(order.getStatus())
                .createdAt(order.getCreatedAt())
                .build();
    }
}
