package com.example.inventory.service;

import com.example.inventory.domain.InventoryItem;
import com.example.inventory.domain.InventoryReservation;
import com.example.inventory.domain.ReservationStatus;
import com.example.inventory.repository.InventoryItemRepository;
import com.example.inventory.repository.InventoryReservationRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Optional;
import java.util.UUID;

@Service
@RequiredArgsConstructor
@Slf4j
public class InventoryService {

    private final InventoryItemRepository inventoryItemRepository;
    private final InventoryReservationRepository reservationRepository;
    private final InventoryEventPublisher eventPublisher;

    @Transactional
    public void reserveInventory(UUID orderId, String productId, Integer quantity) {
        log.info("Processing inventory reservation for orderId={}, productId={}, quantity={}",
                orderId, productId, quantity);

        // Idempotency check: if reservation already exists, skip processing
        if (reservationRepository.existsByOrderId(orderId)) {
            log.info("Reservation already exists for orderId={}, skipping", orderId);
            return;
        }

        Optional<InventoryItem> itemOpt = inventoryItemRepository.findByProductId(productId);

        if (itemOpt.isEmpty()) {
            log.warn("Product not found: {}", productId);
            eventPublisher.publishInventoryFailed(
                    orderId,
                    productId,
                    quantity,
                    0,
                    "Product not found: " + productId
            );
            return;
        }

        InventoryItem item = itemOpt.get();

        if (!item.canReserve(quantity)) {
            log.warn("Insufficient inventory for product: {}, requested: {}, available: {}",
                    productId, quantity, item.getAvailableQuantity());
            eventPublisher.publishInventoryFailed(
                    orderId,
                    productId,
                    quantity,
                    item.getAvailableQuantity(),
                    "Insufficient inventory"
            );
            return;
        }

        // Reserve the inventory
        item.reserve(quantity);
        inventoryItemRepository.save(item);

        // Create reservation record
        InventoryReservation reservation = InventoryReservation.builder()
                .orderId(orderId)
                .productId(productId)
                .quantity(quantity)
                .status(ReservationStatus.PENDING)
                .build();
        reservation = reservationRepository.save(reservation);

        log.info("Inventory reserved successfully: reservationId={}, orderId={}, productId={}, quantity={}",
                reservation.getId(), orderId, productId, quantity);

        // Publish success event
        eventPublisher.publishInventoryReserved(
                orderId,
                productId,
                quantity,
                reservation.getId()
        );
    }
}
