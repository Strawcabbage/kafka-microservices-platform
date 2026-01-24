package com.example.events;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.UUID;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class InventoryReservedEvent {

    private UUID eventId;
    private UUID orderId;
    private String productId;
    private Integer quantityReserved;
    private UUID reservationId;
    private Instant eventTimestamp;
}
