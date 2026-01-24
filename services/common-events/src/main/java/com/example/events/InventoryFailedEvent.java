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
public class InventoryFailedEvent {

    private UUID eventId;
    private UUID orderId;
    private String productId;
    private Integer quantityRequested;
    private Integer quantityAvailable;
    private String failureReason;
    private Instant eventTimestamp;
}
