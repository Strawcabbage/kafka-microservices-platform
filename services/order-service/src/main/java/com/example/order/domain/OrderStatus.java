package com.example.order.domain;

public enum OrderStatus {
    PENDING,
    CONFIRMED,
    INVENTORY_RESERVED,
    PAYMENT_COMPLETED,
    SHIPPED,
    DELIVERED,
    CANCELLED,
    FAILED
}
