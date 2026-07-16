package com.kafka.vigil.common;

import java.time.Instant;
import java.util.UUID;

/**
 * Domain event flowing through the order pipeline.
 */
public record OrderEvent(
        String orderId,
        String customerId,
        String region,
        double amount,
        OrderStatus status,
        Instant createdAt,
        boolean poison // intentionally corrupt for DLQ demos
) {
    public static OrderEvent create(String customerId, String region, double amount) {
        return new OrderEvent(
                UUID.randomUUID().toString(),
                customerId,
                region,
                amount,
                OrderStatus.CREATED,
                Instant.now(),
                false
        );
    }

    public static OrderEvent poison(String customerId, String region) {
        return new OrderEvent(
                "POISON-" + UUID.randomUUID(),
                customerId,
                region,
                -1.0,
                OrderStatus.CREATED,
                Instant.now(),
                true
        );
    }
}
