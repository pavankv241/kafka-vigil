package com.kafka.vigil.common;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class OrderEventTest {
    @Test
    void createGeneratesValidOrder() {
        OrderEvent event = OrderEvent.create("c-1", "us-east", 42.5);
        assertFalse(event.orderId().isBlank());
        assertEquals("c-1", event.customerId());
        assertEquals(OrderStatus.CREATED, event.status());
        assertFalse(event.poison());
    }

    @Test
    void poisonOrdersAreMarked() {
        OrderEvent event = OrderEvent.poison("c-2", "eu-west");
        assertTrue(event.poison());
        assertTrue(event.orderId().startsWith("POISON-"));
        assertTrue(event.amount() < 0);
    }
}
