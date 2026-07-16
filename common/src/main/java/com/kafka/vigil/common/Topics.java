package com.kafka.vigil.common;

/**
 * Shared topic names for the Vigil demo pipeline.
 * Mirrors how platform teams keep topic contracts explicit and versioned.
 */
public final class Topics {
    public static final String ORDERS = "orders.events";
    public static final String ORDERS_DLQ = "orders.events.dlq";
    public static final String CLUSTER_HEALTH = "cluster.health";
    public static final String ANOMALY_ALERTS = "cluster.anomalies";

    private Topics() {
    }
}
