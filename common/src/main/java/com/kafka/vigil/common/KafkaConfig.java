package com.kafka.vigil.common;

/**
 * Shared Kafka connection defaults. Override via env: KAFKA_BOOTSTRAP_SERVERS.
 */
public final class KafkaConfig {
    public static final String BOOTSTRAP_SERVERS =
            System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");

    public static final String CLUSTER_ID = "vigil-local";

    /** Lag above this is DEGRADED. */
    public static final long LAG_WARN_THRESHOLD = Long.parseLong(
            System.getenv().getOrDefault("LAG_WARN_THRESHOLD", "50")
    );

    /** Lag above this is CRITICAL and raises an anomaly alert. */
    public static final long LAG_CRITICAL_THRESHOLD = Long.parseLong(
            System.getenv().getOrDefault("LAG_CRITICAL_THRESHOLD", "200")
    );

    private KafkaConfig() {
    }
}
