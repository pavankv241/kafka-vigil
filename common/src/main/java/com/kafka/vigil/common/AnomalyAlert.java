package com.kafka.vigil.common;

import java.time.Instant;

/**
 * Anomaly alert emitted when lag or throughput crosses thresholds.
 */
public record AnomalyAlert(
        Instant detectedAt,
        String type,
        HealthStatus severity,
        String message,
        String groupId,
        String topic,
        long metricValue,
        long threshold
) {
}
