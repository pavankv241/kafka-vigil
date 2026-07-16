package com.kafka.vigil.common;

import java.time.Instant;
import java.util.List;

/**
 * Snapshot published by the Vigil Agent — analogous to C3 cluster/topic health views.
 */
public record HealthSnapshot(
        Instant collectedAt,
        String clusterId,
        List<TopicHealth> topics,
        List<ConsumerGroupLag> consumerLags,
        HealthStatus overallStatus,
        String summary
) {
}
