package com.kafka.vigil.agent;

import com.kafka.vigil.common.AnomalyAlert;
import com.kafka.vigil.common.ConsumerGroupLag;
import com.kafka.vigil.common.HealthSnapshot;
import com.kafka.vigil.common.HealthStatus;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * Pure health classification logic — easy to unit test without Kafka.
 */
public final class HealthClassifier {
    private final long warnThreshold;
    private final long criticalThreshold;

    public HealthClassifier(long warnThreshold, long criticalThreshold) {
        if (warnThreshold <= 0 || criticalThreshold <= warnThreshold) {
            throw new IllegalArgumentException("Thresholds must satisfy 0 < warn < critical");
        }
        this.warnThreshold = warnThreshold;
        this.criticalThreshold = criticalThreshold;
    }

    public HealthStatus classify(long totalLag) {
        if (totalLag >= criticalThreshold) {
            return HealthStatus.CRITICAL;
        }
        if (totalLag >= warnThreshold) {
            return HealthStatus.DEGRADED;
        }
        return HealthStatus.HEALTHY;
    }

    public List<AnomalyAlert> detectAnomalies(HealthSnapshot snapshot) {
        List<AnomalyAlert> alerts = new ArrayList<>();
        for (ConsumerGroupLag lag : snapshot.consumerLags()) {
            if (lag.totalLag() >= criticalThreshold) {
                alerts.add(new AnomalyAlert(
                        Instant.now(),
                        "CONSUMER_LAG_CRITICAL",
                        HealthStatus.CRITICAL,
                        "Group " + lag.groupId() + " lag on " + lag.topic() + " exceeded critical threshold",
                        lag.groupId(),
                        lag.topic(),
                        lag.totalLag(),
                        criticalThreshold
                ));
            } else if (lag.totalLag() >= warnThreshold) {
                alerts.add(new AnomalyAlert(
                        Instant.now(),
                        "CONSUMER_LAG_WARN",
                        HealthStatus.DEGRADED,
                        "Group " + lag.groupId() + " lag on " + lag.topic() + " is elevated",
                        lag.groupId(),
                        lag.topic(),
                        lag.totalLag(),
                        warnThreshold
                ));
            }

            lag.partitions().stream()
                    .filter(p -> p.lag() >= criticalThreshold)
                    .forEach(p -> alerts.add(new AnomalyAlert(
                            Instant.now(),
                            "PARTITION_LAG_HOTSPOT",
                            HealthStatus.CRITICAL,
                            "Hot partition " + p.partition() + " on " + lag.topic(),
                            lag.groupId(),
                            lag.topic(),
                            p.lag(),
                            criticalThreshold
                    )));
        }
        return alerts;
    }
}
