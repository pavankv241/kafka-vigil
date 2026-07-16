package com.kafka.vigil.agent;

import com.kafka.vigil.common.AnomalyAlert;
import com.kafka.vigil.common.ConsumerGroupLag;
import com.kafka.vigil.common.HealthSnapshot;
import com.kafka.vigil.common.HealthStatus;
import com.kafka.vigil.common.PartitionLag;
import com.kafka.vigil.common.TopicHealth;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class HealthClassifierTest {
    private final HealthClassifier classifier = new HealthClassifier(50, 200);

    @Test
    void classifiesHealthyDegradedCritical() {
        assertEquals(HealthStatus.HEALTHY, classifier.classify(10));
        assertEquals(HealthStatus.DEGRADED, classifier.classify(50));
        assertEquals(HealthStatus.CRITICAL, classifier.classify(200));
    }

    @Test
    void detectsLagAnomaliesAndHotPartitions() {
        HealthSnapshot snapshot = new HealthSnapshot(
                Instant.now(),
                "test",
                List.of(new TopicHealth("orders.events", 3, 1000, 0, false)),
                List.of(new ConsumerGroupLag(
                        "order-validators",
                        "orders.events",
                        250,
                        List.of(
                                new PartitionLag(0, 0, 50, 50),
                                new PartitionLag(1, 0, 220, 220),
                                new PartitionLag(2, 0, 0, 0)
                        )
                )),
                HealthStatus.CRITICAL,
                "critical"
        );

        List<AnomalyAlert> alerts = classifier.detectAnomalies(snapshot);
        assertTrue(alerts.stream().anyMatch(a -> a.type().equals("CONSUMER_LAG_CRITICAL")));
        assertTrue(alerts.stream().anyMatch(a -> a.type().equals("PARTITION_LAG_HOTSPOT")));
    }
}
