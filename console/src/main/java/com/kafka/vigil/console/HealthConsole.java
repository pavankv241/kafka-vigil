package com.kafka.vigil.console;

import com.kafka.vigil.common.AnomalyAlert;
import com.kafka.vigil.common.ConsumerGroupLag;
import com.kafka.vigil.common.HealthSnapshot;
import com.kafka.vigil.common.JsonSerde;
import com.kafka.vigil.common.KafkaConfig;
import com.kafka.vigil.common.PartitionLag;
import com.kafka.vigil.common.TopicBootstrap;
import com.kafka.vigil.common.TopicHealth;
import com.kafka.vigil.common.Topics;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Terminal "Control Center lite" — live view of health snapshots + anomaly alerts.
 */
public final class HealthConsole {
    private static final Logger log = LoggerFactory.getLogger(HealthConsole.class);

    public static void main(String[] args) {
        String bootstrap = KafkaConfig.BOOTSTRAP_SERVERS;
        TopicBootstrap.ensureTopics(bootstrap);

        AtomicBoolean running = new AtomicBoolean(true);
        AtomicReference<HealthSnapshot> latest = new AtomicReference<>();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> running.set(false)));

        Thread healthThread = new Thread(() -> consumeHealth(bootstrap, running, latest), "health-reader");
        Thread alertThread = new Thread(() -> consumeAlerts(bootstrap, running), "alert-reader");
        Thread renderer = new Thread(() -> renderLoop(running, latest), "renderer");

        healthThread.start();
        alertThread.start();
        renderer.start();

        log.info("Health Console listening on {} / {}", Topics.CLUSTER_HEALTH, Topics.ANOMALY_ALERTS);

        try {
            healthThread.join();
            alertThread.join();
            renderer.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static void consumeHealth(
            String bootstrap,
            AtomicBoolean running,
            AtomicReference<HealthSnapshot> latest
    ) {
        Properties props = baseConsumerProps(bootstrap, "vigil-console-health");
        try (KafkaConsumer<String, HealthSnapshot> consumer = new KafkaConsumer<>(
                props,
                new StringDeserializer(),
                JsonSerde.deserializer(HealthSnapshot.class)
        )) {
            consumer.subscribe(List.of(Topics.CLUSTER_HEALTH));
            while (running.get()) {
                ConsumerRecords<String, HealthSnapshot> records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, HealthSnapshot> record : records) {
                    if (record.value() != null) {
                        latest.set(record.value());
                    }
                }
            }
        }
    }

    private static void consumeAlerts(String bootstrap, AtomicBoolean running) {
        Properties props = baseConsumerProps(bootstrap, "vigil-console-alerts");
        try (KafkaConsumer<String, AnomalyAlert> consumer = new KafkaConsumer<>(
                props,
                new StringDeserializer(),
                JsonSerde.deserializer(AnomalyAlert.class)
        )) {
            consumer.subscribe(List.of(Topics.ANOMALY_ALERTS));
            while (running.get()) {
                ConsumerRecords<String, AnomalyAlert> records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, AnomalyAlert> record : records) {
                    AnomalyAlert alert = record.value();
                    if (alert != null) {
                        System.out.printf(
                                "%n⚠  ALERT %-22s [%s] %s (metric=%d threshold=%d)%n",
                                alert.type(),
                                alert.severity(),
                                alert.message(),
                                alert.metricValue(),
                                alert.threshold()
                        );
                    }
                }
            }
        }
    }

    private static void renderLoop(AtomicBoolean running, AtomicReference<HealthSnapshot> latest) {
        while (running.get()) {
            HealthSnapshot snap = latest.get();
            if (snap != null) {
                printDashboard(snap);
            }
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    private static void printDashboard(HealthSnapshot snap) {
        StringBuilder sb = new StringBuilder();
        sb.append("\n");
        sb.append("╔══════════════════════════════════════════════════════════════╗\n");
        sb.append("║  KAFKA VIGIL — Cluster Health Console                        ║\n");
        sb.append("╠══════════════════════════════════════════════════════════════╣\n");
        sb.append(String.format("║  cluster=%-12s  status=%-10s  at=%-20s ║%n",
                snap.clusterId(), snap.overallStatus(), snap.collectedAt()));
        sb.append(String.format("║  %s%n", pad(snap.summary(), 60)));
        sb.append("╠══════════════════════════════════════════════════════════════╣\n");
        sb.append("║  TOPICS                                                      ║\n");
        for (TopicHealth t : snap.topics()) {
            sb.append(String.format("║   %-22s partitions=%d  messages=%-8d      ║%n",
                    t.topic(), t.partitionCount(), t.totalMessages()));
        }
        sb.append("╠══════════════════════════════════════════════════════════════╣\n");
        sb.append("║  CONSUMER LAG                                                ║\n");
        for (ConsumerGroupLag g : snap.consumerLags()) {
            sb.append(String.format("║   group=%-18s topic=%-16s lag=%-6d ║%n",
                    g.groupId(), g.topic(), g.totalLag()));
            for (PartitionLag p : g.partitions()) {
                sb.append(String.format("║      p%-2d  current=%-8d end=%-8d lag=%-6d         ║%n",
                        p.partition(), p.currentOffset(), p.endOffset(), p.lag()));
            }
        }
        sb.append("╚══════════════════════════════════════════════════════════════╝\n");
        System.out.print(sb);
    }

    private static String pad(String text, int width) {
        if (text.length() >= width) {
            return text.substring(0, width);
        }
        return text + " ".repeat(width - text.length()) + "║";
    }

    private static Properties baseConsumerProps(String bootstrap, String groupId) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        return props;
    }
}
