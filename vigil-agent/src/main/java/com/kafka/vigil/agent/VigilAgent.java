package com.kafka.vigil.agent;

import com.kafka.vigil.common.AnomalyAlert;
import com.kafka.vigil.common.ConsumerGroupLag;
import com.kafka.vigil.common.HealthSnapshot;
import com.kafka.vigil.common.HealthStatus;
import com.kafka.vigil.common.JsonSerde;
import com.kafka.vigil.common.KafkaConfig;
import com.kafka.vigil.common.PartitionLag;
import com.kafka.vigil.common.TopicBootstrap;
import com.kafka.vigil.common.TopicHealth;
import com.kafka.vigil.common.Topics;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * Vigil Agent — lightweight observability sidecar inspired by USM Agent / C3 monitoring.
 *
 * Periodically:
 * 1. Inspects topic metadata + end offsets (topic visibility)
 * 2. Computes consumer group lag per partition (consumer monitoring)
 * 3. Classifies cluster health and emits anomaly alerts
 * 4. Publishes HealthSnapshot to cluster.health for the console
 */
public final class VigilAgent {
    private static final Logger log = LoggerFactory.getLogger(VigilAgent.class);
    private static final String WATCHED_GROUP = "order-validators";
    private static final Set<String> WATCHED_TOPICS = Set.of(Topics.ORDERS, Topics.ORDERS_DLQ);

    public static void main(String[] args) throws Exception {
        String bootstrap = KafkaConfig.BOOTSTRAP_SERVERS;
        long intervalMs = Long.parseLong(System.getenv().getOrDefault("VIGIL_INTERVAL_MS", "3000"));

        TopicBootstrap.ensureTopics(bootstrap);

        Properties adminProps = new Properties();
        adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        adminProps.put(AdminClientConfig.CLIENT_ID_CONFIG, "vigil-agent-admin");

        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        producerProps.put(ProducerConfig.ACKS_CONFIG, "all");
        producerProps.put(ProducerConfig.CLIENT_ID_CONFIG, "vigil-agent-publisher");

        AtomicBoolean running = new AtomicBoolean(true);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> running.set(false)));

        HealthClassifier classifier = new HealthClassifier(
                KafkaConfig.LAG_WARN_THRESHOLD,
                KafkaConfig.LAG_CRITICAL_THRESHOLD
        );

        try (AdminClient admin = AdminClient.create(adminProps);
             KafkaProducer<String, Object> publisher = new KafkaProducer<>(
                     producerProps,
                     new StringSerializer(),
                     JsonSerde.serializer()
             )) {

            log.info("Vigil Agent online — watching group={} interval={}ms", WATCHED_GROUP, intervalMs);

            while (running.get()) {
                HealthSnapshot snapshot = collect(admin, classifier);
                publisher.send(new ProducerRecord<>(Topics.CLUSTER_HEALTH, KafkaConfig.CLUSTER_ID, snapshot));

                for (AnomalyAlert alert : classifier.detectAnomalies(snapshot)) {
                    publisher.send(new ProducerRecord<>(Topics.ANOMALY_ALERTS, alert.topic(), alert));
                    log.warn("ANOMALY [{}] {} (value={}, threshold={})",
                            alert.severity(), alert.message(), alert.metricValue(), alert.threshold());
                }
                publisher.flush();

                log.info("health={} lag={} topics={} — {}",
                        snapshot.overallStatus(),
                        snapshot.consumerLags().stream().mapToLong(ConsumerGroupLag::totalLag).sum(),
                        snapshot.topics().size(),
                        snapshot.summary());

                Thread.sleep(intervalMs);
            }
        }
        log.info("Vigil Agent stopped");
    }

    static HealthSnapshot collect(AdminClient admin, HealthClassifier classifier) throws Exception {
        Map<String, TopicDescription> descriptions = admin.describeTopics(WATCHED_TOPICS).allTopicNames().get();

        List<TopicPartition> allPartitions = descriptions.values().stream()
                .flatMap(td -> td.partitions().stream()
                        .map(p -> new TopicPartition(td.name(), p.partition())))
                .toList();

        Map<TopicPartition, OffsetSpec> endSpecs = allPartitions.stream()
                .collect(Collectors.toMap(tp -> tp, tp -> OffsetSpec.latest()));
        Map<TopicPartition, Long> endOffsets = admin.listOffsets(endSpecs).all().get().entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().offset()));

        List<TopicHealth> topicHealths = new ArrayList<>();
        for (String topic : WATCHED_TOPICS) {
            TopicDescription td = descriptions.get(topic);
            long total = endOffsets.entrySet().stream()
                    .filter(e -> e.getKey().topic().equals(topic))
                    .mapToLong(Map.Entry::getValue)
                    .sum();
            topicHealths.add(new TopicHealth(
                    topic,
                    td.partitions().size(),
                    total,
                    total * 256, // rough size estimate for demo dashboards
                    false
            ));
        }

        ListConsumerGroupOffsetsResult offsetsResult = admin.listConsumerGroupOffsets(WATCHED_GROUP);
        Map<TopicPartition, OffsetAndMetadata> committed =
                offsetsResult.partitionsToOffsetAndMetadata().get();

        List<PartitionLag> partitionLags = new ArrayList<>();
        long totalLag = 0;
        Collection<TopicPartition> orderPartitions = allPartitions.stream()
                .filter(tp -> tp.topic().equals(Topics.ORDERS))
                .toList();

        for (TopicPartition tp : orderPartitions) {
            long end = endOffsets.getOrDefault(tp, 0L);
            OffsetAndMetadata meta = committed.get(tp);
            long current = meta == null ? 0L : meta.offset();
            long lag = Math.max(0, end - current);
            totalLag += lag;
            partitionLags.add(new PartitionLag(tp.partition(), current, end, lag));
        }

        ConsumerGroupLag groupLag = new ConsumerGroupLag(
                WATCHED_GROUP,
                Topics.ORDERS,
                totalLag,
                partitionLags
        );

        HealthStatus status = classifier.classify(totalLag);
        String summary = switch (status) {
            case HEALTHY -> "All consumer groups within lag budget";
            case DEGRADED -> "Consumer lag elevated — investigate throughput / processing delay";
            case CRITICAL -> "Critical lag — consumers falling behind producers";
        };

        return new HealthSnapshot(
                Instant.now(),
                KafkaConfig.CLUSTER_ID,
                topicHealths,
                List.of(groupLag),
                status,
                summary
        );
    }
}
