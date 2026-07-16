package com.kafka.vigil.traffic;

import com.kafka.vigil.common.JsonSerde;
import com.kafka.vigil.common.KafkaConfig;
import com.kafka.vigil.common.OrderEvent;
import com.kafka.vigil.common.TopicBootstrap;
import com.kafka.vigil.common.Topics;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Generates order traffic into Kafka — including occasional poison messages for DLQ demos.
 */
public final class TrafficSimulator {
    private static final Logger log = LoggerFactory.getLogger(TrafficSimulator.class);
    private static final List<String> REGIONS = List.of("us-east", "us-west", "eu-west", "ap-south");
    private static final List<String> CUSTOMERS = List.of("acme", "globex", "initech", "umbrella", "stark");

    public static void main(String[] args) throws Exception {
        String bootstrap = KafkaConfig.BOOTSTRAP_SERVERS;
        int ratePerSec = Integer.parseInt(System.getenv().getOrDefault("TRAFFIC_RATE", "5"));
        int poisonEvery = Integer.parseInt(System.getenv().getOrDefault("POISON_EVERY", "25"));

        TopicBootstrap.ensureTopics(bootstrap);

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "vigil-traffic-simulator");

        AtomicBoolean running = new AtomicBoolean(true);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> running.set(false)));

        Random random = new Random();
        long sent = 0;

        try (KafkaProducer<String, OrderEvent> producer = new KafkaProducer<>(
                props,
                new StringSerializer(),
                JsonSerde.serializer()
        )) {
            log.info("Traffic simulator started → {} @ {} msg/s (poison every {})",
                    Topics.ORDERS, ratePerSec, poisonEvery);

            while (running.get()) {
                sent++;
                OrderEvent event = (sent % poisonEvery == 0)
                        ? OrderEvent.poison(pick(CUSTOMERS, random), pick(REGIONS, random))
                        : OrderEvent.create(
                        pick(CUSTOMERS, random),
                        pick(REGIONS, random),
                        10 + random.nextDouble() * 990
                );

                // Key by customer → sticky partition affinity (partitioning demo talking point)
                ProducerRecord<String, OrderEvent> record =
                        new ProducerRecord<>(Topics.ORDERS, event.customerId(), event);

                producer.send(record, (metadata, exception) -> {
                    if (exception != null) {
                        log.error("Send failed", exception);
                    } else if (log.isDebugEnabled()) {
                        log.debug("sent {} → {}-{}", event.orderId(), metadata.topic(), metadata.partition());
                    }
                });

                if (sent % 20 == 0) {
                    log.info("produced {} events (last={}, poison={})", sent, event.orderId(), event.poison());
                }

                Thread.sleep(Duration.ofMillis(Math.max(1, 1000 / ratePerSec)).toMillis());
            }
            producer.flush();
        }
        log.info("Traffic simulator stopped after {} events", sent);
    }

    private static String pick(List<String> values, Random random) {
        return values.get(random.nextInt(values.size()));
    }
}
