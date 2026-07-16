package com.kafka.vigil.processor;

import com.kafka.vigil.common.JsonSerde;
import com.kafka.vigil.common.KafkaConfig;
import com.kafka.vigil.common.OrderEvent;
import com.kafka.vigil.common.OrderStatus;
import com.kafka.vigil.common.TopicBootstrap;
import com.kafka.vigil.common.Topics;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Order stream processor with intentional slow-path + DLQ routing.
 * Slow processing creates measurable consumer lag for Vigil Agent demos.
 */
public final class StreamProcessor {
    public static final String GROUP_ID = "order-validators";
    private static final Logger log = LoggerFactory.getLogger(StreamProcessor.class);

    public static void main(String[] args) {
        String bootstrap = KafkaConfig.BOOTSTRAP_SERVERS;
        long processingDelayMs = Long.parseLong(
                System.getenv().getOrDefault("PROCESSING_DELAY_MS", "200")
        );

        TopicBootstrap.ensureTopics(bootstrap);

        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        consumerProps.put(ConsumerConfig.CLIENT_ID_CONFIG, "vigil-stream-processor");
        consumerProps.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "10");

        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        producerProps.put(ProducerConfig.ACKS_CONFIG, "all");
        producerProps.put(ProducerConfig.CLIENT_ID_CONFIG, "vigil-dlq-producer");

        AtomicBoolean running = new AtomicBoolean(true);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> running.set(false)));

        try (KafkaConsumer<String, OrderEvent> consumer = new KafkaConsumer<>(
                consumerProps,
                new StringDeserializer(),
                JsonSerde.deserializer(OrderEvent.class)
        );
             KafkaProducer<String, OrderEvent> dlqProducer = new KafkaProducer<>(
                     producerProps,
                     new StringSerializer(),
                     JsonSerde.serializer()
             )) {

            consumer.subscribe(List.of(Topics.ORDERS));
            log.info("Stream processor started (group={}, delay={}ms)", GROUP_ID, processingDelayMs);

            while (running.get()) {
                ConsumerRecords<String, OrderEvent> records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, OrderEvent> record : records) {
                    process(record, dlqProducer, processingDelayMs);
                }
                if (!records.isEmpty()) {
                    consumer.commitSync();
                }
            }
        }
        log.info("Stream processor stopped");
    }

    private static void process(
            ConsumerRecord<String, OrderEvent> record,
            KafkaProducer<String, OrderEvent> dlqProducer,
            long processingDelayMs
    ) {
        OrderEvent event = record.value();
        try {
            if (event == null || event.poison() || event.amount() <= 0) {
                throw new IllegalArgumentException("Invalid order payload: " + (event == null ? "null" : event.orderId()));
            }

            // Simulate validation work — tunable to create lag under load
            if (processingDelayMs > 0) {
                Thread.sleep(processingDelayMs);
            }

            log.info("validated order={} region={} amount={} partition={}",
                    event.orderId(), event.region(), String.format("%.2f", event.amount()), record.partition());
        } catch (Exception e) {
            OrderEvent failed = event == null
                    ? null
                    : new OrderEvent(
                    event.orderId(),
                    event.customerId(),
                    event.region(),
                    event.amount(),
                    OrderStatus.FAILED,
                    event.createdAt(),
                    true
            );
            if (failed != null) {
                dlqProducer.send(new ProducerRecord<>(Topics.ORDERS_DLQ, failed.customerId(), failed));
                dlqProducer.flush();
            }
            log.warn("routed to DLQ order={} reason={}", event != null ? event.orderId() : "unknown", e.getMessage());
        }
    }
}
