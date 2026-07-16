package com.kafka.vigil.common;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.errors.TopicExistsException;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * Ensures required topics exist — similar to platform bootstrap / agent provisioning.
 */
public final class TopicBootstrap {
    private TopicBootstrap() {
    }

    public static void ensureTopics(String bootstrapServers) {
        try (AdminClient admin = AdminClient.create(Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers
        ))) {
            List<NewTopic> topics = List.of(
                    new NewTopic(Topics.ORDERS, 3, (short) 1),
                    new NewTopic(Topics.ORDERS_DLQ, 1, (short) 1),
                    new NewTopic(Topics.CLUSTER_HEALTH, 1, (short) 1),
                    new NewTopic(Topics.ANOMALY_ALERTS, 1, (short) 1)
            );
            try {
                admin.createTopics(topics).all().get();
            } catch (ExecutionException e) {
                if (!(e.getCause() instanceof TopicExistsException)) {
                    throw new IllegalStateException("Failed to create topics", e.getCause());
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Interrupted while creating topics", e);
            }
        }
    }
}
