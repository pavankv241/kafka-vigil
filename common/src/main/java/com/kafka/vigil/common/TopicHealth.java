package com.kafka.vigil.common;

public record TopicHealth(
        String topic,
        int partitionCount,
        long totalMessages,
        long bytesEstimate,
        boolean underReplicated // simulated signal for demo
) {
}
