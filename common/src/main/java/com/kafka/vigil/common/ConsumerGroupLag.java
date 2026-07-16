package com.kafka.vigil.common;

import java.util.List;

public record ConsumerGroupLag(
        String groupId,
        String topic,
        long totalLag,
        List<PartitionLag> partitions
) {
}
