package com.kafka.vigil.common;

public record PartitionLag(
        int partition,
        long currentOffset,
        long endOffset,
        long lag
) {
}
