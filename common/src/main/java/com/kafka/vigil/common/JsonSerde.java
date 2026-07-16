package com.kafka.vigil.common;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.charset.StandardCharsets;

public final class JsonSerde {
    private JsonSerde() {
    }

    public static <T> Serializer<T> serializer() {
        return (topic, data) -> {
            if (data == null) {
                return null;
            }
            try {
                return JsonMapper.INSTANCE.writeValueAsBytes(data);
            } catch (Exception e) {
                throw new IllegalStateException("JSON serialize failed for topic " + topic, e);
            }
        };
    }

    public static <T> Deserializer<T> deserializer(Class<T> type) {
        return (topic, data) -> {
            if (data == null) {
                return null;
            }
            try {
                return JsonMapper.INSTANCE.readValue(data, type);
            } catch (Exception e) {
                throw new IllegalStateException(
                        "JSON deserialize failed for topic " + topic + ": " + new String(data, StandardCharsets.UTF_8),
                        e
                );
            }
        };
    }
}
