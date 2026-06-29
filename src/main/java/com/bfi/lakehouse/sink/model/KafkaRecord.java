package com.bfi.lakehouse.sink.model;

import java.io.Serializable;

/**
 * Lightweight, fully-serializable wrapper around a raw Kafka consumer record.
 * Headers are intentionally excluded — they are not needed for plain-JSON
 * ingestion and the Kafka Headers type is not safely serializable by Flink.
 */
public final class KafkaRecord implements Serializable {

    private static final long serialVersionUID = 1L;

    /** Kafka topic name — used to derive the target Iceberg table name. */
    public final String topic;

    public final int partition;
    public final long offset;
    public final byte[] key;

    /**
     * Record value as raw UTF-8 JSON bytes.
     */
    public final byte[] value;

    public final long timestamp;

    public KafkaRecord(
            String topic,
            int partition,
            long offset,
            byte[] key,
            byte[] value,
            long timestamp) {
        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
        this.key = key;
        this.value = value;
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "KafkaRecord{topic='" + topic + "', partition=" + partition
                + ", offset=" + offset + ", valueBytes=" + (value == null ? 0 : value.length) + '}';
    }
}
