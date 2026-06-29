package com.bfi.lakehouse.sink.deserializer;

import com.bfi.lakehouse.sink.model.KafkaRecord;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;

/**
 * Flink {@link KafkaRecordDeserializationSchema} that wraps the raw Kafka
 * {@link ConsumerRecord} into a {@link KafkaRecord} POJO.
 *
 * <p>No Avro deserialisation happens here — the bytes are kept intact so that
 * the downstream generator can do schema-registry lookups and type-mapping
 * lazily, once per unique schema ID rather than once per record.
 */
public class KafkaRecordDeserializationSchema
        implements org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema<KafkaRecord> {

    private static final long serialVersionUID = 1L;

    @Override
    public void deserialize(
            ConsumerRecord<byte[], byte[]> record,
            Collector<KafkaRecord> out) throws IOException {

        out.collect(new KafkaRecord(
                record.topic(),
                record.partition(),
                record.offset(),
                record.key(),
                record.value(),
                record.timestamp()));
    }

    @Override
    public TypeInformation<KafkaRecord> getProducedType() {
        return TypeInformation.of(KafkaRecord.class);
    }
}
