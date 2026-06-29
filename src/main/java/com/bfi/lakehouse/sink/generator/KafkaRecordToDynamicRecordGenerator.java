package com.bfi.lakehouse.sink.generator;

import com.bfi.lakehouse.sink.model.KafkaRecord;
import com.bfi.lakehouse.sink.util.JsonRecordConverter;
import com.bfi.lakehouse.sink.util.JsonSchemaInferrer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.util.Collector;
import org.apache.iceberg.DistributionMode;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.sink.dynamic.DynamicRecord;
import org.apache.iceberg.flink.sink.dynamic.DynamicRecordGenerator;
import org.apache.iceberg.types.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Converts a raw {@link KafkaRecord} (plain JSON value) into one
 * {@link DynamicRecord} for the {@code DynamicIcebergSink}.
 *
 * <h3>How it works</h3>
 * <ol>
 *   <li>Parse the Kafka value bytes as a UTF-8 JSON object.</li>
 *   <li>Infer the Iceberg schema by inspecting the JSON field names and types.</li>
 *   <li>Merge the inferred schema with the cached schema for this topic, adding
 *       any new columns (schema evolution handled automatically by the sink).</li>
 *   <li>Convert the JSON object to a Flink {@link RowData}.</li>
 *   <li>Derive the target {@link TableIdentifier} from the Kafka topic name.</li>
 *   <li>Emit a {@link DynamicRecord}.</li>
 * </ol>
 *
 * <h3>Schema caching strategy</h3>
 * One {@link Schema} is cached per topic.  On each message the inferred schema
 * is merged into the cached one.  New fields are appended with a fresh field-ID
 * and the merged schema is sent in the {@code DynamicRecord} so the sink can
 * issue an {@code ALTER TABLE … ADD COLUMN} as needed.
 *
 * <h3>Table naming</h3>
 * Topic {@code "raw_orders"} → table {@code raw.raw_orders} (namespace set at construction time)<br>
 * Dots and hyphens in topic names are replaced with underscores.
 */
public class KafkaRecordToDynamicRecordGenerator
        implements DynamicRecordGenerator<KafkaRecord> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(KafkaRecordToDynamicRecordGenerator.class);

    private final String targetNamespace;
    private final int writeParallelism;
    private final CatalogLoader catalogLoader;

    /** Re-created in each Flink task after deserialisation. */
    private transient ObjectMapper objectMapper;

    /** Topics we have already logged a sample record for. */
    private transient java.util.Set<String> loggedTopics;

    /** Accumulated (growing) Iceberg schema per topic. */
    private transient Map<String, Schema> schemaCache;

    /** Cached Flink RowType per topic (derived from the current Schema). */
    private transient Map<String, RowType> rowTypeCache;

    /**
     * Per-topic counters that generate monotonically increasing Iceberg field IDs.
     * Field IDs must be unique within a schema and must never be reused.
     */
    private transient Map<String, AtomicInteger> fieldIdCounters;

    /** Lazily-created catalog for loading existing table schemas on restart. */
    private transient Catalog catalog;

    /** Diagnostic counters for monitoring record processing. */
    private transient long totalReceived;
    private transient long totalEmitted;
    private transient long totalSkipped;
    private transient long totalErrors;

    /**
     * @param targetNamespace Iceberg namespace (database) — e.g. {@code "raw"}
     * @param writeParallelism writer parallelism hint for {@link DynamicRecord}
     * @param catalogLoader used to load existing table schemas on job restart
     */
    public KafkaRecordToDynamicRecordGenerator(String targetNamespace, int writeParallelism,
                                                CatalogLoader catalogLoader) {
        this.targetNamespace = targetNamespace;
        this.writeParallelism = writeParallelism;
        this.catalogLoader = catalogLoader;
    }

    // -------------------------------------------------------------------------
    // Lifecycle
    // -------------------------------------------------------------------------

    @Override
    public void open(OpenContext openContext) throws Exception {
        objectMapper    = new ObjectMapper();
        schemaCache     = new HashMap<>();
        rowTypeCache    = new HashMap<>();
        fieldIdCounters = new HashMap<>();
        loggedTopics    = new java.util.HashSet<>();
        totalReceived   = 0;
        totalEmitted    = 0;
        totalSkipped    = 0;
        totalErrors     = 0;
        LOG.info("KafkaRecordToDynamicRecordGenerator opened. Namespace: {}", targetNamespace);
    }

    // -------------------------------------------------------------------------
    // Core generation logic
    // -------------------------------------------------------------------------

    @Override
    public void generate(KafkaRecord kafkaRecord, Collector<DynamicRecord> out) throws Exception {
        totalReceived++;

        if (kafkaRecord.value == null || kafkaRecord.value.length == 0) {
            totalSkipped++;
            LOG.warn("Skipping empty/tombstone record from topic={} offset={}",
                    kafkaRecord.topic, kafkaRecord.offset);
            return;
        }

        // Log the very first record of each topic at INFO so we can see the actual format
        boolean isNewTopic = loggedTopics.add(kafkaRecord.topic);
        String rawValue = new String(kafkaRecord.value, StandardCharsets.UTF_8);

        if (isNewTopic) {
            LOG.info("FIRST RECORD sample — topic={} offset={} first_byte=0x{} value={}",
                    kafkaRecord.topic,
                    kafkaRecord.offset,
                    String.format("%02X", kafkaRecord.value[0] & 0xFF),
                    rawValue.length() > 500 ? rawValue.substring(0, 500) + "..." : rawValue);
        }

        JsonNode jsonNode;
        try {
            jsonNode = objectMapper.readTree(rawValue);
        } catch (Exception e) {
            totalSkipped++;
            if (isNewTopic || totalSkipped <= 5) {
                LOG.warn("CANNOT PARSE JSON — topic={} offset={} first_byte=0x{} error={} value_preview={}",
                        kafkaRecord.topic, kafkaRecord.offset,
                        String.format("%02X", kafkaRecord.value[0] & 0xFF),
                        e.getMessage(),
                        rawValue.length() > 200 ? rawValue.substring(0, 200) : rawValue);
            }
            return;
        }

        if (!jsonNode.isObject()) {
            totalSkipped++;
            if (isNewTopic || totalSkipped <= 5) {
                LOG.warn("NON-OBJECT JSON — topic={} offset={} nodeType={} value={}",
                        kafkaRecord.topic, kafkaRecord.offset, jsonNode.getNodeType(),
                        rawValue.length() > 200 ? rawValue.substring(0, 200) : rawValue);
            }
            return;
        }

        // Unwrap Debezium envelope: {"schema":{...}, "payload":{...}}
        // Extract only the payload node which contains the actual row data.
        if (jsonNode.has("payload") && jsonNode.get("payload").isObject()) {
            if (isNewTopic) {
                LOG.info("Debezium envelope detected for topic={} — extracting payload", kafkaRecord.topic);
            }
            jsonNode = jsonNode.get("payload");
        }

        // Skip Debezium delete tombstones (__deleted = "true")
        JsonNode deletedFlag = jsonNode.get("__deleted");
        if (deletedFlag != null && "true".equals(deletedFlag.asText())) {
            totalSkipped++;
            return;
        }

        try {
            // ── 1. Infer / evolve schema ───────────────────────────────────────
            Schema schema = getOrEvolveSchema(kafkaRecord.topic, jsonNode);

            // ── 2. Derive Flink RowType from the (possibly updated) schema ─────
            RowType rowType = rowTypeCache.computeIfAbsent(
                    kafkaRecord.topic,
                    k -> FlinkSchemaUtil.convert(schema));

            // If schema changed (new columns), invalidate the cached RowType
            if (rowType.getFieldCount() != schema.columns().size()) {
                rowType = FlinkSchemaUtil.convert(schema);
                rowTypeCache.put(kafkaRecord.topic, rowType);
            }

            // ── 3. Convert JSON → RowData ──────────────────────────────────────
            RowData rowData = JsonRecordConverter.toRowData(jsonNode, rowType);

            // ── 4. Build and emit DynamicRecord ───────────────────────────────
            TableIdentifier tableId = topicToTableIdentifier(kafkaRecord.topic);

            DynamicRecord dynamicRecord = new DynamicRecord(
                    tableId,
                    "main",
                    schema,
                    rowData,
                    PartitionSpec.unpartitioned(),
                    DistributionMode.NONE,
                    writeParallelism);

            out.collect(dynamicRecord);
            totalEmitted++;

            if (totalEmitted <= 5 || totalEmitted % 1000 == 0) {
                LOG.info("EMIT #{} → table={} fields={} schema={}",
                        totalEmitted, tableId, schema.columns().size(), schema);
            }

        } catch (Exception e) {
            totalErrors++;
            // Log FULL stack trace for the first 10 errors so we can diagnose
            if (totalErrors <= 10) {
                LOG.error("ERROR #{} — Skipping record from topic={} offset={}: {}",
                        totalErrors, kafkaRecord.topic, kafkaRecord.offset, e.getMessage(), e);
            } else if (totalErrors % 1000 == 0) {
                LOG.error("Error count reached {} — latest from topic={} offset={}: {}",
                        totalErrors, kafkaRecord.topic, kafkaRecord.offset, e.getMessage());
            }
        }

        // Periodic stats every 5000 records
        if (totalReceived % 5000 == 0) {
            LOG.info("STATS — received={} emitted={} skipped={} errors={}",
                    totalReceived, totalEmitted, totalSkipped, totalErrors);
        }
    }

    // -------------------------------------------------------------------------
    // Schema management
    // -------------------------------------------------------------------------

    /**
     * Returns the current (possibly evolved) schema for the given topic.
     *
     * <p>If this is the first message for the topic, we first try to load the
     * existing Iceberg table schema from the catalog. This ensures column ordering
     * matches the table — critical because Hive Metastore compares columns by
     * position during ALTER TABLE. New fields from the JSON message are appended
     * after the existing columns.
     *
     * <p>For subsequent messages, any new top-level fields are merged in using
     * {@link JsonSchemaInferrer#mergeSchemas}.
     */
    private Schema getOrEvolveSchema(String topic, JsonNode jsonNode) {
        AtomicInteger counter = fieldIdCounters.computeIfAbsent(
                topic, k -> new AtomicInteger(1));

        Schema existing = schemaCache.get(topic);

        if (existing == null) {
            // First message for this topic — try to load existing table schema
            // so that column ordering matches the Iceberg table (avoids Hive
            // Metastore InvalidOperationException on ALTER TABLE).
            existing = tryLoadTableSchema(topic, counter);

            if (existing != null) {
                LOG.info("Loaded existing table schema for topic='{}': {} fields",
                        topic, existing.columns().size());
                // Merge with the current message to pick up any brand-new fields
                AtomicInteger tempCounter = new AtomicInteger(counter.get());
                Schema inferred = JsonSchemaInferrer.inferSchema(jsonNode, tempCounter);
                Schema merged = JsonSchemaInferrer.mergeSchemas(existing, inferred, counter);
                schemaCache.put(topic, merged);
                if (merged.columns().size() != existing.columns().size()) {
                    LOG.info("Schema evolved (on reload) for topic='{}': {} → {} fields",
                            topic, existing.columns().size(), merged.columns().size());
                }
                return merged;
            }

            // Table doesn't exist yet — infer schema from scratch
            Schema fresh = JsonSchemaInferrer.inferSchema(jsonNode, counter);
            schemaCache.put(topic, fresh);
            LOG.info("Inferred initial schema for topic='{}': {} fields", topic, fresh.columns().size());
            return fresh;
        }

        // Infer from the current message using a temporary counter (so IDs don't
        // advance if we don't actually add new fields)
        AtomicInteger tempCounter = new AtomicInteger(counter.get());
        Schema inferred = JsonSchemaInferrer.inferSchema(jsonNode, tempCounter);

        // Merge — only adds NEW fields; existing fields are unchanged
        Schema merged = JsonSchemaInferrer.mergeSchemas(existing, inferred, counter);

        if (merged.columns().size() != existing.columns().size()) {
            LOG.info("Schema evolved for topic='{}': {} → {} fields",
                    topic, existing.columns().size(), merged.columns().size());
            schemaCache.put(topic, merged);
            rowTypeCache.remove(topic);   // invalidate cached RowType
        }

        return merged;
    }

    /**
     * Tries to load the current schema of an existing Iceberg table for the given topic.
     * Returns null if the table does not exist or any error occurs.
     * Also advances the field ID counter past the highest existing field ID.
     */
    private Schema tryLoadTableSchema(String topic, AtomicInteger counter) {
        try {
            if (catalog == null) {
                catalog = catalogLoader.loadCatalog();
            }
            TableIdentifier tableId = topicToTableIdentifier(topic);
            if (!catalog.tableExists(tableId)) {
                return null;
            }
            Table table = catalog.loadTable(tableId);
            Schema tableSchema = table.schema();

            // Advance the field ID counter past the highest existing ID
            int maxId = 0;
            for (Types.NestedField field : tableSchema.columns()) {
                if (field.fieldId() > maxId) {
                    maxId = field.fieldId();
                }
            }
            if (maxId >= counter.get()) {
                counter.set(maxId + 1);
            }

            return tableSchema;
        } catch (Exception e) {
            LOG.warn("Could not load existing table schema for topic='{}': {}", topic, e.getMessage());
            return null;
        }
    }

    // -------------------------------------------------------------------------
    // Topic → TableIdentifier mapping
    // -------------------------------------------------------------------------

    /**
     * Derives the Iceberg {@link TableIdentifier} from a Kafka topic name.
     *
     * <p>Examples (namespace = {@code "raw"}):
     * <ul>
     *   <li>{@code "orders"}      → {@code raw.orders}</li>
     *   <li>{@code "user.events"} → {@code raw.user_events}</li>
     *   <li>{@code "raw-data"}    → {@code raw.raw_data}</li>
     * </ul>
     *
     * Override this method to implement custom topic-to-table routing.
     */
    protected TableIdentifier topicToTableIdentifier(String topic) {
        String tableName = topic
                .replace('.', '_')
                .replace('-', '_');
        return TableIdentifier.of(targetNamespace, tableName);
    }
}
