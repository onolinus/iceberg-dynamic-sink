# Lakehouse Dynamic Sink

Flink job that reads from multiple Kafka topics (Debezium CDC JSON), dynamically infers schemas at runtime, and writes to Apache Iceberg tables using the [Dynamic Iceberg Sink](https://flink.apache.org/2025/11/11/from-stream-to-lakehouse-kafka-ingestion-with-the-flink-dynamic-iceberg-sink/) (Iceberg 1.10.0).

No Schema Registry required -- schemas are inferred at runtime from JSON message content.

## Architecture

```
Kafka Topics (Debezium JSON)
        |
        v
+----------------------------+
|   KafkaSource              |  subscribes to topics via regex pattern
|   (multi-topic, parallel)  |
+------------+---------------+
             |  KafkaRecord (raw bytes + topic metadata)
             v
+--------------------------------------------+
|  KafkaRecordToDynamicRecordGenerator       |
|  1. Parse JSON value                       |
|  2. Unwrap Debezium envelope (payload)     |
|  3. Filter delete tombstones               |
|  4. Infer Iceberg schema from JSON fields  |
|  5. Convert JSON -> Flink RowData          |
|  6. Derive TableIdentifier from topic name |
+------------+-------------------------------+
             |  DynamicRecord
             v
+----------------------------+
|   DynamicIcebergSink       |  routes to correct table, handles schema evolution
|   (Apache Iceberg 1.10)    |  auto-creates tables, adds new columns on the fly
+------------+---------------+
             |
             v
    Alibaba Cloud OSS-DLS
    +-- oss://your-bucket.../user/hive/warehouse/
        +-- raw.db/agreement_loan_document/
        +-- raw.db/agent_agent_details/
        +-- raw.db/<topic_name>/
```

**Key benefit:** adding a new Kafka topic automatically creates a new Iceberg table -- no job restart, no manual schema registration.

**Target Runtime**: Alibaba Cloud VVR 11.6 (Flink 1.20 / JDK 11)

---

## Stack

| Component          | Technology                                                   |
|--------------------|--------------------------------------------------------------|
| Stream Engine      | Apache Flink 1.20 (VVR 11.6)                                |
| Table Format       | Apache Iceberg 1.10.0                                        |
| Catalog            | Hive Metastore (`thrift://<your-metastore-host>:9083`)       |
| Storage            | Alibaba Cloud OSS-DLS                                        |
| Message Format     | Debezium JSON envelope (`{"schema":{...}, "payload":{...}}`) |
| Source             | Kafka (`<your-kafka-broker>:9092`)                           |

---

## Project Structure

```
src/main/java/
+-- com/bfi/lakehouse/sink/
|   +-- DynamicIcebergSinkJob.java              # Main entry point
|   +-- model/
|   |   +-- KafkaRecord.java                    # Lightweight Kafka record POJO
|   +-- deserializer/
|   |   +-- KafkaRecordDeserializationSchema.java  # Raw bytes deserializer
|   +-- generator/
|   |   +-- KafkaRecordToDynamicRecordGenerator.java  # JSON -> DynamicRecord
|   +-- io/
|   |   +-- DlsAwareOSSFileIO.java              # OSS FileIO with DLS URL rewriting (Alibaba Cloud)
|   +-- util/
|       +-- JsonSchemaInferrer.java              # JSON -> Iceberg Schema inference
|       +-- JsonRecordConverter.java             # JSON -> Flink RowData conversion
+-- org/apache/hadoop/util/
    +-- Sets.java                                # Hadoop 3.4 shim (VVR ships 3.3)
```

---

## Key Components

### DynamicIcebergSinkJob
Main Flink job entry point. Configures Kafka source with topic-pattern subscription, Hive Metastore catalog with OSS storage, and `DynamicIcebergSink` for automatic table creation and schema evolution.

### KafkaRecordToDynamicRecordGenerator
Implements `DynamicRecordGenerator<KafkaRecord>` -- the only interface you need to implement for the Dynamic Iceberg Sink:
1. Parses Kafka value as JSON
2. Unwraps Debezium envelope (`payload` extraction)
3. Filters delete tombstones (`__deleted = "true"`)
4. Infers Iceberg schema from JSON fields (cached per topic)
5. Merges with cached schema for schema evolution
6. Converts to Flink `RowData`
7. Emits `DynamicRecord` with target `TableIdentifier` derived from topic name

### DlsAwareOSSFileIO
Custom `FileIO` wrapper that handles Alibaba Cloud OSS-DLS format URLs. The Hive Metastore stores table locations in DLS format (`oss://bucket.region.oss-dls.aliyuncs.com/path`). The standard Aliyun OSS SDK rejects this as an invalid bucket name. This wrapper rewrites paths to standard format (`oss://bucket/path`) before delegating to `OSSFileIO`.

### JsonSchemaInferrer
Infers Iceberg schemas from JSON by inspecting field types:

| JSON Type | Iceberg Type |
|-----------|-------------|
| boolean   | BooleanType |
| integer   | LongType    |
| decimal   | DoubleType  |
| string    | StringType  |
| null      | StringType  |
| object    | StructType  |
| array     | ListType    |

### Hadoop Sets Shim
`org.apache.hadoop.util.Sets` -- Iceberg 1.10.0 uses this class from Hadoop 3.4+, but VVR 11.6 ships Hadoop 3.3.x which lacks it. The shim provides required factory methods (`newHashSet`, `newHashSetWithExpectedSize`).

---

## Configuration

All configuration via environment variables (with defaults):

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka broker list |
| `KAFKA_TOPICS_PATTERN` | `[^_].*` | Java regex for topic matching |
| `KAFKA_CONSUMER_GROUP` | `flink-lakehouse-sink` | Consumer group ID |
| `KAFKA_STARTING_OFFSET` | `latest` | `latest`, `earliest`, or `committed` |
| `HIVE_METASTORE_URI` | `thrift://localhost:9083` | Hive Metastore thrift URI |
| `CATALOG_NAME` | `iceberg_oss` | Iceberg catalog name |
| `CATALOG_NAMESPACE` | `raw` | Iceberg namespace/database |
| `WAREHOUSE_PATH` | `oss://your-bucket...` | OSS warehouse path (DLS format) |
| `OSS_ENDPOINT` | `oss-your-region-internal.aliyuncs.com` | OSS endpoint |
| `OSS_ACCESS_KEY_ID` | *(required)* | OSS access key ID |
| `OSS_ACCESS_KEY_SECRET` | *(required)* | OSS secret access key |
| `WRITE_PARALLELISM` | `2` | Iceberg writer parallelism |
| `CHECKPOINT_INTERVAL_MS` | `60000` | Flink checkpoint interval (ms) |

---

## Build

Requires Java 11 and Maven.

```bash
# Build fat jar
mvn clean package -DskipTests

# Output: target/lakehouse-dynamic-sink-1.0-SNAPSHOT.jar
```

---

## Deploy to VVR (Alibaba Cloud Ververica)

1. **Upload** the fat jar to OSS:
   ```
   oss://<your-flink-bucket>/artifacts/namespaces/<your-namespace>/lakehouse-dynamic-sink-1.0-SNAPSHOT.jar
   ```

2. **Create a VVR deployment**:
   - Engine Version: `vvr-11.6-jdk11-flink-1.20`
   - JAR URI: the OSS path above
   - Entry Point Class: `com.bfi.lakehouse.sink.DynamicIcebergSinkJob`

3. **Set credentials**: Configure `OSS_ACCESS_KEY_ID` and `OSS_ACCESS_KEY_SECRET` via VVR Variables/Secrets (use `${secret_values.your-secret-name}` format).

4. **Start** the deployment.

---

## Dependency Version Constraints

These versions are pinned to avoid classpath conflicts with VVR 11.6. **Do not upgrade without testing.**

| Dependency | Version | Reason |
|------------|---------|--------|
| `iceberg-flink-runtime-1.20` | `1.10.0` | Must match Flink 1.20; contains Dynamic Iceberg Sink |
| `aliyun-sdk-oss` | **`3.10.2`** | 3.15+ changed `deleteObject()` return type from `void` to `VoidResult` -- causes `NoSuchMethodError` |
| `libthrift` | **`0.9.3`** | Hive 3.1.3 compiled against Thrift 0.9.3; later versions changed `TFramedTransport` hierarchy -- causes `VerifyError` |
| `hive-metastore` | `3.1.3` | VVR does not provide this on its classpath; must be bundled |
| `flink-metrics-dropwizard` | `1.20.1` | Must match Flink version; VVR's version has incompatible constructor |

---

## Shade Plugin Relocations

The fat jar uses Maven Shade relocations to avoid VVR classloader conflicts:

| Original Package | Relocated To | Reason |
|------------------|--------------|--------|
| `org.apache.flink.dropwizard` | `com.bfi.shaded.flink.dropwizard` | VVR loads `org.apache.flink.*` parent-first; VVR's `DropwizardHistogramWrapper` has incompatible constructor |
| `com.codahale.metrics` | `com.bfi.shaded.codahale.metrics` | Must match the relocated `DropwizardHistogramWrapper` |
| `org.apache.thrift` | `com.bfi.shaded.thrift` | VVR's Thrift version has incompatible `TFramedTransport` class hierarchy with Hive 3.1.3 |

---

## Data Flow

1. **Kafka Source** reads from topics matching the regex pattern
2. **Generator** parses JSON, unwraps Debezium envelope, infers schema, converts to `RowData`, emits `DynamicRecord`
3. **Updater** creates new Iceberg tables or evolves schema if new columns detected
4. **Writer** writes Parquet data files to OSS
5. **Committer** commits snapshots to Iceberg metadata on Flink checkpoint completion

**Important**: Data only becomes queryable after a **successful Flink checkpoint**. The checkpoint interval controls how frequently data is committed.

---

## Querying Data

After deployment and at least one successful checkpoint:

```sql
-- In Trino/Presto (connected to same Hive Metastore)
SELECT * FROM iceberg.raw.agreement_loan_document;

-- List all schemas
SHOW SCHEMAS FROM iceberg;

-- List tables
SHOW TABLES FROM iceberg.raw;
```

---

## Issues Resolved During Development

### 1. Hive Metastore Connection -- `TFramedTransport` VerifyError
**Problem**: `hive-metastore:3.1.3` compiled against Thrift 0.9.3 where `TFramedTransport extends TTransport`. Later Thrift versions broke this hierarchy.
**Fix**: Pin `libthrift:0.9.3` and relocate via shade plugin to `com.bfi.shaded.thrift`.

### 2. OSS SDK Missing -- `NoClassDefFoundError: com/aliyun/oss/OSS`
**Problem**: `iceberg-aliyun` declares `aliyun-sdk-oss` as optional; Maven doesn't pull it transitively.
**Fix**: Add explicit `aliyun-sdk-oss:3.10.2` dependency.

### 3. OSS Credentials Not Reaching OSSFileIO
**Problem**: Iceberg's `AliyunProperties` reads credentials from `client.access-key-id` and `client.access-key-secret`, not from the Flink SQL connector's `access-key-id`/`access-key-secret` keys.
**Fix**: Set all known credential key variants in catalog properties.

### 4. DLS-Format OSS URLs -- Invalid Bucket Name
**Problem**: Hive Metastore stores table locations in DLS format (`oss://bucket.region.oss-dls.aliyuncs.com/path`). The standard Aliyun OSS SDK rejects the compound authority as an invalid bucket name.
**Fix**: Created `DlsAwareOSSFileIO` wrapper that rewrites DLS URLs to standard `oss://bucket/path` format.

### 5. Flink Metrics -- `DropwizardHistogramWrapper` NoSuchMethodError
**Problem**: VVR's `DropwizardHistogramWrapper` has a different constructor than Iceberg expects. Since `org.apache.flink.*` is always loaded parent-first, bundled version was ignored.
**Fix**: Relocate both `org.apache.flink.dropwizard` and `com.codahale.metrics` via shade plugin to bypass parent-first classloading.

### 6. Hadoop `Sets` Class Missing -- `NoClassDefFoundError`
**Problem**: Iceberg 1.10.0 uses `org.apache.hadoop.util.Sets` from Hadoop 3.4+, but VVR ships Hadoop 3.3.x.
**Fix**: Created a shim class providing required factory methods (`newHashSet`, `newHashSetWithExpectedSize`).

### 7. OSS SDK Version -- `deleteObject` Return Type Change
**Problem**: `aliyun-sdk-oss` 3.15+ changed `deleteObject` return type from `void` to `VoidResult`. Iceberg's `OSSFileIO` compiled against the old signature.
**Fix**: Pin `aliyun-sdk-oss:3.10.2`.

### 8. Debezium Envelope -- Records Not Flowing
**Problem**: Kafka messages are in Debezium JSON envelope format (`{"schema":{...}, "payload":{...}}`). The generator was trying to infer schema from the envelope, not the payload.
**Fix**: Added Debezium envelope detection and `payload` extraction in the generator.

### 9. Namespace Check -- `namespaceExists` Not on Catalog Interface
**Problem**: `namespaceExists()` and `createNamespace()` are on `SupportsNamespaces`, not `Catalog`.
**Fix**: Cast to `SupportsNamespaces` with `instanceof` check.

### 10. Jackson Classloader Conflict
**Problem**: Using individual Iceberg thin modules caused `LinkageError` for Jackson classes -- different classloaders loading the same type.
**Fix**: Use `iceberg-flink-runtime-1.20` fat jar which properly shades Jackson internally.

---

## Version Compatibility

| Component | Version |
|-----------|---------|
| Apache Flink | 1.20.1 (VVR 11.6) |
| Apache Iceberg | 1.10.0 |
| flink-connector-kafka | 3.3.0-1.20 |
| Aliyun OSS SDK | 3.10.2 |
| Hive Metastore | 3.1.3 |
| libthrift | 0.9.3 |
| Java | 11 |

---

## TODO

- [ ] Remove hardcoded OSS credentials -- use VVR secret variables (`${secret_values.your-secret-name}`)
- [ ] Change `KAFKA_TOPICS_PATTERN` from single-topic testing to `[^_].*` for all non-internal topics
- [ ] Add monitoring/alerting for error counters
- [ ] Consider adding partition spec (e.g., by date) for large tables
