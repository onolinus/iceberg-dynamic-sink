package com.bfi.lakehouse.sink;

import com.bfi.lakehouse.sink.deserializer.KafkaRecordDeserializationSchema;
import com.bfi.lakehouse.sink.generator.KafkaRecordToDynamicRecordGenerator;
import com.bfi.lakehouse.sink.model.KafkaRecord;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.sink.dynamic.DynamicIcebergSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Main Flink job: <strong>Kafka → Dynamic Iceberg Sink</strong>.
 *
 * <p>This job reads raw Avro messages from any number of Kafka topics (matched
 * by a regex pattern), automatically looks up schemas from the Confluent Schema
 * Registry, converts each record into an Iceberg {@code RowData}, and writes it
 * into the matching Iceberg table via the {@code DynamicIcebergSink}.
 *
 * <h3>Key behaviour</h3>
 * <ul>
 *   <li>No job restart needed when new topics are added — the source uses a
 *       topic pattern subscription.</li>
 *   <li>Schema evolution is handled transparently by the Dynamic Iceberg Sink —
 *       new columns are added to the target table automatically.</li>
 *   <li>New topics auto-route to new Iceberg tables (one table per topic).</li>
 * </ul>
 *
 * <h3>Configuration (environment variables)</h3>
 * <pre>
 *  KAFKA_BOOTSTRAP_SERVERS   Kafka broker list           (default: localhost:9092)
 *  KAFKA_TOPICS_PATTERN      Java regex for topics       (default: .* = all topics)
 *  KAFKA_CONSUMER_GROUP      Consumer group ID           (default: flink-lakehouse-sink)
 *  KAFKA_STARTING_OFFSET     latest | earliest | committed (default: latest)
 *  HIVE_METASTORE_URI        Hive Metastore thrift URI   (default: thrift://localhost:9083)
 *  CATALOG_NAME              Catalog name                (default: iceberg_oss)
 *  CATALOG_NAMESPACE         Iceberg database/namespace  (default: raw)
 *  WAREHOUSE_PATH            OSS warehouse path          (default: oss://your-bucket.your-region.oss-dls.aliyuncs.com/user/hive/warehouse)
 *  OSS_ENDPOINT              Alibaba Cloud OSS endpoint  (default: oss-your-region-internal.aliyuncs.com)
 *  OSS_ACCESS_KEY_ID         OSS access key ID           (REQUIRED — set via secrets)
 *  OSS_ACCESS_KEY_SECRET     OSS access key secret       (REQUIRED — set via secrets)
 *  WRITE_PARALLELISM         Iceberg writer parallelism  (default: 2)
 *  CHECKPOINT_INTERVAL_MS    Flink checkpoint interval   (default: 60000)
 * </pre>
 *
 * <h3>Running locally (IDE)</h3>
 * <ol>
 *   <li>{@code docker compose up -d} — start the infrastructure stack.</li>
 *   <li>Build with the local-run profile: {@code mvn package -Plocal-run}.</li>
 *   <li>Run {@code DynamicIcebergSinkJob.main(new String[]{})} from your IDE.</li>
 * </ol>
 *
 * <h3>Submitting to a Flink cluster</h3>
 * <pre>
 *   mvn clean package
 *   flink run -c com.bfi.lakehouse.sink.DynamicIcebergSinkJob \
 *       target/lakehouse-dynamic-sink-1.0-SNAPSHOT.jar
 * </pre>
 */
public class DynamicIcebergSinkJob {

    private static final Logger LOG = LoggerFactory.getLogger(DynamicIcebergSinkJob.class);

    public static void main(String[] args) throws Exception {

        // -----------------------------------------------------------------------
        // 1. Read configuration
        // -----------------------------------------------------------------------
        String kafkaBootstrapServers = env("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");
        // Single topic for testing — change to "[^_].*" for all non-internal topics
        String kafkaTopicsPattern    = env("KAFKA_TOPICS_PATTERN",    "[^_].*");
        String consumerGroupId       = env("KAFKA_CONSUMER_GROUP",    "flink-lakehouse-sink");
        String startingOffset        = env("KAFKA_STARTING_OFFSET",   "earliest");

        // Iceberg catalog — matches the Flink SQL connector config exactly
        String hiveMetastoreUri   = env("HIVE_METASTORE_URI",  "thrift://localhost:9083");
        String catalogName        = env("CATALOG_NAME",         "iceberg_oss");
        String namespace          = env("CATALOG_NAMESPACE",    "raw");
        String warehousePath      = env("WAREHOUSE_PATH",
                "oss://your-bucket.your-region.oss-dls.aliyuncs.com/user/hive/warehouse");
        String ossEndpoint        = env("OSS_ENDPOINT",         "oss-your-region-internal.aliyuncs.com");
        String ossRegion          = env("OSS_REGION",           "your-region");
        // TODO: remove hardcoded credentials after testing — use VVR secrets in production
        String ossAccessKeyId     = env("OSS_ACCESS_KEY_ID",     "");
        String ossAccessKeySecret = env("OSS_ACCESS_KEY_SECRET", "");

        int writeParallelism      = Integer.parseInt(env("WRITE_PARALLELISM",     "2"));
        long checkpointIntervalMs = Long.parseLong( env("CHECKPOINT_INTERVAL_MS", "30000")); // 30s for testing

        LOG.info("Starting DynamicIcebergSinkJob");
        LOG.info("  Kafka brokers    : {}", kafkaBootstrapServers);
        LOG.info("  Topics pattern   : {}", kafkaTopicsPattern);
        LOG.info("  Hive Metastore   : {}", hiveMetastoreUri);
        LOG.info("  Catalog name     : {}", catalogName);
        LOG.info("  Namespace        : {}", namespace);
        LOG.info("  Warehouse (OSS)  : {}", warehousePath);
        LOG.info("  OSS endpoint     : {}", ossEndpoint);
        LOG.info("  OSS region       : {}", ossRegion);
        LOG.info("  Write parallelism: {}", writeParallelism);

        // -----------------------------------------------------------------------
        // 2. Flink execution environment
        // -----------------------------------------------------------------------
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // VVR manages checkpoint storage automatically via its built-in OSS support.
        env.enableCheckpointing(checkpointIntervalMs);

        // -----------------------------------------------------------------------
        // 3. Kafka source — subscribes to all topics matching the regex pattern
        // -----------------------------------------------------------------------
        OffsetsInitializer offsetsInitializer;
        switch (startingOffset.toLowerCase()) {
            case "earliest":
                offsetsInitializer = OffsetsInitializer.earliest();
                break;
            case "committed":
                offsetsInitializer = OffsetsInitializer.committedOffsets();
                break;
            default:
                offsetsInitializer = OffsetsInitializer.latest();
        }

        KafkaSource<KafkaRecord> kafkaSource = KafkaSource.<KafkaRecord>builder()
                .setBootstrapServers(kafkaBootstrapServers)
                .setTopicPattern(Pattern.compile(kafkaTopicsPattern))
                .setGroupId(consumerGroupId)
                .setStartingOffsets(offsetsInitializer)
                .setDeserializer(new KafkaRecordDeserializationSchema())
                .build();

        DataStream<KafkaRecord> sourceStream = env.fromSource(
                kafkaSource,
                WatermarkStrategy.noWatermarks(),
                "kafka-multi-topic-source");

        // -----------------------------------------------------------------------
        // 4. Iceberg catalog: Hive Metastore + Alibaba Cloud OSS FileIO
        // -----------------------------------------------------------------------
        Configuration hadoopConf = buildHadoopConf(
                hiveMetastoreUri, ossEndpoint, ossAccessKeyId, ossAccessKeySecret);

        Map<String, String> catalogProperties = new HashMap<>();
        catalogProperties.put("warehouse",          warehousePath);
        // Custom FileIO that rewrites DLS-format URLs to standard OSS format
        catalogProperties.put("io-impl",            "com.bfi.lakehouse.sink.io.DlsAwareOSSFileIO");
        catalogProperties.put("oss.endpoint",              ossEndpoint);
        catalogProperties.put("oss.access-key-id",         ossAccessKeyId);
        catalogProperties.put("oss.access-key-secret",     ossAccessKeySecret);
        catalogProperties.put("client.access-key-id",      ossAccessKeyId);
        catalogProperties.put("client.access-key-secret",   ossAccessKeySecret);
        catalogProperties.put("access-key-id",             ossAccessKeyId);
        catalogProperties.put("access-key-secret",         ossAccessKeySecret);

        CatalogLoader catalogLoader = CatalogLoader.hive(
                catalogName,
                hadoopConf,
                catalogProperties);

        // -----------------------------------------------------------------------
        // 5. Ensure the target namespace exists in the Hive catalog
        //    HiveCatalog will NOT auto-create the database — tables cannot be
        //    created inside a namespace that doesn't exist yet.
        // -----------------------------------------------------------------------
        try {
            org.apache.iceberg.catalog.Catalog catalog = catalogLoader.loadCatalog();
            org.apache.iceberg.catalog.Namespace ns =
                    org.apache.iceberg.catalog.Namespace.of(namespace);
            if (catalog instanceof org.apache.iceberg.catalog.SupportsNamespaces) {
                org.apache.iceberg.catalog.SupportsNamespaces nsAware =
                        (org.apache.iceberg.catalog.SupportsNamespaces) catalog;
                if (!nsAware.namespaceExists(ns)) {
                    nsAware.createNamespace(ns);
                    LOG.info("Created Iceberg namespace: {}", namespace);
                } else {
                    LOG.info("Iceberg namespace already exists: {}", namespace);
                }
            } else {
                LOG.warn("Catalog does not support namespace management — skipping namespace check");
            }
        } catch (Exception e) {
            LOG.warn("Could not verify/create namespace '{}': {}", namespace, e.getMessage());
        }

        // -----------------------------------------------------------------------
        // 6. Dynamic Iceberg Sink — one sink handles all topics / tables
        // -----------------------------------------------------------------------
        DynamicIcebergSink.forInput(sourceStream)
                .generator(new KafkaRecordToDynamicRecordGenerator(
                        namespace,
                        writeParallelism,
                        catalogLoader))
                .catalogLoader(catalogLoader)
                .writeParallelism(writeParallelism)
                .immediateTableUpdate(true)   // triggers Updater for new tables/schemas
                .uidPrefix("lakehouse-sink")
                .append();

        // -----------------------------------------------------------------------
        // 6. Execute
        // -----------------------------------------------------------------------
        env.execute("Lakehouse — Kafka to Iceberg Dynamic Sink");
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    /**
     * Reads an environment variable with a fallback default value.
     */
    private static String env(String key, String defaultValue) {
        String value = System.getenv(key);
        return (value != null && !value.isBlank()) ? value : defaultValue;
    }

    /**
     * Builds the Hadoop {@link Configuration} pointing at the Hive Metastore.
     *
     * <p>If your cluster uses Kerberos, uncomment the kerberos lines and set
     * the appropriate principal and keytab path.
     *
     * <p>If your Flink cluster already has {@code hive-site.xml} on its
     * classpath (common in CDH/HDP clusters), this configuration is redundant
     * but harmless — the explicit URI takes precedence.
     */
    private static Configuration buildHadoopConf(
            String hiveMetastoreUri,
            String ossEndpoint,
            String ossAccessKeyId,
            String ossAccessKeySecret) {
        Configuration conf = new Configuration();

        // Point Hive client at your existing Hive Metastore
        conf.set("hive.metastore.uris", hiveMetastoreUri);

        // OSS filesystem credentials for DLS-format URLs
        conf.set("fs.oss.endpoint", ossEndpoint);
        conf.set("fs.oss.accessKeyId", ossAccessKeyId);
        conf.set("fs.oss.accessKeySecret", ossAccessKeySecret);

        // If HMS uses a non-default port for Thrift, it is already in the URI above.

        // ---- Kerberos (uncomment if your HMS is Kerberized) ----
        // conf.set("hadoop.security.authentication", "kerberos");
        // conf.set("hive.metastore.sasl.enabled", "true");
        // conf.set("hive.metastore.kerberos.principal", "hive/_HOST@YOUR.REALM");
        // UserGroupInformation.setConfiguration(conf);
        // UserGroupInformation.loginUserFromKeytab("flink@YOUR.REALM", "/path/to/flink.keytab");

        return conf;
    }
}
