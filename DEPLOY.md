# Deployment Guide — Flink Jar Submission

## Prerequisites

| Tool | Check |
|------|-------|
| Java 11 | `java -version` |
| Maven 3.8+ | `mvn -version` |
| Flink CLI | `flink --version` (or set `FLINK_HOME`) |

---

## Step 1 — Build the fat jar

```bash
mvn clean package
```

Output: `target/lakehouse-dynamic-sink-1.0-SNAPSHOT.jar`

This jar bundles all dependencies (Iceberg, Kafka connector, Jackson, Aliyun OSS)
except Flink core and Hadoop — those are already on the cluster classpath.

---

## Step 2 — Set OSS credentials

The job reads credentials from environment variables so they are never hardcoded.

```bash
export OSS_ACCESS_KEY_ID=<your-access-key-id>
export OSS_ACCESS_KEY_SECRET=<your-access-key-secret>
```

> On Alibaba Cloud EMR, if the cluster nodes already have instance-role OSS access,
> you may not need to set these — in that case remove the credential check in
> `DynamicIcebergSinkJob.java` and omit the OSS key properties from the catalog config.

---

## Step 3 — Submit with `flink run`

### Option A — CLI (recommended)

```bash
flink run \
  --class com.bfi.lakehouse.sink.DynamicIcebergSinkJob \
  --detached \
  -D containerized.taskmanager.env.OSS_ACCESS_KEY_ID="${OSS_ACCESS_KEY_ID}" \
  -D containerized.taskmanager.env.OSS_ACCESS_KEY_SECRET="${OSS_ACCESS_KEY_SECRET}" \
  -D containerized.master.env.OSS_ACCESS_KEY_ID="${OSS_ACCESS_KEY_ID}" \
  -D containerized.master.env.OSS_ACCESS_KEY_SECRET="${OSS_ACCESS_KEY_SECRET}" \
  target/lakehouse-dynamic-sink-1.0-SNAPSHOT.jar
```

Or use the script:
```bash
bash scripts/run-job.sh
```

### Option B — Flink Web UI

1. Open the Flink Web UI.
2. Go to **Submit New Job** → **+ Add New**.
3. Upload `target/lakehouse-dynamic-sink-1.0-SNAPSHOT.jar`.
4. Set **Entry Class**: `com.bfi.lakehouse.sink.DynamicIcebergSinkJob`
5. Before submitting, set environment variables via the cluster's config
   (see "Passing credentials via cluster config" below).
6. Click **Submit**.

### Option C — YARN (if Flink runs on YARN)

```bash
flink run \
  --target yarn-per-job \
  --class com.bfi.lakehouse.sink.DynamicIcebergSinkJob \
  -Dyarn.application.name="lakehouse-sink" \
  -Djobmanager.memory.process.size=1g \
  -Dtaskmanager.memory.process.size=2g \
  -Dtaskmanager.numberOfTaskSlots=2 \
  -D containerized.taskmanager.env.OSS_ACCESS_KEY_ID="${OSS_ACCESS_KEY_ID}" \
  -D containerized.taskmanager.env.OSS_ACCESS_KEY_SECRET="${OSS_ACCESS_KEY_SECRET}" \
  -D containerized.master.env.OSS_ACCESS_KEY_ID="${OSS_ACCESS_KEY_ID}" \
  -D containerized.master.env.OSS_ACCESS_KEY_SECRET="${OSS_ACCESS_KEY_SECRET}" \
  target/lakehouse-dynamic-sink-1.0-SNAPSHOT.jar
```

---

## Passing credentials via cluster config (alternative to -D)

If you prefer not to pass secrets on the command line, add these to
`$FLINK_HOME/conf/flink-conf.yaml` on every cluster node **before starting Flink**:

```yaml
# flink-conf.yaml
env.java.opts.taskmanager: >
  -DOSS_ACCESS_KEY_ID=your-key-id
  -DOSS_ACCESS_KEY_SECRET=your-key-secret
```

Then the job reads them via `System.getenv()` → falls back to system properties
(add `-D`-style reading in `DynamicIcebergSinkJob.env()` if needed).

---

## Configuration reference

All settings have hardcoded defaults that match your environment.
Override any of them by passing `-D env.xxx` or setting the env var at job start.

| Env Variable | Default (hardcoded) | Description |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka brokers |
| `KAFKA_TOPICS_PATTERN` | `.*` | Regex — consume all matching topics |
| `KAFKA_CONSUMER_GROUP` | `flink-lakehouse-sink` | Consumer group ID |
| `KAFKA_STARTING_OFFSET` | `latest` | `latest`, `earliest`, or `committed` |
| `HIVE_METASTORE_URI` | `thrift://localhost:9083` | Hive Metastore |
| `CATALOG_NAME` | `iceberg_oss` | Iceberg catalog name |
| `CATALOG_NAMESPACE` | `raw` | Target Iceberg database |
| `WAREHOUSE_PATH` | `oss://your-bucket…` | OSS warehouse root |
| `OSS_ENDPOINT` | `oss-your-region-internal.aliyuncs.com` | OSS endpoint |
| `OSS_ACCESS_KEY_ID` | *(required)* | OSS access key |
| `OSS_ACCESS_KEY_SECRET` | *(required)* | OSS secret key |
| `WRITE_PARALLELISM` | `2` | Iceberg writer parallelism |
| `CHECKPOINT_INTERVAL_MS` | `60000` | Checkpoint interval (ms) |

---

## Verify the job is running

**Check Flink Web UI** — the job `Lakehouse — Kafka to Iceberg Dynamic Sink`
should appear in the Running Jobs list.

**Check Iceberg tables via Hive:**
```sql
-- In HiveQL or spark-sql
SHOW TABLES IN raw;
SELECT * FROM raw.orders LIMIT 10;
```

**Check OSS:**  
Browse `oss://your-bucket.your-region.oss-dls.aliyuncs.com/user/hive/warehouse/raw.db/`
— Iceberg data files (`.parquet`) should appear after the first checkpoint.

---

## Stopping the job

```bash
# List running jobs
flink list

# Cancel by job ID
flink cancel <job-id>
```

Or click **Cancel Job** in the Flink Web UI.

---

## Troubleshooting

| Symptom | Fix |
|---------|-----|
| `ClassNotFoundException: OSSFileIO` | Ensure `iceberg-aliyun` is in the fat jar — run `jar tf target/*.jar \| grep aliyun` |
| `MetaException: Unable to connect to metastore` | Check VPC/network access to `localhost:9083` from TaskManager nodes |
| `AccessDenied` on OSS | Verify `OSS_ACCESS_KEY_ID` / `OSS_ACCESS_KEY_SECRET` are reaching TaskManagers |
| Job appears but no records processed | Check Kafka topic regex in `KAFKA_TOPICS_PATTERN` matches your topic names |
| `IllegalStateException: OSS credentials must be set` | Set `OSS_ACCESS_KEY_ID` and `OSS_ACCESS_KEY_SECRET` env vars before submitting |
| Schema not evolving | Confirm Iceberg 1.10.0 is in the jar — check `jar tf target/*.jar \| grep iceberg-flink-sink-dynamic` |
