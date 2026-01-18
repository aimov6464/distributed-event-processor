# distributed-event-processor

Distributed event processing scaffold for Apache Spark Structured Streaming 3.5+.
The goal is to provide a backend-only foundation for building effectively-once pipelines
with well-defined exactly-once boundaries, fed by Kafka or other append-only sources.

## Processing Modes
- **MODE A (DEDUPE)** – Kafka -> stateful dedupe (`withWatermark + dropDuplicates` on `eventId`) with optional Parquet or Kafka sink.
- **MODE B (DEDUPE + AGGREGATE)** – Kafka -> JSON parsing -> dedupe -> watermark-aware tumbling windows by `userId`, emitting Parquet (append) and/or Kafka (update) aggregates.

## Configuration
- Use `application.conf` (Typesafe Config) only; override any field via `-DmodeA.sink.type=kafka` etc.
- `spark.checkpointBaseDir` defines the base path; each mode composes mode-specific checkpoint folders.
- Run `ModeAStreamingJob` or `ModeBStreamingJob` depending on the pipeline you need.

## Correctness & Exactly-Once Boundaries
- Input sources must deliver each record at-least-once; dedupe relies on deterministic `eventId` and watermarks.
- Spark checkpoints + Parquet sink provide effectively-once semantics; Kafka sink remains at-least-once but keys enable downstream dedupe.
- Changing schema/dedupe keys requires resetting checkpoints to avoid state corruption.

## Semantics & Guarantees (Mode A)
- The pipeline uses Structured Streaming checkpoints to provide fault-tolerant processing.
- Deduplication is stateful: `withWatermark(eventTime) + dropDuplicates(eventId)` stores seen `eventId`s in the state store.
- Watermark bounds state growth: events later than the watermark may be dropped from dedupe tracking.
- Parquet sink + checkpointing provides reproducible results on restart (effectively-once relative to the sink).
- Kafka sink should be treated as end-to-end at-least-once; `eventId` is emitted as the Kafka message key to enable downstream deduplication.

## How to verify Mode A dedupe
1) Produce two Kafka messages with the same `eventId` and close `eventTime`.
2) Run the job with Parquet sink.
3) Confirm only one row is written for that `eventId`.
4) Restart the job using the same checkpoint directory and re-send the same `eventId`.
5) Confirm no duplicate row appears (dedupe state is restored from checkpoint).

## Run the jobs
### Mode A (Parquet sink)
```bash
mvn -q -DskipTests package
$SPARK_HOME/bin/spark-submit \
  --class com.example.streaming.ModeAStreamingJob \
  target/distributed-event-processor-0.1.0-SNAPSHOT.jar \
  -Dspark.app.name=mode-a \
  -DmodeA.input.kafkaBootstrapServers=localhost:9092 \
  -DmodeA.input.topic=incoming-events
```

### Mode B (Parquet + Kafka sinks)
```bash
$SPARK_HOME/bin/spark-submit \
  --class com.example.streaming.ModeBStreamingJob \
  target/distributed-event-processor-0.1.0-SNAPSHOT.jar \
  -Dspark.app.name=mode-b \
  -DmodeB.input.kafkaBootstrapServers=localhost:9092 \
  -DmodeB.input.topic=incoming-events \
  -DmodeB.sinks.kafka.enabled=true
```

## Local Kafka via Docker Compose
1. Start the stack:
   ```bash
   docker compose up -d
   ```
2. Produce sample events:
   ```bash
   docker compose exec kafka kafka-console-producer --bootstrap-server localhost:9092 --topic incoming-events
   > {"eventId":"1","eventTime":"2024-01-01T00:00:00Z","payload":"hello"}
   ```
3. Observe outputs:
   - Parquet files land under `file:///tmp/distributed-event-processor/output/parquet` (Mode A) or `file:///tmp/distributed-event-processor/mode-b/parquet` (Mode B).
   - Kafka aggregates appear on `mode-b-aggregates` (enable Mode B Kafka sink).

## Performance Notes & Limitations
- State store size tracks watermark horizon; lower watermarks improve latency but keep more state in memory/disk.
- Events arriving later than the configured watermark are dropped from dedupe/aggregation.
- Kafka sink stays at-least-once even with update mode; consumers must dedupe on key.
- Schema is fixed (`eventId`, `userId`, `amount`, `eventType`, `eventTime`) and requires code/config changes for evolution.
- Window aggregations are append/update-only; changing window length requires checkpoint reset.
- Checkpoint directories must be reset when changing dedupe keys, window definitions, or aggregation logic.
