# distributed-event-processor

Distributed event processing scaffold for Apache Spark Structured Streaming 3.5+. The goal is to provide a backend-only foundation for building exactly-once pipelines fed by Kafka or other append-only sources.

## Processing Modes
- **DEDUPE** – Stream of events passes through a stateful de-duplication stage keyed by event identity (e.g., id + source). Late records are discarded if an equal or newer version already exists.
- **AGGREGATE** – Records are aggregated by a user-defined grouping key with watermark-aware windows. Aggregates are emitted once per micro-batch to keep output idempotent.

## Correctness & Exactly-Once Boundaries
- Input sources must deliver each record at-least-once; final deduplication relies on deterministic keys and Spark state TTLs.
- Exactly-once output holds when writing to transactional sinks (Delta Lake, Kafka with idempotent producers) and when checkpointing stays stable.
- Cross-mode interactions share the same checkpoint directory; any change to schema or keys requires checkpoint reset to avoid state corruption.

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
