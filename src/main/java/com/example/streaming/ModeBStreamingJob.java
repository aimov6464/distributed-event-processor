package com.example.streaming;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeoutException;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.concat_ws;
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.coalesce;
import static org.apache.spark.sql.functions.from_json;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.struct;
import static org.apache.spark.sql.functions.sum;
import static org.apache.spark.sql.functions.to_json;
import static org.apache.spark.sql.functions.window;

public final class ModeBStreamingJob {
    private static final Logger LOG = LoggerFactory.getLogger(ModeBStreamingJob.class);
    private static final StructType EVENT_SCHEMA = new StructType()
            .add("eventId", DataTypes.StringType, true)
            .add("userId", DataTypes.StringType, true)
            .add("amount", DataTypes.DoubleType, true)
            .add("eventType", DataTypes.StringType, true)
            .add("eventTime", DataTypes.TimestampType, true);

    private ModeBStreamingJob() {
    }

    public static void main(String[] args) throws TimeoutException, StreamingQueryException {
        JobConfig config = JobConfig.fromTypesafe();
        SparkSession spark = SparkSession.builder()
                .appName(config.appName())
                .getOrCreate();

        Dataset<Row> kafkaSource = spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", config.kafkaBootstrapServers())
                .option("subscribe", config.kafkaTopic())
                .option("startingOffsets", config.startingOffsets())
                .option("maxOffsetsPerTrigger", String.valueOf(config.maxOffsetsPerTrigger()))
                .load();

        Dataset<Row> parsedEvents = parseJson(kafkaSource);
        Dataset<Row> deduped = deduplicate(parsedEvents, config);
        Dataset<Row> aggregated = aggregate(deduped, config);
        Dataset<Row> flattened = flattenWindows(aggregated);
        Dataset<Row> kafkaOutput = config.kafkaSink().enabled() ? prepareKafkaPayload(flattened) : null;

        LOG.info("Mode B job starting with windowDuration={} watermarkDelay={}", config.windowDuration(), config.watermarkDelay());
        List<StreamingQuery> queries = SinkWriter.start(flattened, kafkaOutput, config);
        LOG.info("Mode B streaming job started with {} sink(s)", queries.size());

        spark.streams().awaitAnyTermination();
    }

    private static Dataset<Row> parseJson(Dataset<Row> kafkaSource) {
        return kafkaSource.selectExpr("CAST(value AS STRING) AS json")
                .select(from_json(col("json"), EVENT_SCHEMA).alias("data"))
                .select("data.*")
                .filter(col("eventId").isNotNull())
                .filter(col("userId").isNotNull())
                .filter(col("eventTime").isNotNull())
                .withColumn("amount", coalesce(col("amount"), lit(0.0)));
    }

    private static Dataset<Row> aggregate(Dataset<Row> events, JobConfig config) {
        return events
                .groupBy(window(col("eventTime"), config.windowDuration()), col("userId"))
                .agg(
                        count(lit(1)).alias("eventCount"),
                        sum("amount").alias("totalAmount")
                );
    }

    private static Dataset<Row> deduplicate(Dataset<Row> events, JobConfig config) {
        return events
                .withWatermark("eventTime", config.watermarkDelay())
                .dropDuplicates("eventId");
    }

    private static Dataset<Row> flattenWindows(Dataset<Row> aggregated) {
        return aggregated.select(
                col("userId"),
                col("window").getField("start").alias("windowStart"),
                col("window").getField("end").alias("windowEnd"),
                col("eventCount"),
                col("totalAmount")
        );
    }

    private static Dataset<Row> prepareKafkaPayload(Dataset<Row> flattened) {
        Column keyCol = concat_ws("|", col("userId"), col("windowStart").cast("string"));
        Column valueCol = to_json(struct(
                col("userId"),
                col("windowStart"),
                col("windowEnd"),
                col("eventCount"),
                col("totalAmount")
        ));
        return flattened.select(keyCol.alias("key"), valueCol.alias("value"));
    }

    static record JobConfig(
            String appName,
            String kafkaBootstrapServers,
            String kafkaTopic,
            String startingOffsets,
            long maxOffsetsPerTrigger,
            String windowDuration,
            String watermarkDelay,
            long triggerIntervalSeconds,
            ParquetSinkConfig parquetSink,
            KafkaSinkConfig kafkaSink
    ) {
        static JobConfig fromTypesafe() {
            Config base = ConfigFactory.load();
            Config merged = ConfigFactory.systemEnvironment()
                    .withFallback(ConfigFactory.systemProperties())
                    .withFallback(base)
                    .resolve();
            return fromConfig(merged);
        }

        static JobConfig fromConfig(Config config) {
            Config sparkCfg = config.getConfig("spark");
            Config modeCfg = config.getConfig("modeB");
            Config inputCfg = modeCfg.getConfig("input");
            Config sinksCfg = modeCfg.getConfig("sinks");
            Config parquetCfg = sinksCfg.getConfig("parquet");
            Config kafkaCfg = sinksCfg.getConfig("kafka");
            String sparkCheckpointBase = sparkCfg.getString("checkpointBaseDir");
            String kafkaOutputMode = kafkaCfg.hasPath("outputMode")
                    ? kafkaCfg.getString("outputMode").toLowerCase()
                    : "update";
            if (!kafkaOutputMode.equals("append")
                    && !kafkaOutputMode.equals("update")
                    && !kafkaOutputMode.equals("complete")) {
                throw new IllegalArgumentException("modeB.sinks.kafka.outputMode must be one of: append|update|complete");
            }
            return new JobConfig(
                    sparkCfg.getString("appName"),
                    inputCfg.getString("kafkaBootstrapServers"),
                    inputCfg.getString("topic"),
                    inputCfg.getString("startingOffsets"),
                    inputCfg.getLong("maxOffsetsPerTrigger"),
                    modeCfg.getString("windowDuration"),
                    modeCfg.getString("watermarkDelay"),
                    modeCfg.getLong("triggerIntervalSeconds"),
                    new ParquetSinkConfig(
                            parquetCfg.getBoolean("enabled"),
                            parquetCfg.getString("path"),
                            parquetCfg.hasPath("checkpointDir")
                                    ? parquetCfg.getString("checkpointDir")
                                    : sparkCheckpointBase + "/mode-b/parquet"
                    ),
                    new KafkaSinkConfig(
                            kafkaCfg.getBoolean("enabled"),
                            kafkaCfg.getString("topic"),
                            kafkaOutputMode,
                            kafkaCfg.hasPath("checkpointDir")
                                    ? kafkaCfg.getString("checkpointDir")
                                    : sparkCheckpointBase + "/mode-b/kafka"
                    )
            );
        }

        Duration triggerInterval() {
            return Duration.ofSeconds(triggerIntervalSeconds);
        }

        String triggerIntervalString() {
            return String.format("%d seconds", triggerIntervalSeconds);
        }

        record ParquetSinkConfig(boolean enabled, String path, String checkpointDir) {
        }

        record KafkaSinkConfig(boolean enabled, String topic, String outputMode, String checkpointDir) {
        }
    }

    static final class SinkWriter {
        private static final SinkIO DEFAULT_SINK_IO = new SparkSinkIO();

        private SinkWriter() {
        }

        static List<StreamingQuery> start(Dataset<Row> parquetDf,
                                          Dataset<Row> kafkaDf,
                                          JobConfig config) throws StreamingQueryException, TimeoutException {
            return start(parquetDf, kafkaDf, config, DEFAULT_SINK_IO);
        }

        static List<StreamingQuery> start(Dataset<Row> parquetDf,
                                          Dataset<Row> kafkaDf,
                                          JobConfig config,
                                          SinkIO sinkIO) throws StreamingQueryException, TimeoutException {
            Objects.requireNonNull(config, "config");
            List<StreamingQuery> queries = new ArrayList<>();
            if (config.parquetSink().enabled()) {
                queries.add(sinkIO.startParquet(parquetDf, config));
                LOG.info("Parquet sink enabled: {}", config.parquetSink().path());
            } else {
                LOG.info("Parquet sink disabled");
            }
            if (config.kafkaSink().enabled()) {
                queries.add(sinkIO.startKafka(kafkaDf, config));
                LOG.info("Kafka sink enabled: {}", config.kafkaSink().topic());
            } else {
                LOG.info("Kafka sink disabled");
            }
            if (queries.isEmpty()) {
                throw new IllegalStateException("At least one sink must be enabled for Mode B");
            }
            return queries;
        }

        interface SinkIO {
            StreamingQuery startParquet(Dataset<Row> df, JobConfig config) throws StreamingQueryException, TimeoutException;

            StreamingQuery startKafka(Dataset<Row> df, JobConfig config) throws StreamingQueryException, TimeoutException;
        }

        private static final class SparkSinkIO implements SinkIO {
            @Override
            public StreamingQuery startParquet(Dataset<Row> df, JobConfig config) throws TimeoutException {
                return df.writeStream()
                        .format("parquet")
                        .option("path", config.parquetSink().path())
                        .option("checkpointLocation", config.parquetSink().checkpointDir())
                        .trigger(Trigger.ProcessingTime(config.triggerIntervalString()))
                        .outputMode(OutputMode.Append())
                        .start();
            }

            @Override
            public StreamingQuery startKafka(Dataset<Row> df, JobConfig config) throws TimeoutException {
                return df.writeStream()
                        .format("kafka")
                        .option("kafka.bootstrap.servers", config.kafkaBootstrapServers())
                        .option("topic", config.kafkaSink().topic())
                        .option("checkpointLocation", config.kafkaSink().checkpointDir())
                        .trigger(Trigger.ProcessingTime(config.triggerIntervalString()))
                        .outputMode(config.kafkaSink().outputMode())
                        .start();
            }
        }
    }
}
