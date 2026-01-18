package com.example.streaming;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import java.time.Duration;
import java.util.concurrent.TimeoutException;

public final class ModeAStreamingJob {
    private static final Logger LOG = LoggerFactory.getLogger(ModeAStreamingJob.class);

    public static void main(String[] args) throws StreamingQueryException, TimeoutException {
        JobConfig config = JobConfig.fromTypesafe();
        SparkSession spark = SparkSession.builder()
                .appName(config.appName())
                .getOrCreate();

        Dataset<Row> sourceDf = spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", config.kafkaBootstrapServers())
                .option("subscribe", config.kafkaTopic())
                .option("startingOffsets", config.startingOffsets())
                .option("maxOffsetsPerTrigger", String.valueOf(config.maxOffsetsPerTrigger()))
                .load();

        Dataset<Row> parsed = sourceDf.selectExpr(
                "CAST(value AS STRING) as json"
        ).selectExpr(
                "from_json(json, 'eventId STRING, eventTime TIMESTAMP, payload STRING') as data"
        ).select("data.*");

        Dataset<Row> deduped = parsed
                .withWatermark("eventTime", config.watermarkDelay())
                .dropDuplicates("eventId");

        StreamingQuery query = SinkWriter.write(deduped, config);
        LOG.info("Mode A streaming query started");
        query.awaitTermination();
    }

    static record JobConfig(
            String appName,
            String kafkaBootstrapServers,
            String kafkaTopic,
            String startingOffsets,
            long maxOffsetsPerTrigger,
            String watermarkDelay,
            SinkType sinkType,
            String parquetPath,
            String sinkKafkaTopic,
            String checkpointDir,
            long triggerInterval

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
            Config modeCfg = config.getConfig("modeA");
            Config sinkCfg = modeCfg.getConfig("sink");
            return new JobConfig(
                    sparkCfg.getString("appName"),
                    modeCfg.getConfig("input").getString("kafkaBootstrapServers"),
                    modeCfg.getConfig("input").getString("topic"),
                    modeCfg.getConfig("input").getString("startingOffsets"),
                    modeCfg.getConfig("input").getLong("maxOffsetsPerTrigger"),
                    modeCfg.getString("watermarkDelay"),
                    SinkType.valueOf(sinkCfg.getString("type").toUpperCase()),
                    sinkCfg.getString("parquetPath"),
                    sinkCfg.getString("kafkaTopic"),
                    sparkCfg.getString("checkpointDir"),
                    sinkCfg.getLong("triggerIntervalSeconds")
            );
        }
    }

    enum SinkType {
        PARQUET,
        KAFKA
    }

    static final class SinkWriter {
        private static final SinkIO DEFAULT_SINK_IO = new SparkSinkIO();

        private SinkWriter() {
        }

        static StreamingQuery write(Dataset<Row> df, JobConfig config) throws StreamingQueryException, TimeoutException {
            return write(df, config, DEFAULT_SINK_IO);
        }

        static StreamingQuery write(Dataset<Row> df, JobConfig config, SinkIO sinkIO) throws StreamingQueryException, TimeoutException {
            return switch (config.sinkType()) {
                case PARQUET -> sinkIO.writeParquet(df, config);
                case KAFKA -> sinkIO.writeKafka(df, config);
            };
        }

        interface SinkIO {
            StreamingQuery writeParquet(Dataset<Row> df, JobConfig config) throws StreamingQueryException, TimeoutException;

            StreamingQuery writeKafka(Dataset<Row> df, JobConfig config) throws StreamingQueryException, TimeoutException;
        }

        private static final class SparkSinkIO implements SinkIO {
            @Override
            public StreamingQuery writeParquet(Dataset<Row> df, JobConfig config) throws StreamingQueryException, TimeoutException {
                return df
                        .writeStream()
                        .format("parquet")
                        .option("path", config.parquetPath())
                        .option("checkpointLocation", config.checkpointDir())
                        .trigger(Trigger.ProcessingTime(config.triggerInterval()))
                        .outputMode(OutputMode.Append())
                        .start();
            }

            @Override
            public StreamingQuery writeKafka(Dataset<Row> df, JobConfig config) throws StreamingQueryException, TimeoutException {
                return df
                        .selectExpr("CAST(eventId AS STRING) AS key", "to_json(struct(*)) AS value")
                        .writeStream()
                        .format("kafka")
                        .option("kafka.bootstrap.servers", config.kafkaBootstrapServers())
                        .option("topic", config.sinkKafkaTopic())
                        .option("checkpointLocation", config.checkpointDir())
                        .trigger(Trigger.ProcessingTime(config.triggerInterval()))
                        .outputMode(OutputMode.Append())
                        .start();
            }
        }
    }
}
