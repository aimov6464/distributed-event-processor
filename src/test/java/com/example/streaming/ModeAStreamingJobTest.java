package com.example.streaming;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ModeAStreamingJobTest {

    @Test
    void jobConfigLoadsFromCustomConfig() {
        Config cfg = ConfigFactory.parseResources("application-test.conf").resolve();
        ModeAStreamingJob.JobConfig jobConfig = ModeAStreamingJob.JobConfig.fromConfig(cfg);
        assertEquals("test-app", jobConfig.appName());
        assertEquals("test:9092", jobConfig.kafkaBootstrapServers());
        assertEquals("test-topic", jobConfig.kafkaTopic());
        assertEquals("earliest", jobConfig.startingOffsets());
        assertEquals(100, jobConfig.maxOffsetsPerTrigger());
        assertEquals("2 minutes", jobConfig.watermarkDelay());
        assertEquals(ModeAStreamingJob.SinkType.KAFKA, jobConfig.sinkType());
        assertEquals("file:///tmp/test-parquet", jobConfig.parquetPath());
        assertEquals("out-test", jobConfig.sinkKafkaTopic());
        assertEquals("file:///tmp/test-checkpoint", jobConfig.checkpointDir());
        assertEquals(Duration.ofSeconds(30), jobConfig.triggerInterval());
    }

    @Test
    void sinkWriterRoutesToParquet() throws TimeoutException, org.apache.spark.sql.streaming.StreamingQueryException {
        ModeAStreamingJob.JobConfig config = new ModeAStreamingJob.JobConfig(
                "app",
                "bootstrap",
                "topic",
                "latest",
                1,
                "1 minute",
                ModeAStreamingJob.SinkType.PARQUET,
                "file:///tmp/parquet",
                "sink-topic",
                "file:///tmp/checkpoints",
                30
        );
        ModeAStreamingJob.SinkWriter.SinkIO io = new FakeSinkIO();
        ModeAStreamingJob.SinkWriter.write(null, config, io);
        assertEquals("parquet", ((FakeSinkIO) io).lastCall);
    }

    @Test
    void sinkWriterRoutesToKafka() throws TimeoutException, org.apache.spark.sql.streaming.StreamingQueryException {
        ModeAStreamingJob.JobConfig config = new ModeAStreamingJob.JobConfig(
                "app",
                "bootstrap",
                "topic",
                "latest",
                1,
                "1 minute",
                ModeAStreamingJob.SinkType.KAFKA,
                "file:///tmp/parquet",
                "sink-topic",
                "file:///tmp/checkpoints",
                30
        );
        ModeAStreamingJob.SinkWriter.SinkIO io = new FakeSinkIO();
        ModeAStreamingJob.SinkWriter.write(null, config, io);
        assertEquals("kafka", ((FakeSinkIO) io).lastCall);
    }

    private static final class FakeSinkIO implements ModeAStreamingJob.SinkWriter.SinkIO {
        String lastCall;
        StreamingQuery lastQuery = org.mockito.Mockito.mock(StreamingQuery.class);

        @Override
        public StreamingQuery writeParquet(org.apache.spark.sql.Dataset<org.apache.spark.sql.Row> df, ModeAStreamingJob.JobConfig config) {
            lastCall = "parquet";
            return lastQuery;
        }

        @Override
        public StreamingQuery writeKafka(org.apache.spark.sql.Dataset<org.apache.spark.sql.Row> df, ModeAStreamingJob.JobConfig config) {
            lastCall = "kafka";
            return lastQuery;
        }
    }
}
