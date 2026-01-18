package com.example.streaming;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ModeBStreamingJobTest {

    @Test
    void configLoadsAllModeBFields() {
        Config cfg = ConfigFactory.parseResources("application-test.conf");
        ModeBStreamingJob.JobConfig config = ModeBStreamingJob.JobConfig.fromConfig(cfg);
        assertEquals("test-app", config.appName());
        assertEquals("test:9092", config.kafkaBootstrapServers());
        assertEquals("mode-b-input", config.kafkaTopic());
        assertEquals("earliest", config.startingOffsets());
        assertEquals(500, config.maxOffsetsPerTrigger());
        assertEquals("5 minutes", config.windowDuration());
        assertEquals("15 minutes", config.watermarkDelay());
        assertEquals(45, config.triggerIntervalSeconds());
        assertEquals("file:///tmp/mode-b/parquet", config.parquetSink().path());
        assertEquals("file:///tmp/test-checkpoints/mode-b/parquet", config.parquetSink().checkpointDir());
        assertEquals("mode-b-output", config.kafkaSink().topic());
        assertEquals("update", config.kafkaSink().outputMode());
        assertEquals("file:///tmp/test-checkpoints/mode-b/kafka", config.kafkaSink().checkpointDir());
    }

    @Test
    void sinkWriterRequiresAtLeastOneSink() {
        ModeBStreamingJob.JobConfig config = new ModeBStreamingJob.JobConfig(
                "app",
                "bootstrap",
                "topic",
                "latest",
                100,
                "5 minutes",
                "10 minutes",
                60,
                new ModeBStreamingJob.JobConfig.ParquetSinkConfig(false, "/tmp/none", "/tmp/none"),
                new ModeBStreamingJob.JobConfig.KafkaSinkConfig(false, "topic", "update", "/tmp/none")
        );
        ModeBStreamingJob.SinkWriter.SinkIO sinkIO = new FakeSinkIO();
        assertThrows(IllegalStateException.class, () -> ModeBStreamingJob.SinkWriter.start(null, null, config, sinkIO));
    }

    @Test
    void sinkWriterStartsEnabledSinks() throws Exception {
        ModeBStreamingJob.JobConfig config = new ModeBStreamingJob.JobConfig(
                "app",
                "bootstrap",
                "topic",
                "latest",
                100,
                "5 minutes",
                "10 minutes",
                60,
                new ModeBStreamingJob.JobConfig.ParquetSinkConfig(true, "/tmp/parquet", "/tmp/ckpt1"),
                new ModeBStreamingJob.JobConfig.KafkaSinkConfig(true, "topic", "update", "/tmp/ckpt2")
        );
        FakeSinkIO sinkIO = new FakeSinkIO();
        List<StreamingQuery> queries = ModeBStreamingJob.SinkWriter.start(null, null, config, sinkIO);
        assertEquals(2, queries.size());
        assertEquals(true, sinkIO.parquetStarted);
        assertEquals(true, sinkIO.kafkaStarted);
    }

    private static final class FakeSinkIO implements ModeBStreamingJob.SinkWriter.SinkIO {
        boolean parquetStarted;
        boolean kafkaStarted;

        @Override
        public StreamingQuery startParquet(Dataset<Row> df, ModeBStreamingJob.JobConfig config) {
            parquetStarted = true;
            return org.mockito.Mockito.mock(StreamingQuery.class);
        }

        @Override
        public StreamingQuery startKafka(Dataset<Row> df, ModeBStreamingJob.JobConfig config) {
            kafkaStarted = true;
            return org.mockito.Mockito.mock(StreamingQuery.class);
        }
    }
}
