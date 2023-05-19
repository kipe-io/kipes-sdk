package io.kipe.streams.kafka.examples.zomatorideranalysis.runner;

import io.kipe.streams.kafka.examples.zomatorideranalysis.stream.ZomatoAnalysisStream;
import io.micronaut.configuration.kafka.streams.ConfiguredStreamBuilder;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ZomotoAnalysisStreamRunner {
    private static final Logger LOG = LoggerFactory.getLogger(ZomotoAnalysisStreamRunner.class);
    private static final String APPLICATION_ID = "zomoto-rider-analysis";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";

    public static void main(String[] args) {
        // Instantiate ZomatoAnalysisStream and ConfiguredStreamBuilder
        ZomatoAnalysisStream riderStatsApp = new ZomatoAnalysisStream();

        Properties properties = getProperties();

        ConfiguredStreamBuilder streamBuilder = new ConfiguredStreamBuilder(properties);
        // Build the Topology
        Topology topology = riderStatsApp.zomatoAnalysisStream(streamBuilder);

        // Initialize and start the KafkaStreams instance
        KafkaStreams kafkaStreams = new KafkaStreams(topology, properties);

        kafkaStreams.setUncaughtExceptionHandler(e -> {
            LOG.error("Caught exception in thread: ", e);
            return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_APPLICATION;
        });

        kafkaStreams.start();

        // Add shutdown hook to gracefully close the KafkaStreams instance
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOG.info("Shutting down stream");
            kafkaStreams.close();
        }));
    }

    private static Properties getProperties() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_ID);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        return props;
    }
}
