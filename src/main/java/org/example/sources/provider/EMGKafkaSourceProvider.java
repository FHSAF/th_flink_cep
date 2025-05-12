// File: Flink-CEP/src/main/java/org/example/sources/provider/EMGKafkaSourceProvider.java
package org.example.sources.provider; // Updated package

import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.example.models.EMGReading; // Updated model import
import org.example.config.KafkaConfig;
import org.example.sources.deserializer.EMGDeserializationSchema; // Updated deserializer import
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

// Renamed from EMGSourceProvider
public class EMGKafkaSourceProvider {

    private static final Logger logger = LoggerFactory.getLogger(EMGKafkaSourceProvider.class);

    /**
     * Gets a configured KafkaSource for EMG data using topics from KafkaConfig.
     *
     * @param brokers Comma-separated list of Kafka broker addresses.
     * @param groupId Kafka consumer group ID for this source instance.
     * @return A KafkaSource<EMGReading>
     * @throws IllegalArgumentException if brokers or groupId are null or empty.
     * @throws IllegalStateException if required KafkaConfig values are missing.
     */
    public static KafkaSource<EMGReading> getEMGKafkaSource(String brokers, String groupId) {
        // (Keep existing logic, just ensure EMGDeserializationSchema is used)
         if (brokers == null || brokers.isEmpty()) {
            throw new IllegalArgumentException("Kafka bootstrap servers cannot be null or empty.");
        }
        if (groupId == null || groupId.isEmpty()) {
            throw new IllegalArgumentException("Kafka consumer group ID cannot be null or empty.");
        }

        if (KafkaConfig.EMG_SOURCE_TOPIC_LEFT_ARM == null ||
            KafkaConfig.EMG_SOURCE_TOPIC_RIGHT_ARM == null || KafkaConfig.EMG_SOURCE_TOPIC_TRUNK == null) {
             throw new IllegalStateException("Required Kafka EMG topics missing in KafkaConfig class.");
        }
        List<String> topics = Arrays.asList(
                KafkaConfig.EMG_SOURCE_TOPIC_LEFT_ARM,
                KafkaConfig.EMG_SOURCE_TOPIC_RIGHT_ARM,
                KafkaConfig.EMG_SOURCE_TOPIC_TRUNK
        );

        logger.info("Configuring EMG Kafka Source:");
        logger.info("  Brokers: {}", brokers);
        logger.info("  Topics: {}", topics);
        logger.info("  Group ID: {}", groupId);

        try {
            KafkaSource<EMGReading> source = KafkaSource.<EMGReading>builder()
                    .setBootstrapServers(brokers)
                    .setTopics(topics)
                    .setGroupId(groupId)
                    .setStartingOffsets(OffsetsInitializer.latest())
                    .setDeserializer(new EMGDeserializationSchema()) // Uses the custom deserializer
                    .build();
            logger.info("EMG Kafka Source configured successfully.");
            return source;
        } catch (Exception e) {
            logger.error("Failed to build EMG Kafka Source", e);
            throw new RuntimeException("Failed to build EMG Kafka Source", e);
        }
    }
}