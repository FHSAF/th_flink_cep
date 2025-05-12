// File: Flink-CEP/src/main/java/org/example/sources/provider/EyeGazeKafkaSourceProvider.java
package org.example.sources.provider; // Updated package

import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.example.models.EyeGazeReading; // Updated model import
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.example.sources.deserializer.EyeGazeDeserializationSchema; // Updated deserializer import

// Renamed from EyeGazeSourceProvider
public class EyeGazeKafkaSourceProvider {

    private static final Logger logger = LoggerFactory.getLogger(EyeGazeKafkaSourceProvider.class);

    /**
     * Gets a configured KafkaSource for Eye Gaze Attention data.
     *
     * @param brokers Comma-separated list of Kafka broker addresses.
     * @param topic   The Kafka topic containing the gaze attention JSON messages.
     * @param groupId Kafka consumer group ID for this source instance.
     * @return A KafkaSource<EyeGazeReading>
     * @throws IllegalArgumentException if brokers, topic, or groupId are null or empty.
     */
    public static KafkaSource<EyeGazeReading> getEyeGazeKafkaSource(String brokers, String topic, String groupId) {
        // (Keep existing logic, just ensure EyeGazeDeserializationSchema is used)
        if (brokers == null || brokers.isEmpty()) {
            throw new IllegalArgumentException("Kafka bootstrap servers cannot be null or empty for EyeGazeKafkaSourceProvider.");
        }
        if (topic == null || topic.isEmpty()) {
            throw new IllegalArgumentException("Kafka topic cannot be null or empty for EyeGazeKafkaSourceProvider.");
        }
        if (groupId == null || groupId.isEmpty()) {
            throw new IllegalArgumentException("Kafka consumer group ID cannot be null or empty for EyeGazeKafkaSourceProvider.");
        }

        logger.info("Configuring Eye Gaze Kafka Source:");
        logger.info("  Brokers: {}", brokers);
        logger.info("  Topic: {}", topic);
        logger.info("  Group ID: {}", groupId);

        try {
            KafkaSource<EyeGazeReading> source = KafkaSource.<EyeGazeReading>builder()
                    .setBootstrapServers(brokers)
                    .setTopics(topic)
                    .setGroupId(groupId)
                    .setStartingOffsets(OffsetsInitializer.latest())
                    .setDeserializer(new EyeGazeDeserializationSchema()) // Uses the custom deserializer
                    .build();

            logger.info("Eye Gaze Kafka Source configured successfully for topic '{}'.", topic);
            return source;

        } catch (Exception e) {
            logger.error("Failed to build Eye Gaze Kafka Source for topic '{}'", topic, e);
            throw new RuntimeException("Failed to build Eye Gaze Kafka Source", e);
        }
    }
}