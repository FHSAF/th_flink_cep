// File: Flink-CEP/src/main/java/org/example/sources/provider/MoCapKafkaSourceProvider.java
package org.example.sources.provider; // Updated package

import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.example.config.KafkaConfig;
import org.example.models.MoCapReading; // Import Correct Model
import org.example.sources.deserializer.MoCapDeserializationSchema; // Import New Deserializer
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// Renamed from MoCapSourceProvider
public class MoCapKafkaSourceProvider {
    private static final Logger logger = LoggerFactory.getLogger(MoCapKafkaSourceProvider.class);

    /**
     * Creates and configures a KafkaSource for reading Rokoko MoCap data.
     * Uses MoCapDeserializationSchema to parse JSON into MoCapReading objects.
     *
     * @return Configured KafkaSource<MoCapReading>
     * @throws IllegalStateException if required KafkaConfig values are missing.
     */
    public static KafkaSource<MoCapReading> getKafkaSource() { // Return type updated
        try {
            logger.info("#############################################");
            logger.info("Initializing MoCap Kafka Source Configuration...");
            logger.info("  Brokers: {}", KafkaConfig.BOOTSTRAP_SERVERS);
            logger.info("  Topic: {}", KafkaConfig.MOCAP_SOURCE_TOPIC);
            logger.info("  Group ID: {}", KafkaConfig.MOCAP_GROUP_ID); // Use base group ID or a specific one
            logger.info("#############################################");

            if (KafkaConfig.BOOTSTRAP_SERVERS == null || KafkaConfig.MOCAP_SOURCE_TOPIC == null || KafkaConfig.MOCAP_GROUP_ID == null) {
                throw new IllegalStateException("Kafka configuration (servers, rokoko topic, group id) missing in KafkaConfig.");
            }

            KafkaSource<MoCapReading> kafkaSource = KafkaSource.<MoCapReading>builder() // Output type MoCapReading
                    .setBootstrapServers(KafkaConfig.BOOTSTRAP_SERVERS)
                    .setTopics(KafkaConfig.MOCAP_SOURCE_TOPIC)
                    .setGroupId(KafkaConfig.MOCAP_GROUP_ID)
                    .setStartingOffsets(OffsetsInitializer.latest())
                    // Use the new Deserialization Schema
                    .setDeserializer(new MoCapDeserializationSchema()) // UPDATED
                    .build();

            logger.info("MoCap Kafka Source configured successfully.");
            return kafkaSource;

        } catch (Exception e) {
            logger.error("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@");
            logger.error("Error initializing MoCap Kafka Source", e);
            logger.error("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@");
            throw new RuntimeException("Error initializing MoCap Kafka Source", e);
        }
    }
}