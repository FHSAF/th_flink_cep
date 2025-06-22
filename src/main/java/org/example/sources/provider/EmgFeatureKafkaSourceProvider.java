// File: Flink-CEP/src/main/java/org/example/sources/provider/EmgFeatureKafkaSourceProvider.java
package org.example.sources.provider;

import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.example.models.EmgFeatureMessage;
import org.example.sources.deserializer.EmgFeatureDeserializationSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EmgFeatureKafkaSourceProvider {

    private static final Logger logger = LoggerFactory.getLogger(EmgFeatureKafkaSourceProvider.class);

    public static KafkaSource<EmgFeatureMessage> getEmgFeatureKafkaSource(String brokers, String topic, String groupId) {
        if (brokers == null || brokers.isEmpty() || topic == null || topic.isEmpty() || groupId == null || groupId.isEmpty()) {
            throw new IllegalArgumentException("Kafka brokers, topic, and groupId cannot be null or empty.");
        }

        logger.info("Configuring EMG Feature Kafka Source for topic '{}' with group '{}'", topic, groupId);

        return KafkaSource.<EmgFeatureMessage>builder()
                .setBootstrapServers(brokers)
                .setTopics(topic)
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setDeserializer(new EmgFeatureDeserializationSchema())
                .build();
    }
}