
// File: Flink-CEP/src/main/java/org/example/sinks/kafka/EMGFatigueAlertKafkaSink.java
package org.example.sinks.kafka; // Updated package

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Properties; // Import Properties
import org.apache.kafka.clients.producer.ProducerConfig; // Import ProducerConfig

// Renamed from EMGKafkaSink
public class EMGFatigueAlertKafkaSink {

    private static final Logger logger = LoggerFactory.getLogger(EMGFatigueAlertKafkaSink.class);

    // Default topic can be moved to KafkaConfig if preferred
    private static final String DEFAULT_EMG_FATIGUE_ALERT_TOPIC = "emg_fatigue_alerts";

    /**
     * Creates a Flink KafkaSink for EMG Fatigue Alert JSON strings.
     *
     * @param brokers   Comma-separated list of Kafka broker addresses.
     * @param topic     The specific Kafka topic to write alerts to. If null or empty, uses default.
     * @return Configured KafkaSink<String>
     */
    public static KafkaSink<String> getKafkaSink(String brokers, String topic) {
        // (Keep the existing getKafkaSink logic from EMGKafkaSink)
         if (brokers == null || brokers.isEmpty()) {
            throw new IllegalArgumentException("Kafka bootstrap servers cannot be null or empty for EMGFatigueAlertKafkaSink");
        }

        String targetTopic = (topic != null && !topic.isEmpty()) ? topic : DEFAULT_EMG_FATIGUE_ALERT_TOPIC;
        logger.info("Configuring Kafka Sink for EMG Fatigue Alerts. Brokers: {}, Topic: {}", brokers, targetTopic);

        return KafkaSink.<String>builder()
                .setBootstrapServers(brokers)
                .setKafkaProducerConfig(getKafkaProperties()) // Apply common producer props
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(targetTopic)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                )
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();
    }

    /**
     * Creates a Flink KafkaSink using the default topic name.
     * @param brokers Comma-separated list of Kafka broker addresses.
     * @return Configured KafkaSink<String>
     */
    public static KafkaSink<String> getKafkaSink(String brokers) {
        return getKafkaSink(brokers, null); // Call the main method with null topic
    }

     /**
     * Configures basic Kafka Producer properties.
     * @return Properties object for Kafka producer.
     */
    private static Properties getKafkaProperties() {
        // (Keep the existing getKafkaProperties logic)
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, "3");
        return properties;
    }
}
