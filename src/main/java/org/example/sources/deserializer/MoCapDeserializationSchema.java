// File: Flink-CEP/src/main/java/org/example/sources/deserializer/MoCapDeserializationSchema.java
package org.example.sources.deserializer; // New package

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.example.models.MoCapReading; // Use correct model
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

// NEW Deserializer - Logic moved from DataParser
public class MoCapDeserializationSchema implements KafkaRecordDeserializationSchema<MoCapReading> {

    private static final Logger logger = LoggerFactory.getLogger(MoCapDeserializationSchema.class);
    private transient ObjectMapper objectMapper; // transient for Flink serialization
    private static final DateTimeFormatter CURRENT_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private ObjectMapper getObjectMapper() {
        if (objectMapper == null) {
            objectMapper = new ObjectMapper();
        }
        return objectMapper;
    }

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<MoCapReading> out) throws IOException {
        byte[] message = record.value();
        if (message == null) {
            return; // Skip null messages
        }

        String jsonString = null;
        try {
            jsonString = new String(message, StandardCharsets.UTF_8);
            JsonNode node = getObjectMapper().readTree(jsonString);

            // Validate and Extract Essential Fields
            String thingId = getText(node, "thingid", null);
            if (thingId == null || thingId.trim().isEmpty()) {
                logger.warn("MoCap Deserializer: Missing or empty 'thingid'. Skipping: {}", jsonString);
                return;
            }

            String timestampStr = getTextOrDefaultTimestamp(node, "timestamp");

            // Create MoCapReading object
            MoCapReading reading = new MoCapReading(
                    thingId,
                    getDouble(node, "elbow_flex_ext_left"),
                    getDouble(node, "elbow_flex_ext_right"),
                    getDouble(node, "shoulder_flex_ext_left"),
                    getDouble(node, "shoulder_flex_ext_right"),
                    getDouble(node, "shoulder_abd_add_left"),
                    getDouble(node, "shoulder_abd_add_right"),
                    getDouble(node, "lowerarm_pron_sup_left"),
                    getDouble(node, "lowerarm_pron_sup_right"),
                    getDouble(node, "upperarm_rotation_left"),
                    getDouble(node, "upperarm_rotation_right"),
                    getDouble(node, "hand_flex_ext_left"),
                    getDouble(node, "hand_flex_ext_right"),
                    getDouble(node, "hand_radial_ulnar_left"),
                    getDouble(node, "hand_radial_ulnar_right"),
                    getDouble(node, "neck_flex_ext"),
                    getDouble(node, "neck_torsion"),
                    getDouble(node, "head_tilt"),
                    getDouble(node, "torso_tilt"),
                    getDouble(node, "torso_side_tilt"),
                    getDouble(node, "back_curve"),
                    getDouble(node, "back_torsion"),
                    getDouble(node, "knee_flex_ext_left"),
                    getDouble(node, "knee_flex_ext_right"),
                    timestampStr
            );

            out.collect(reading);

        } catch (Exception e) {
            logger.error("MoCap Deserializer: Error parsing MoCapReading JSON from topic {}: {}", record.topic(), jsonString, e);
            // Optionally skip or handle error differently
        }
    }

     /**
     * Helper to get a double value from a JsonNode, defaulting to 0.0 on error/missing.
     */
    private static double getDouble(JsonNode node, String field) {
        JsonNode value = node.get(field);
        if (value != null && !value.isNull() && value.isNumber()) {
            return value.asDouble();
        } else {
            // Log warning only if field exists but is wrong type/null
            if (value != null && !value.isNull()) {
                 logger.warn("MoCap Deserializer: Numeric field '{}' has invalid type '{}'. Defaulting to 0.0", field, value.getNodeType());
            } else if (value == null) {
                 // Optionally log missing field if needed, but can be noisy
                 // logger.warn("Numeric field '{}' is missing. Defaulting to 0.0", field);
            }
            return 0.0;
        }
    }

    /**
     * Helper to get a text value from a JsonNode, defaulting to a specified value.
     */
    private static String getText(JsonNode node, String field, String defaultValue) {
        JsonNode value = node.get(field);
        if (value != null && !value.isNull() && value.isTextual()) {
            return value.asText();
        } else {
            // Log warning only if field exists but is wrong type/null
             if (value != null && !value.isNull()) {
                 logger.warn("MoCap Deserializer: Text field '{}' has invalid type '{}'. Returning default.", field, value.getNodeType());
            }
            return defaultValue;
        }
    }

     /**
     * Helper specifically for timestamp: gets text value or defaults to current formatted time string.
     */
    private static String getTextOrDefaultTimestamp(JsonNode node, String field) {
        String timestampStr = getText(node, field, null);
        if (timestampStr == null || timestampStr.trim().isEmpty()) {
            // String defaultTimestamp = LocalDateTime.now().format(CURRENT_TIME_FORMATTER);
            String defaultTimestamp = Instant.now().toString(); // ISO 8601 format
            logger.warn("MoCap Deserializer: Missing/empty timestamp field '{}'. Defaulting to current time: {}", field, defaultTimestamp);
            return defaultTimestamp;
        } else {
            // Basic format validation could be added here if needed
            return timestampStr.trim();
        }
    }


    @Override
    public TypeInformation<MoCapReading> getProducedType() {
        return TypeInformation.of(MoCapReading.class);
    }
}