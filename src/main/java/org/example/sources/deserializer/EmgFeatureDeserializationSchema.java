// File: Flink-CEP/src/main/java/org/example/sources/deserializer/EmgFeatureDeserializationSchema.java
package org.example.sources.deserializer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.example.models.EmgFeatureMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class EmgFeatureDeserializationSchema implements KafkaRecordDeserializationSchema<EmgFeatureMessage> {

    private static final Logger logger = LoggerFactory.getLogger(EmgFeatureDeserializationSchema.class);
    
    private transient ObjectMapper objectMapper;

    private ObjectMapper getObjectMapper() {
        if (objectMapper == null) {
            objectMapper = new ObjectMapper();
            objectMapper.setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE);
        }
        return objectMapper;
    }

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<EmgFeatureMessage> out) throws IOException {
        byte[] messageBytes = record.value();
        if (messageBytes == null || messageBytes.length == 0) {
            return; // Skip empty messages
        }

        String jsonString = new String(messageBytes, StandardCharsets.UTF_8);

        try {
            EmgFeatureMessage message = getObjectMapper().readValue(jsonString, EmgFeatureMessage.class);

            if (message != null) {
                if (message.getThingId() == null) {
                    logger.warn("Parsed EmgFeatureMessage is missing a thingId: {}", jsonString);
                    return;
                }
                out.collect(message);
            }
        } catch (Exception e) {
            logger.warn("Failed to deserialize EmgFeatureMessage JSON from topic {}: [{}]. Error: {}", 
                        record.topic(), jsonString, e.getMessage());
        }
    }

    @Override
    public TypeInformation<EmgFeatureMessage> getProducedType() {
        return TypeInformation.of(EmgFeatureMessage.class);
    }
}