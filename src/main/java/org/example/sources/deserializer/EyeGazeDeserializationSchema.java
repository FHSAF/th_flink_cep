// File: Flink-CEP/src/main/java/org/example/sources/deserializer/EyeGazeDeserializationSchema.java
package org.example.sources.deserializer; // Updated package

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.example.models.EyeGazeReading; // Updated model import
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

// No changes needed to internal logic, just package declaration
public class EyeGazeDeserializationSchema implements KafkaRecordDeserializationSchema<EyeGazeReading> {

    private static final Logger logger = LoggerFactory.getLogger(EyeGazeDeserializationSchema.class);
    private transient Gson gson;

    private Gson getGson() {
        if (gson == null) { gson = new Gson(); }
        return gson;
    }

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<EyeGazeReading> out) throws IOException {
        // (Keep existing deserialize logic from EyeGazeDeserializationSchema)
        byte[] message = record.value();
        if (message == null) { return; }
        String jsonString = null;
        try {
            jsonString = new String(message, StandardCharsets.UTF_8);
            logger.info("EyeGaze Deserializer: Received raw message from Kafka topic {}: {}", record.topic(), jsonString);
            EyeGazeReading reading = getGson().fromJson(jsonString, EyeGazeReading.class);

            if (reading == null) { logger.warn("EyeGaze Deserializer: Gson parsed JSON to null: {}", jsonString); return; }
            if (reading.getThingid() == null || reading.getThingid().trim().isEmpty()) { logger.warn("EyeGaze Deserializer: Parsed object missing thingId: {}", jsonString); return; }
            if (reading.getTimestamp() == null || reading.getTimestamp().trim().isEmpty()) { logger.warn("EyeGaze Deserializer: Parsed object missing timestamp for {}: {}", reading.getThingid(), jsonString); return; }

            out.collect(reading);
        } catch (JsonSyntaxException e) {
            logger.warn("EyeGaze Deserializer: Failed parse JSON from topic {}: [{}]. Error: {}", record.topic(), jsonString, e.getMessage());
        } catch (Exception e) {
             logger.error("EyeGaze Deserializer: Unexpected error deserializing from topic {}: [{}]. Error: {}", record.topic(), jsonString, e.getMessage(), e);
        }
    }

    @Override
    public TypeInformation<EyeGazeReading> getProducedType() {
        return TypeInformation.of(EyeGazeReading.class);
    }
}
