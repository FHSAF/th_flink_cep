// File: Flink-CEP/src/main/java/org/example/sources/deserializer/EMGDeserializationSchema.java
package org.example.sources.deserializer; // Updated package

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.example.models.EMGReading; // Updated model import
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

// No changes needed to internal logic, just package declaration
public class EMGDeserializationSchema implements KafkaRecordDeserializationSchema<EMGReading> {

    private static final Logger logger = LoggerFactory.getLogger(EMGDeserializationSchema.class);
    private transient Gson gson;

    private Gson getGson() {
        if (gson == null) { gson = new Gson(); }
        return gson;
    }

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<EMGReading> out) throws IOException {
        // (Keep existing deserialize logic from EMGDeserializationSchema)
        byte[] message = record.value();
        if (message == null) { return; }
        String jsonString = null;
        try {
            jsonString = new String(message, StandardCharsets.UTF_8);
            EMGReading reading = getGson().fromJson(jsonString, EMGReading.class);

            if (reading == null) { logger.warn("EMG Deserializer: Gson parsed JSON to null: {}", jsonString); return; }
            if (reading.getThingid() == null || reading.getThingid().trim().isEmpty()) { logger.warn("EMG Deserializer: Parsed object missing thingId: {}", jsonString); return; }
            if (reading.getTimestamp() == null || reading.getTimestamp().trim().isEmpty()) { logger.warn("EMG Deserializer: Parsed object missing timestamp for {}: {}", reading.getThingid(), jsonString); return; }

            out.collect(reading);
        } catch (JsonSyntaxException e) {
            logger.warn("EMG Deserializer: Failed parse JSON from topic {}: [{}]. Error: {}", record.topic(), jsonString, e.getMessage());
        } catch (Exception e) {
             logger.error("EMG Deserializer: Unexpected error deserializing from topic {}: [{}]. Error: {}", record.topic(), jsonString, e.getMessage(), e);
        }
    }

    @Override
    public TypeInformation<EMGReading> getProducedType() {
        return TypeInformation.of(EMGReading.class);
    }
}