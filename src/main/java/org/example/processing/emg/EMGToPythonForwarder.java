// File: Flink-CEP/src/main/java/org/example/processing/emg/EMGToPythonForwarder.java
package org.example.processing.emg;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.util.Collector;
import org.example.models.EMGReading;
import org.example.config.ProcessingParamsConfig; // To get MUSCLES_TO_MONITOR_FOR_MDF

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Set;

public class EMGToPythonForwarder {

    private static final Logger logger = LoggerFactory.getLogger(EMGToPythonForwarder.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final DateTimeFormatter ISO_TIMESTAMP_FORMATTER = DateTimeFormatter.ISO_OFFSET_DATE_TIME; // Assuming EMGReading.timestamp is ISO

    // Define the specific muscles your Python script is interested in for MDF
    // This could also be passed as a parameter if it might change.
    // For now, referencing ProcessingParamsConfig if it defines this specific set,
    // or define it locally. Let's assume ProcessingParamsConfig will have it.
    // Ensure ProcessingParamsConfig.java has:
    // public static final Set<String> EMG_MUSCLES_FOR_MDF_PYTHON = Set.of(
    // "trapezius_right",
    // "deltoids_right",
    // "latismus_dorsi_right" // Check spelling: often "latissimus_dorsi_right"
    // );
    // Using the constant name from your python script for consistency.
    private static final Set<String> MUSCLES_TO_FORWARD = ProcessingParamsConfig.EMG_MUSCLES_TO_MONITOR_FOR_MDF;


    public static DataStream<String> forwardMuscleDataForPython(DataStream<EMGReading> emgStream) {
        return emgStream.flatMap(new FlatMapFunction<EMGReading, String>() {
            @Override
            public void flatMap(EMGReading reading, Collector<String> out) throws Exception {
                if (reading == null || reading.getThingid() == null || reading.getTimestamp() == null) {
                    return;
                }

                long sourceTimestampMs;
                try {
                    // Parse the original timestamp string from EMGReading to epoch milliseconds
                    Instant instant = Instant.from(ISO_TIMESTAMP_FORMATTER.parse(reading.getTimestamp()));
                    sourceTimestampMs = instant.toEpochMilli();
                } catch (DateTimeParseException e) {
                    logger.warn("Could not parse timestamp for EMG reading: {} for thingid: {}. Skipping.",
                            reading.getTimestamp(), reading.getThingid(), e);
                    return;
                }

                long flinkSentTimestampMs = System.currentTimeMillis(); // Timestamp when Flink processes this

                // Check and emit data for each target muscle
                // NOTE: Ensure the getter names in EMGReading match these strings exactly.
                // And ensure your Python script's MUSCLES_TO_MONITOR_FOR_MDF uses these exact names.

                if (MUSCLES_TO_FORWARD.contains("deltoids_right")) {
                    // Assuming getDeltoids_right() returns 0.0 if not present or not applicable for that source message
                    double value = reading.getDeltoids_right();
                    if (value != 0.0 || isMuscleExpectedFromSource(reading, "deltoids_right")) { // Send if non-zero or if it's expected (e.g. from device 02)
                        ObjectNode node = createMuscleJsonNode(reading.getThingid(), sourceTimestampMs, flinkSentTimestampMs, "deltoids_right", value);
                        out.collect(node.toString());
                    }
                }

                if (MUSCLES_TO_FORWARD.contains("trapezius_right")) {
                    double value = reading.getTrapezius_right();
                     if (value != 0.0 || isMuscleExpectedFromSource(reading, "trapezius_right")) { // Send if non-zero or if it's expected (e.g. from device 03)
                        ObjectNode node = createMuscleJsonNode(reading.getThingid(), sourceTimestampMs, flinkSentTimestampMs, "trapezius_right", value);
                        out.collect(node.toString());
                    }
                }
                
                // Corrected name based on typical anatomy: "latissimus_dorsi_right"
                // Your Python used "latismus_right", ensure consistency. I'll use the common one here.
                String latissimusKey = "latissimus_dorsi_right"; 
                if (MUSCLES_TO_FORWARD.contains("latismus_dorsi_right")) { // Checking against your python key
                    latissimusKey = "latismus_dorsi_right"; // Use the key from python config
                } else if (!MUSCLES_TO_FORWARD.contains(latissimusKey)){ // If neither python key nor standard key is present
                     // Potentially log that a specific muscle name is not in the forward list if you expect it to be
                }


                if (MUSCLES_TO_FORWARD.contains(latissimusKey)) { // Use the determined key
                    double value = reading.getLatissimus_right(); // Assumes EMGReading.java has this getter
                     if (value != 0.0 || isMuscleExpectedFromSource(reading, latissimusKey)) {
                        ObjectNode node = createMuscleJsonNode(reading.getThingid(), sourceTimestampMs, flinkSentTimestampMs, latissimusKey, value);
                        out.collect(node.toString());
                    }
                }
            }
        });
    }

    private static ObjectNode createMuscleJsonNode(String thingid, long sourceTsMs, long flinkSentTsMs, String muscleName, double value) {
        ObjectNode node = objectMapper.createObjectNode();
        node.put("thingid", thingid);
        node.put("sourceTimestampMs", sourceTsMs);       // Original sensor event time
        node.put("flinkSentTimestampMs", flinkSentTsMs); // Flink processing time
        node.put("muscle", muscleName);
        node.put("value", value);                        // EMG value in uV
        return node;
    }
    
    // Helper to determine if a muscle value (even if 0.0) should be sent.
    // This is tricky because EMGReading aggregates all possible muscles.
    // We only want to send a reading for a muscle if it's genuinely from the source device
    // that usually reports that muscle. For example, 'deltoids_right' comes from device k_myontech_shirt02_emg.
    // This function would ideally need to know the source topic/device of the EMGReading.
    // Since EMGReading doesn't store its original topic, we make a simplifying assumption:
    // If the EMGReading object has non-zero values for *any* muscle typically associated with a certain device,
    // then a zero value for a target muscle from that device group is still valid to send.
    // This is a heuristic. A more robust way is if EMGReading itself knew its source topic.
    private static boolean isMuscleExpectedFromSource(EMGReading reading, String muscleName) {
        // Example: "deltoids_right" is from "k_myontech_shirt02_emg" (right arm sensor)
        if (muscleName.equals("deltoids_right")) {
            return reading.getDeltoids_right() != 0 || reading.getTriceps_right() != 0 || reading.getBiceps_right() != 0 || reading.getWrist_extensors_right() != 0 || reading.getWrist_flexor_right() != 0;
        }
        // Example: "trapezius_right", "latissimus_dorsi_right" are from "k_myontech_shirt03_emg" (trunk sensor)
        if (muscleName.equals("trapezius_right") || muscleName.equals("latissimus_dorsi_right") || muscleName.equals("latismus_dorsi_right")) {
             return reading.getTrapezius_left() != 0 || reading.getTrapezius_right() != 0 || reading.getPectoralis_left() != 0 || reading.getPectoralis_right() != 0 || reading.getLatissimus_left() != 0 || reading.getLatissimus_right() != 0;
        }
        // By default, if we can't determine the source, don't send zero values unless explicitly non-zero.
        return false; 
    }
}