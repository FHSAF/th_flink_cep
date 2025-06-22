// File: Flink-CEP/src/main/java/org/example/processing/mocap/MoCapSlidingWindowAlertProcessor.java
package org.example.processing.mocap;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.example.config.ProcessingParamsConfig;
import org.example.models.MoCapReading;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class MoCapSlidingWindowAlertProcessor {

    private static final Logger logger = LoggerFactory.getLogger(MoCapSlidingWindowAlertProcessor.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ISO_INSTANT;

    public static class JointStateEvent implements Serializable {
        public String thingId;
        public String jointName;
        public String severity;
        public double value;
        public long timestamp;
        public JointStateEvent(String thingId, String jointName, String severity, double value, long timestamp) {
            this.thingId = thingId; this.jointName = jointName; this.severity = severity; this.value = value; this.timestamp = timestamp;
        }
        public String getThingId() { return thingId; }
        public String getJointName() { return jointName; }
    }

    // This FlatMapFunction is still needed to convert MoCapReadings into classified JointStateEvents
    public static class ClassifyAndFlattenMoCap implements FlatMapFunction<MoCapReading, JointStateEvent> {
        private static final Map<String, ProcessingParamsConfig.SeverityZones> thresholds = ProcessingParamsConfig.getSelectedMocapDurationThresholds();
        private static final Set<String> jointsToMonitor = ProcessingParamsConfig.MOCAP_DURATION_JOINTS_TO_MONITOR;
        @Override
        public void flatMap(MoCapReading reading, Collector<JointStateEvent> out) {
            if (reading == null || reading.getTimestamp() == null) return;
            long timestamp;
            try {
                timestamp = Instant.parse(reading.getTimestamp()).toEpochMilli();
            } catch (Exception e) {
                logger.warn("Failed to parse timestamp: {}. Skipping record.", reading.getTimestamp());
                return;
            }
            for (String jointKey : jointsToMonitor) {
                double value = getJointValue(reading, jointKey);
                if (!Double.isNaN(value)) {
                    String severity = thresholds.get(jointKey).getSeverity(value);
                    out.collect(new JointStateEvent(reading.getThingid(), jointKey, severity, value, timestamp));
                }
            }
        }
        private double getJointValue(MoCapReading r, String key) {
             switch (key) {
                case ProcessingParamsConfig.TORSO_TILT_KEY: return r.getTorsoTilt();
                case ProcessingParamsConfig.BACK_CURVE_KEY: return r.getBackCurve();
                case ProcessingParamsConfig.SHOULDER_FLEX_KEY_RIGHT: return r.getShoulderFlexExtRight();
                case ProcessingParamsConfig.UPPERARM_ROTATION_KEY_RIGHT: return r.getUpperarmRotationRight();
                case ProcessingParamsConfig.NECK_FLEX_KEY: return r.getNeckFlexExt();
                default: return Double.NaN;
            }
        }
    }
    
    /**
     * A generic window function that calculates the percentage of events matching a target severity
     * and generates an alert if it exceeds a threshold.
     */
    public static class PercentageCheckWindowFunction extends ProcessWindowFunction<JointStateEvent, String, String, TimeWindow> {
        private final String targetSeverity;
        private final double percentageThreshold;

        public PercentageCheckWindowFunction(String targetSeverity, double percentageThreshold) {
            this.targetSeverity = targetSeverity;
            this.percentageThreshold = percentageThreshold;
        }

        @Override
        public void process(String key, Context context, Iterable<JointStateEvent> events, Collector<String> out) {
            String[] keyParts = key.split("-", 2);
            String thingId = keyParts[0];
            String jointName = keyParts[1];
            
            int matchCount = 0;
            int totalCount = 0;
            
            // Buffer events to get a reliable count, as the iterable can only be traversed once.
            List<JointStateEvent> windowEvents = new ArrayList<>();
            events.forEach(windowEvents::add);

            totalCount = windowEvents.size();
            if (totalCount == 0) return; // Skip empty windows

            for (JointStateEvent event : windowEvents) {
                if (targetSeverity.equals(event.severity)) {
                    matchCount++;
                }
            }
            
            double percentage = ((double) matchCount / totalCount) * 100.0;
            
            logger.info("WINDOW CHECK: Joint=[{}], Severity=[{}], WindowEnd={}, EventsInWindow={}, MatchCount={}, Percentage={}%",
                        jointName, targetSeverity, context.window().getEnd(), totalCount, matchCount, String.format("%.2f", percentage));

            if (percentage > this.percentageThreshold) {
                try {
                    ObjectNode alertJson = objectMapper.createObjectNode();
                    alertJson.put("thingId", thingId);
                    alertJson.put("feedbackType", "slidingWindowPostureAlert");
                    alertJson.put("alertTriggerTimestamp", context.window().getEnd());
                    alertJson.put("joint", jointName);
                    alertJson.put("severity", this.targetSeverity);
                    alertJson.put("windowStart", Instant.ofEpochMilli(context.window().getStart()).toString());
                    alertJson.put("windowEnd", Instant.ofEpochMilli(context.window().getEnd()).toString());
                    alertJson.put("badPosturePercentage", String.format("%.2f", percentage));
                    alertJson.put("eventsInWindow", totalCount);
                    
                    logger.info(">>> SLIDING WINDOW ALERT GENERATED: {}", alertJson.toString());
                    out.collect(alertJson.toString());
                } catch (Exception e) {
                     logger.error("Error formatting JSON for sliding window alert", e);
                }
            }
        }
    }


    public static DataStream<String> processSlidingWindowAlerts(DataStream<MoCapReading> sensorStream) {
        
        logger.info("--- Initializing MoCapSlidingWindowAlertProcessor ---");

        // Step 1: Flatten and classify each joint angle from the MoCap reading.
        KeyedStream<JointStateEvent, String> keyedStream = sensorStream
                .filter(Objects::nonNull)
                .flatMap(new ClassifyAndFlattenMoCap())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<JointStateEvent>forBoundedOutOfOrderness(ProcessingParamsConfig.GENERAL_EVENT_TIME_BOUNDED_OUT_OF_ORDERNESS)
                                .withTimestampAssigner((event, timestamp) -> event.timestamp)
                                .withIdleness(ProcessingParamsConfig.GENERAL_IDLENESS_DURATION))
                .keyBy(event -> event.getThingId() + "-" + event.getJointName());
        
        logger.info("Stream classified and keyed by 'thingId-jointName'. Ready for windowing.");

        // --- PATH 1: RED Alert Processing ---
        DataStream<String> redAlerts = keyedStream
                .window(SlidingEventTimeWindows.of(Duration.ofSeconds(10), Duration.ofSeconds(5)))
                .process(new PercentageCheckWindowFunction("RED", 40.0))
                .name("RedAlertWindow");

        // --- PATH 2: YELLOW Alert Processing ---
        DataStream<String> yellowAlerts = keyedStream
                .window(SlidingEventTimeWindows.of(Duration.ofSeconds(15), Duration.ofSeconds(10)))
                .process(new PercentageCheckWindowFunction("YELLOW", 50.0))
                .name("YellowAlertWindow");

        // --- Final Step: Union both alert streams into one ---
        return redAlerts.union(yellowAlerts);
    }
}