// File: Flink-CEP/src/main/java/org/example/processing/mocap/MoCapAverageAngleProcessor.java
package org.example.processing.mocap;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.example.config.ProcessingParamsConfig;
import org.example.models.MoCapReading;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.flink.api.common.typeinfo.Types;

import java.io.Serializable;
// import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;
// import java.util.ArrayList; // Added
// import java.util.List;    // Added

public class MoCapAverageAngleProcessor {
    private static final Logger logger = LoggerFactory.getLogger(MoCapAverageAngleProcessor.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();

    private static final Map<String, double[]> thresholds = ProcessingParamsConfig.getSelectedMocapThresholds();
    private static final Set<String> jointsToMonitor = ProcessingParamsConfig.MOCAP_AVG_ANGLE_JOINTS_TO_MONITOR;

    public static class TimestampedMoCapReading implements Serializable {
        private static final long serialVersionUID = 201L;
        public long timestampMillis;
        public MoCapReading reading;
        public TimestampedMoCapReading() {}
        public TimestampedMoCapReading(long ts, MoCapReading r) { this.timestampMillis = ts; this.reading = r; }
    }

    public static class TimestampParser implements MapFunction<MoCapReading, TimestampedMoCapReading> {
        private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ISO_OFFSET_DATE_TIME;
        @Override
        public TimestampedMoCapReading map(MoCapReading value) throws Exception {
            if (value == null || value.getTimestamp() == null) return null;
            try {
                LocalDateTime ldt = LocalDateTime.parse(value.getTimestamp(), FORMATTER);
                long timestampMillis = ldt.toInstant(ZoneOffset.UTC).toEpochMilli();
                return new TimestampedMoCapReading(timestampMillis, value);
            } catch (Exception e) {
                logger.warn("Failed to parse MoCap timestamp string: {}. Skipping record.", value.getTimestamp(), e);
                return null;
            }
        }
    }

    // 1. Modify AverageJointAngles POJO
    public static class AverageJointAngles implements Serializable {
        private static final long serialVersionUID = 202L; // Updated serialVersionUID due to field changes
        public String thingId;
        public long windowStart; // Added for clarity
        public long windowEnd;
        public Map<String, Double> values = new HashMap<>();
        public long sumOfEventTimestampsInWindow = 0L; // For calculating average
        public int countOfEventsInWindow = 0;          // For calculating average
        public AverageJointAngles() {}
    }

    // 2. Modify JointAngleAccumulator
    public static class JointAngleAccumulator implements Serializable {
        private static final long serialVersionUID = 302L; // Updated serialVersionUID
        public Map<String, Tuple2<Double, Long>> sumsAndCounts = new HashMap<>();
        public String thingId = null;
        public long sumOfEventTimestamps = 0L;
        public int eventCount = 0;

        public void add(MoCapReading s, long eventTimestampMillis) {
            if (thingId == null && s != null) thingId = s.getThingid();
            if (s == null) return;

            sumOfEventTimestamps += eventTimestampMillis;
            eventCount++;

            ProcessingParamsConfig.MonitoredArm arm = ProcessingParamsConfig.MOCAP_MONITORED_ARM;
            if (jointsToMonitor.contains(ProcessingParamsConfig.ELBOW_FLEX_KEY)) {
                 accumulate(ProcessingParamsConfig.ELBOW_FLEX_KEY, (arm == ProcessingParamsConfig.MonitoredArm.RIGHT) ? s.getElbowFlexExtRight() : s.getElbowFlexExtLeft());
            }
            if (jointsToMonitor.contains(ProcessingParamsConfig.SHOULDER_ROTATION_KEY)) {
                accumulate(ProcessingParamsConfig.SHOULDER_ROTATION_KEY, (arm == ProcessingParamsConfig.MonitoredArm.RIGHT) ? s.getUpperarmRotationRight() : s.getUpperarmRotationLeft());
            }
            if (jointsToMonitor.contains(ProcessingParamsConfig.NECK_FLEX_KEY)) {
                accumulate(ProcessingParamsConfig.NECK_FLEX_KEY, s.getNeckFlexExt());
            }
            if (jointsToMonitor.contains(ProcessingParamsConfig.TRUNK_TILT_KEY)) {
                accumulate(ProcessingParamsConfig.TRUNK_TILT_KEY, s.getTorsoTilt());
            }
        }

        private void accumulate(String jointName, double value) {
            Tuple2<Double, Long> current = sumsAndCounts.getOrDefault(jointName, Tuple2.of(0.0, 0L));
            sumsAndCounts.put(jointName, Tuple2.of(current.f0 + value, current.f1 + 1));
        }

        public AverageJointAngles getResult() {
            AverageJointAngles result = new AverageJointAngles();
            result.thingId = this.thingId;
            for (String jointName : jointsToMonitor) {
                result.values.put(jointName, getAverage(jointName));
            }
            result.sumOfEventTimestampsInWindow = this.sumOfEventTimestamps;
            result.countOfEventsInWindow = this.eventCount;
            return result;
        }

        private double getAverage(String jointName) {
             Tuple2<Double, Long> sumCount = sumsAndCounts.get(jointName);
             return (sumCount != null && sumCount.f1 > 0) ? sumCount.f0 / sumCount.f1 : Double.NaN;
         }
    }

    // 3. Modify AverageAngleAggregator
    public static class AverageAngleAggregator implements AggregateFunction<TimestampedMoCapReading, JointAngleAccumulator, AverageJointAngles> {
         @Override public JointAngleAccumulator createAccumulator() { return new JointAngleAccumulator(); }
         @Override public JointAngleAccumulator add(TimestampedMoCapReading value, JointAngleAccumulator acc) {
             if (value != null && value.reading != null) {
                 acc.add(value.reading, value.timestampMillis); // Pass the event timestamp
             }
             return acc;
         }
         @Override public AverageJointAngles getResult(JointAngleAccumulator acc) { return acc.getResult(); }
         @Override public JointAngleAccumulator merge(JointAngleAccumulator a, JointAngleAccumulator b) {
              b.sumsAndCounts.forEach((key, valueB) -> {
                if (jointsToMonitor.contains(key)) {
                    a.sumsAndCounts.merge(key, valueB, (valueA, valB) -> Tuple2.of(valueA.f0 + valB.f0, valueA.f1 + valB.f1));
                }
             });
             a.sumOfEventTimestamps += b.sumOfEventTimestamps;
             a.eventCount += b.eventCount;
             if (a.thingId == null) a.thingId = b.thingId;
             return a;
         }
    }

    // 4. Modify WindowAverageProcessor to set windowStart, windowEnd and calculate avg data delay
    public static class WindowAverageProcessor extends ProcessWindowFunction<AverageJointAngles, AverageJointAngles, String, TimeWindow> {
       @Override
        public void process(String key, Context context, Iterable<AverageJointAngles> averages, Collector<AverageJointAngles> out) {
            AverageJointAngles avgBundle = averages.iterator().next(); // Contains sumOfEventTimestamps and countOfEvents
            avgBundle.windowStart = context.window().getStart();
            avgBundle.windowEnd = context.window().getEnd(); // This is the alert_trigger_timestamp for this type of alert
            out.collect(avgBundle);
        }
    }

    public static DataStream<String> processAverageAnglesForFeedback(DataStream<MoCapReading> sensorStream) {
        DataStream<TimestampedMoCapReading> sensorStreamWithTimestamps = sensorStream
                .map(new TimestampParser())
                .filter(value -> value != null)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<TimestampedMoCapReading>forBoundedOutOfOrderness(ProcessingParamsConfig.GENERAL_EVENT_TIME_BOUNDED_OUT_OF_ORDERNESS)
                                .withTimestampAssigner((event, timestamp) -> event.timestampMillis)
                                .withIdleness(ProcessingParamsConfig.GENERAL_IDLENESS_DURATION)
                );

        WindowedStream<TimestampedMoCapReading, String, TimeWindow> windowedStream = sensorStreamWithTimestamps
                .keyBy(tsr -> tsr.reading.getThingid())
                .window(SlidingEventTimeWindows.of(
                        ProcessingParamsConfig.MOCAP_AVG_ANGLE_WINDOW_SIZE,
                        ProcessingParamsConfig.MOCAP_AVG_ANGLE_WINDOW_SLIDE
                ));

        DataStream<AverageJointAngles> averageStream = windowedStream
                .aggregate(new AverageAngleAggregator(), new WindowAverageProcessor());

        // 5. Update FlatMap to create the final JSON
        DataStream<String> alertStream = averageStream
                .flatMap((AverageJointAngles avgBundle, Collector<String> out) -> {
                    try {
                        ObjectNode alertJson = objectMapper.createObjectNode();
                        alertJson.put("thingId", avgBundle.thingId);
                        alertJson.put("alertTriggerTimestamp", avgBundle.windowEnd); // Key for alert delay calc
                        alertJson.put("windowStartTimestamp", avgBundle.windowStart);
                        alertJson.put("windowEndTimestamp", avgBundle.windowEnd);
                        alertJson.put("feedbackType", "averageAngleAlert");

                        long averageDataDelayMs = 0;
                        if (avgBundle.countOfEventsInWindow > 0) {
                            double averageEventTimestampInWindow = (double) avgBundle.sumOfEventTimestampsInWindow / avgBundle.countOfEventsInWindow;
                            // This is the average "age" of data relative to the window end
                            averageDataDelayMs = (long) (avgBundle.windowEnd - averageEventTimestampInWindow);
                        }
                        alertJson.put("averageDataAgeInWindowMs", averageDataDelayMs); // Pre-calculated
                        alertJson.put("numberOfEventsInWindow", avgBundle.countOfEventsInWindow);


                        ArrayNode jointAlerts = alertJson.putArray("alerts");
                        // Check only the configured joints
                        for(String jointKey : jointsToMonitor){
                            if(avgBundle.values.containsKey(jointKey)){
                                checkAverageAndAddAlert(jointAlerts, jointKey, avgBundle.values.get(jointKey));
                            }
                        }

                        if (jointAlerts.size() > 0) {
                           logger.info("Average Angle Alert Generated for {}: {}", avgBundle.thingId, alertJson.toString());
                           out.collect(alertJson.toString());
                        }
                    } catch (Exception e) {
                        logger.error("Error formatting JSON alert for average angles", e);
                    }
                })
                .returns(Types.STRING);

        return alertStream;
    }

    private static void checkAverageAndAddAlert(ArrayNode alertArray, String jointName, Double averageValue) {
        // ... (same as before, ensure it handles averageValue being null/NaN if a joint had no data)
        if (averageValue == null || Double.isNaN(averageValue)) {
            logger.debug("Average value for joint {} is null or NaN. Skipping alert.", jointName);
            return;
        }
        if (!thresholds.containsKey(jointName)) {
            logger.warn("No threshold configured for joint: {}. Skipping alert.", jointName);
            return;
        }

        double[] limits = thresholds.get(jointName);
        double minNormal = limits[0];
        double maxNormal = limits[1];
        double minYellowBuffer = limits[2];
        double maxYellowBuffer = limits[3];

        String severity = "GREEN";
        if (averageValue < minNormal || averageValue > maxNormal) {
            severity = "RED";
        } else if (averageValue < (minNormal + minYellowBuffer) || averageValue > (maxNormal - maxYellowBuffer)) {
            severity = "YELLOW";
        }

        if (!severity.equals("GREEN")) {
            ObjectNode jointAlert = objectMapper.createObjectNode();
            jointAlert.put("joint", jointName);
            jointAlert.put("severity", severity);
            jointAlert.put("averageValue", Math.round(averageValue * 100.0) / 100.0);
            jointAlert.put("minNormal", minNormal);
            jointAlert.put("maxNormal", maxNormal);
            alertArray.add(jointAlert);
        }
    }
}