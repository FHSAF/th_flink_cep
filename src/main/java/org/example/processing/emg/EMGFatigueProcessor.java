// File: Flink-CEP/src/main/java/org/example/processing/emg/EMGFatigueProcessor.java
package org.example.processing.emg; // New package

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.example.config.ProcessingParamsConfig;
import org.example.models.EMGReading;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

// Renamed from EMGProcessing
public class EMGFatigueProcessor {

    private static final Logger logger = LoggerFactory.getLogger(EMGFatigueProcessor.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();

    // Configuration Constants - NOW FROM ProcessingParamsConfig
    private static final Duration RMS_WINDOW_SIZE = ProcessingParamsConfig.EMG_RMS_WINDOW_SIZE;
    private static final Duration RMS_WINDOW_SLIDE = ProcessingParamsConfig.EMG_RMS_WINDOW_SLIDE;
    private static final Duration FATIGUE_TREND_WINDOW_DURATION = ProcessingParamsConfig.EMG_RMS_FATIGUE_TREND_WINDOW_DURATION;
    private static final double HIGH_RMS_THRESHOLD = ProcessingParamsConfig.EMG_RMS_HIGH_THRESHOLD;
    private static final int MIN_HISTORY_SIZE_FOR_ALERT = ProcessingParamsConfig.EMG_RMS_MIN_HISTORY_SIZE_FOR_ALERT;
    // private static final DateTimeFormatter TIMESTAMP_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static final DateTimeFormatter ISO_TIMESTAMP_FORMATTER = DateTimeFormatter.ISO_OFFSET_DATE_TIME;

    /**
     * Main processing pipeline for EMG fatigue detection based on RMS trend.
     * Note: This uses RMS as an indicator. For MDF trend, use the separate Python service.
     * @param rawEmgStream Input stream of EMGReading objects.
     * @param musclesToMonitor Set of muscle names to process.
     * @return Stream of JSON fatigue alerts.
     */
    public static DataStream<String> processEMGFatigueRMS(
            DataStream<EMGReading> rawEmgStream, // Takes EMGReading directly
            Set<String> musclesToMonitor) {

        // 1. Parse Timestamp and create wrapper object
        DataStream<TimestampedEMGReading> timedStream = rawEmgStream
                .map(new TimestampParserEMG()) // Use the parser
                .filter(value -> value != null)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<TimestampedEMGReading>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner((event, timestamp) -> event.timestampMillis)
                                .withIdleness(Duration.ofMinutes(1))
                );

        // 2. Flatten the structure to individual muscle readings
        DataStream<EMGMuscleReading> muscleStream = timedStream
                .flatMap(new MuscleDataExtractor(musclesToMonitor))
                .filter(value -> value != null);

        // 3. Calculate RMS over short sliding windows per muscle
        TypeInformation<Tuple2<String, String>> keyTypeInfo = TypeInformation.of(new TypeHint<Tuple2<String, String>>() {});
        WindowedStream<EMGMuscleReading, Tuple2<String, String>, TimeWindow> rmsWindowedStream = muscleStream
                .keyBy(r -> Tuple2.of(r.thingId, r.muscleName), keyTypeInfo)
                .window(SlidingEventTimeWindows.of(RMS_WINDOW_SIZE, RMS_WINDOW_SLIDE));

        DataStream<Tuple4<String, String, Long, Double>> rmsStream = rmsWindowedStream
                .aggregate(new RMSAggregator(), new RMSWindowProcessor());

        // 4. Detect Fatigue Trend (Sustained High RMS) using stateful processing
        TypeInformation<Tuple2<String, String>> processKeyTypeInfo = TypeInformation.of(new TypeHint<Tuple2<String, String>>() {});
        DataStream<String> fatigueAlertStream = rmsStream
                .keyBy(t -> Tuple2.of(t.f0, t.f1), processKeyTypeInfo)
                .process(new FatigueDetector(FATIGUE_TREND_WINDOW_DURATION.toMillis(), HIGH_RMS_THRESHOLD, MIN_HISTORY_SIZE_FOR_ALERT));

        return fatigueAlertStream;
    }

    // --- Helper Classes ---

    /** Wrapper to add parsed timestamp */
    public static class TimestampedEMGReading implements Serializable {
        // (Keep existing TimestampedEMGSensorReading logic, but use EMGReading)
        private static final long serialVersionUID = 202L;
        public long timestampMillis;
        public EMGReading reading;
        public TimestampedEMGReading() {}
        public TimestampedEMGReading(long ts, EMGReading r) { this.timestampMillis = ts; this.reading = r; }
    }

    /** Parses the string timestamp from EMGReading */
    public static class TimestampParserEMG implements MapFunction<EMGReading, TimestampedEMGReading> {
        // (Keep existing TimestampParserEMG logic)
        @Override
        public TimestampedEMGReading map(EMGReading value) {
            if (value == null || value.getTimestamp() == null) return null;
            try {
                Instant instant = Instant.from(ISO_TIMESTAMP_FORMATTER.parse(value.getTimestamp()));
                long timestampMillis = instant.toEpochMilli();

                // LocalDateTime ldt = LocalDateTime.parse(value.getTimestamp(), TIMESTAMP_FORMATTER);
                // long timestampMillis = ldt.toInstant(ZoneOffset.UTC).toEpochMilli();
                return new TimestampedEMGReading(timestampMillis, value);
            } catch (Exception e) {
                logger.warn("EMG Parser: Failed to parse timestamp string: {}. Skipping.", value.getTimestamp(), e);
                return null;
            }
        }
    }

    /** Represents a single muscle reading at a point in time */
    public static class EMGMuscleReading implements Serializable {
        // (Keep existing EMGReading logic, renamed for clarity within this processor)
        private static final long serialVersionUID = 203L;
        public String thingId;
        public long timestampMillis;
        public String muscleName;
        public double microvolts;
        public EMGMuscleReading() {}
        public EMGMuscleReading(String thingId, long timestampMillis, String muscleName, double microvolts) {
            this.thingId = thingId; this.timestampMillis = timestampMillis; this.muscleName = muscleName; this.microvolts = microvolts;
        }
         public String getThingId() { return thingId; }
         public long getTimestampMillis() { return timestampMillis; }
         public String getMuscleName() { return muscleName; }
         public double getMicrovolts() { return microvolts; }
    }

    /** Extracts individual muscle readings */
    public static class MuscleDataExtractor implements FlatMapFunction<TimestampedEMGReading, EMGMuscleReading> {
         // (Keep existing MuscleDataExtractor logic, but output EMGMuscleReading)
         private final Set<String> musclesToMonitor;
         public MuscleDataExtractor(Set<String> musclesToMonitor) { this.musclesToMonitor = musclesToMonitor; }
         @Override
         public void flatMap(TimestampedEMGReading tsReading, Collector<EMGMuscleReading> out) {
            if (tsReading == null || tsReading.reading == null) return;
            EMGReading reading = tsReading.reading;
            long timestamp = tsReading.timestampMillis;
            String thingId = reading.getThingid();
            try {
                if (musclesToMonitor.contains("deltoids_left")) out.collect(new EMGMuscleReading(thingId, timestamp, "deltoids_left", reading.getDeltoids_left()));
                if (musclesToMonitor.contains("triceps_left")) out.collect(new EMGMuscleReading(thingId, timestamp, "triceps_left", reading.getTriceps_left()));
                // ... Add ALL other muscles from EMGReading ...
                if (musclesToMonitor.contains("latissimus_right")) out.collect(new EMGMuscleReading(thingId, timestamp, "latissimus_right", reading.getLatissimus_right()));
            } catch (Exception e) { logger.error("Error extracting muscle data for thingId {}", thingId, e); }
         }
     }

    /** Accumulator for RMS calculation */
    public static class RMSAccumulator implements Serializable {
        // (Keep existing RMSAccumulator logic)
        private static final long serialVersionUID = 204L;
        public double sumOfSquares = 0.0; public long count = 0L;
    }

    /** Calculates RMS within a window */
    public static class RMSAggregator implements AggregateFunction<EMGMuscleReading, RMSAccumulator, Double> {
        // (Keep existing RMSAggregator logic, but input is EMGMuscleReading)
        private static final long serialVersionUID = 205L;
        @Override public RMSAccumulator createAccumulator() { return new RMSAccumulator(); }
        @Override public RMSAccumulator add(EMGMuscleReading v, RMSAccumulator a) { a.sumOfSquares += v.microvolts*v.microvolts; a.count++; return a; }
        @Override public Double getResult(RMSAccumulator a) { return (a.count==0) ? Double.NaN : Math.sqrt(a.sumOfSquares / a.count); }
        @Override public RMSAccumulator merge(RMSAccumulator a, RMSAccumulator b) { a.sumOfSquares += b.sumOfSquares; a.count += b.count; return a; }
    }

    /** Adds metadata (key, window end time) to the aggregated RMS value */
    public static class RMSWindowProcessor extends ProcessWindowFunction<Double, Tuple4<String, String, Long, Double>, Tuple2<String, String>, TimeWindow> {
        // (Keep existing RMSWindowProcessor logic)
         private static final long serialVersionUID = 206L;
         @Override
         public void process(Tuple2<String, String> key, Context context, Iterable<Double> aggregates, Collector<Tuple4<String, String, Long, Double>> out) {
             Double rms = aggregates.iterator().next();
             out.collect(Tuple4.of(key.f0, key.f1, context.window().getEnd(), rms));
         }
     }

    /** Stateful function to detect fatigue based on sustained high average RMS */
    public static class FatigueDetector extends KeyedProcessFunction<Tuple2<String, String>, Tuple4<String, String, Long, Double>, String> {
        // (Keep existing FatigueDetector logic)
        private static final long serialVersionUID = 207L;
        private final long trendWindowMillis; private final double highRmsThreshold; private final int minHistorySize;
        private transient ListState<Tuple2<Long, Double>> rmsHistoryState;
        public FatigueDetector(long t, double h, int m) {this.trendWindowMillis=t; this.highRmsThreshold=h; this.minHistorySize=m;}
        @Override public void open(Configuration parameters) throws Exception {
            rmsHistoryState = getRuntimeContext().getListState(new ListStateDescriptor<>("rmsHistory", TypeInformation.of(new TypeHint<Tuple2<Long, Double>>() {})));
            logger.info("Initialized FatigueDetector state for subtask {}/{}", getRuntimeContext().getTaskInfo().getIndexOfThisSubtask(), getRuntimeContext().getTaskInfo().getNumberOfParallelSubtasks());
        }
        @Override public void processElement(Tuple4<String, String, Long, Double> value, Context ctx, Collector<String> out) throws Exception {
             String thingId = value.f0; String muscle = value.f1; Long currentTimestamp = value.f2; Double currentRms = value.f3;
             if (currentRms == null || currentRms.isNaN()) return;
             rmsHistoryState.add(Tuple2.of(currentTimestamp, currentRms));
             List<Tuple2<Long, Double>> history = new ArrayList<>(); Long oldestAllowedTimestamp = currentTimestamp - trendWindowMillis; boolean stateChanged = false;
             for (Tuple2<Long, Double> entry : rmsHistoryState.get()) { if (entry.f0 != null && entry.f0 >= oldestAllowedTimestamp) { history.add(entry); } else { stateChanged = true; } }
             if(stateChanged || history.isEmpty()){ rmsHistoryState.update(history); }
             logger.debug("Processing EMG for Key: {}, Timestamp: {}, RMS: {:.2f}, History Size: {}", ctx.getCurrentKey(), currentTimestamp, currentRms, history.size());
             if (history.size() >= minHistorySize) {
                double sumRms = 0; for(Tuple2<Long, Double> entry : history) { sumRms += entry.f1; } double avgRms = sumRms / history.size();
                logger.trace("Key: {}, Avg RMS over last {}ms: {:.2f} (Threshold: {})", ctx.getCurrentKey(), trendWindowMillis, avgRms, highRmsThreshold);
                if (avgRms > highRmsThreshold) {
                    ObjectNode alert = objectMapper.createObjectNode();
                    alert.put("thingId", thingId); alert.put("timestamp", currentTimestamp); alert.put("feedbackType", "emgFatigueAlert"); // Keep type consistent
                    alert.put("muscle", muscle); alert.put("severity", "HIGH"); alert.put("reason", "Sustained High Average RMS");
                    alert.put("averageRMS", Math.round(avgRms * 100.0) / 100.0); alert.put("checkWindowSeconds", trendWindowMillis / 1000);
                    alert.put("rmsThreshold", highRmsThreshold);
                    out.collect(alert.toString());
                    logger.warn("EMG Fatigue Alert Triggered: Key={}, Alert={}", ctx.getCurrentKey(), alert.toString());
                }
             }
        }
    }
}