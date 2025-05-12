package org.example.processing.emg;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import org.example.processing.emg.EMGFatigueProcessor.EMGMuscleReading; // Using this structure
import org.example.config.ProcessingParamsConfig;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

import org.jtransforms.fft.DoubleFFT_1D;
import org.apache.commons.math3.stat.regression.SimpleRegression;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class EMGMdfFatigueProcessor {

    private static final Logger logger = LoggerFactory.getLogger(EMGMdfFatigueProcessor.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();

    // Configuration from ProcessingParamsConfig
    private static final double SAMPLING_RATE_HZ = 100.0; // Ensure this is in ProcessingParamsConfig
    private static final int FFT_WINDOW_SAMPLES = (int) (SAMPLING_RATE_HZ * 1.0); // e.g., 1s window
    private static final double FFT_OVERLAP_PERCENT = 0.5;
    private static final int NPERSEG_WELCH = FFT_WINDOW_SAMPLES;
    private static final int NOVERLAP_WELCH = (int) (NPERSEG_WELCH * FFT_OVERLAP_PERCENT);
    private static final int STEP_WELCH = NPERSEG_WELCH - NOVERLAP_WELCH; // How many new samples to trigger next Welch segment

    private static final int TREND_WINDOW_SECONDS = ProcessingParamsConfig.EMG_MDF_TREND_WINDOW_SEC;
    private static final int MIN_MDF_POINTS_FOR_TREND = ProcessingParamsConfig.EMG_MDF_MIN_POINTS_FOR_TREND;
    private static final double MDF_SLOPE_THRESHOLD = ProcessingParamsConfig.EMG_MDF_SLOPE_FATIGUE_THRESHOLD;
    private static final double R_SQUARED_THRESHOLD = ProcessingParamsConfig.EMG_MDF_REGRESSION_R_SQUARED_THRESHOLD;
    private static final int ALERT_COOLDOWN_MS = ProcessingParamsConfig.EMG_MDF_ALERT_COOLDOWN_SEC * 1000;

    // Bandpass filter parameters (example, should be configurable)
    // private static final double LOWCUT_FILTER = 20.0;
    // private static final double HIGHCUT_FILTER = 450.0; // Or narrower if desired for MDF focus
    // Note: Implementing robust IIR/FIR filters in Java from scratch is complex.
    // For production, using a library or pre-filtered data is advised.
    // This example will focus on Welch and MDF assuming reasonably clean input for that stage.


    /**
     * Input stream of EMGMuscleReading (already filtered by muscle, if applicable before this processor)
     * Each EMGMuscleReading contains: thingid, timestampMillis (event time), muscleName, microvolts
     */
    public static DataStream<String> processMdfFatigue(DataStream<EMGMuscleReading> emgMuscleReadingStream) {
        return emgMuscleReadingStream
            .keyBy(reading -> Tuple2.of(reading.getThingId(), reading.getMuscleName()))
            .process(new MdfCalculatorAndTrendDetector());
    }


    public static class MdfCalculatorAndTrendDetector extends KeyedProcessFunction<Tuple2<String, String>, EMGMuscleReading, String> {
        private static final long serialVersionUID = 101L;

        // State to buffer raw EMG samples for Welch's method segments
        private transient ListState<Double> rawEmgSegmentBufferState;
        // State to store the history of (timestamp, MDF) values for trend analysis
        private transient ListState<Tuple2<Long, Double>> mdfHistoryState;
        // State to store the timestamp of the last alert to manage cooldown
        private transient ValueState<Long> lastAlertTimestampSentState;

        @Override
        public void open(Configuration parameters) throws Exception {
            rawEmgSegmentBufferState = getRuntimeContext().getListState(
                new ListStateDescriptor<>("rawEmgSegmentBufferMDF", TypeInformation.of(new TypeHint<Double>() {}))
            );
            mdfHistoryState = getRuntimeContext().getListState(
                new ListStateDescriptor<>("mdfHistoryFlink", TypeInformation.of(new TypeHint<Tuple2<Long, Double>>() {}))
            );
            lastAlertTimestampSentState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("lastMdfAlertTimeFlink", Long.class)
            );
        }

        @Override
        public void processElement(EMGMuscleReading reading, Context ctx, Collector<String> out) throws Exception {
            List<Double> segmentBuffer = new ArrayList<>();
            if (rawEmgSegmentBufferState.get() != null) {
                for (Double val : rawEmgSegmentBufferState.get()) {
                    segmentBuffer.add(val);
                }
            }
            segmentBuffer.add(reading.getMicrovolts());

            // Manage buffer size for Welch segments (enough for NPERSEG_WELCH + potential for overlap handling)
            // This simple version triggers calculation when NPERSEG_WELCH samples are accumulated.
            // A more accurate Welch would manage overlapping segments from a longer continuous buffer.
            while (segmentBuffer.size() > NPERSEG_WELCH) { // Keep only the latest NPERSEG samples
                segmentBuffer.remove(0);
            }
            rawEmgSegmentBufferState.update(segmentBuffer);

            if (segmentBuffer.size() == NPERSEG_WELCH) {
                double[] currentSegmentArray = segmentBuffer.stream().mapToDouble(Double::doubleValue).toArray();

                // --- Apply Hann Window ---
                double[] hannWindow = hannWindow(NPERSEG_WELCH);
                double[] windowedSegment = new double[NPERSEG_WELCH];
                for (int i = 0; i < NPERSEG_WELCH; i++) {
                    windowedSegment[i] = currentSegmentArray[i] * hannWindow[i];
                }
                
                // --- Detrend ---
                double[] detrendedSegment = detrendSignal(windowedSegment);

                // --- FFT (using JTransforms) ---
                DoubleFFT_1D fft = new DoubleFFT_1D(NPERSEG_WELCH);
                // JTransforms realForward modifies input array and stores complex result in specific layout
                double[] fftInputOutput = Arrays.copyOf(detrendedSegment, NPERSEG_WELCH); // Use detrended signal
                fft.realForward(fftInputOutput); // For N data points, output is N complex points packed into N doubles

                // --- Calculate Power Spectral Density (PSD) ---
                // For N real points, FFT result has N/2+1 unique points if N is even
                int numUniquePts = NPERSEG_WELCH / 2 + 1;
                double[] psd = new double[numUniquePts];
                psd[0] = fftInputOutput[0] * fftInputOutput[0] / NPERSEG_WELCH; // DC component (real)
                for (int i = 1; i < NPERSEG_WELCH / 2; i++) {
                    // fftInputOutput[2*i] is real part, fftInputOutput[2*i+1] is imaginary part for frequency i
                    psd[i] = (fftInputOutput[2 * i] * fftInputOutput[2 * i] +
                              fftInputOutput[2 * i + 1] * fftInputOutput[2 * i + 1]) / NPERSEG_WELCH;
                }
                if (NPERSEG_WELCH % 2 == 0) { // Nyquist frequency (if N is even, it's real)
                     psd[NPERSEG_WELCH / 2] = fftInputOutput[1] * fftInputOutput[1] / NPERSEG_WELCH; // Stored at index 1 for N/2 Hz
                }
                // Note: Proper scaling for Welch method usually involves averaging PSDs from multiple segments.
                // This is a PSD from a single segment.

                // --- Calculate MDF from PSD ---
                double mdf = calculateMdfFromPsdArray(psd, SAMPLING_RATE_HZ, NPERSEG_WELCH);

                if (!Double.isNaN(mdf)) {
                    long mdfCalcTimestamp = reading.getTimestampMillis(); // Timestamp of the last sample in this segment
                    logger.debug("Flink MDF: Key={}, Time={}, MDF={:.2f} Hz", ctx.getCurrentKey(), new java.util.Date(mdfCalcTimestamp), mdf);

                    List<Tuple2<Long, Double>> history = new ArrayList<>();
                    if (mdfHistoryState.get() != null) {
                        mdfHistoryState.get().forEach(history::add);
                    }
                    history.add(Tuple2.of(mdfCalcTimestamp, mdf));

                    // Prune history
                    long trendWindowStartTs = mdfCalcTimestamp - (TREND_WINDOW_SECONDS * 1000L);
                    history.removeIf(entry -> entry.f0 < trendWindowStartTs);
                    mdfHistoryState.update(history);
                    
                    Long lastAlertMs = lastAlertTimestampSentState.value();
                    if (lastAlertMs != null && ctx.timestamp() < lastAlertMs + ALERT_COOLDOWN_MS) { // Using processing time for cooldown to avoid event time complexities
                        logger.debug("Key {}: In Flink MDF alert cooldown.", ctx.getCurrentKey());
                        return;
                    }

                    if (history.size() >= MIN_MDF_POINTS_FOR_TREND) {
                        SimpleRegression regression = new SimpleRegression(true); // include intercept
                        long firstTsInHistory = history.get(0).f0;
                        for (Tuple2<Long, Double> point : history) {
                            regression.addData(((double)point.f0 - firstTsInHistory) / 1000.0, point.f1); // time in seconds relative to start
                        }

                        if (regression.getN() >= 2) {
                            double slope = regression.getSlope();
                            double rSquared = regression.getRSquare();

                            logger.info("Flink MDF Trend: Key={}, Slope={:.4f}, R2={:.3f}, N={}", ctx.getCurrentKey(), slope, rSquared, regression.getN());

                            if (!Double.isNaN(slope) && !Double.isNaN(rSquared) &&
                                slope < MDF_SLOPE_THRESHOLD && rSquared >= R_SQUARED_THRESHOLD) {
                                
                                ObjectNode alert = objectMapper.createObjectNode();
                                alert.put("thingId", ctx.getCurrentKey().f0);
                                alert.put("alertTriggerTimestamp", mdfCalcTimestamp);
                                alert.put("feedbackType", "emgMdfFatigueAlert");
                                alert.put("muscle", ctx.getCurrentKey().f1);
                                alert.put("severity", "HIGH");
                                alert.put("reason", "Decreasing MDF Trend Detected (Flink)");
                                alert.put("mdfSlopeHzPerSec", Math.round(slope * 10000.0) / 10000.0);
                                alert.put("regressionRSquared", Math.round(rSquared * 1000.0) / 1000.0);
                                alert.put("trendWindowSecondsUsed", (int) Math.round((mdfCalcTimestamp - firstTsInHistory) / 1000.0));
                                alert.put("numberOfMdfPointsInTrend", history.size());
                                alert.put("mdfSlopeThreshold", MDF_SLOPE_THRESHOLD);
                                alert.put("rSquaredThreshold", R_SQUARED_THRESHOLD);
                                alert.put("lastMdfValue", Math.round(history.get(history.size()-1).f1 * 100.0)/100.0);
                                
                                // Calculate average age of MDF data points in trend
                                double sumMdfTimestamps = 0;
                                for(Tuple2<Long, Double> p : history) sumMdfTimestamps += p.f0;
                                long avgMdfDataAgeInTrendMs = 0;
                                if(!history.isEmpty()){
                                    avgMdfDataAgeInTrendMs = (long)(mdfCalcTimestamp - (sumMdfTimestamps / history.size()));
                                }
                                alert.put("averageMdfDataAgeInTrendMs", avgMdfDataAgeInTrendMs);


                                out.collect(alert.toString());
                                lastAlertTimestampSentState.update(ctx.timestamp()); // Processing time for cooldown
                                logger.warn("Flink Generated MDF FATIGUE ALERT for {}: {}", ctx.getCurrentKey(), alert.toString());
                                // mdfHistoryState.clear(); // Optional
                            }
                        }
                    }
                }
                // To implement Welch properly with overlap, you'd typically advance the buffer by `STEP_WELCH`
                // and keep the overlapping samples. This simplified version processes full, distinct buffers.
                // For a simple tumbling window approach for FFT:
                // currentBuffer.clear();
                // emgSignalBufferState.update(currentBuffer);
            }
        }
        
        // --- Signal Processing Helper Methods ---
        private static double[] hannWindow(int size) {
            double[] window = new double[size];
            for (int i = 0; i < size; i++) {
                window[i] = 0.5 * (1 - Math.cos(2 * Math.PI * i / (size - 1)));
            }
            return window;
        }

        private static double[] detrendSignal(double[] signal) {
            if (signal == null || signal.length < 2) return signal;
            SimpleRegression regression = new SimpleRegression();
            for (int i = 0; i < signal.length; i++) {
                regression.addData(i, signal[i]);
            }
            double[] detrended = new double[signal.length];
            for (int i = 0; i < signal.length; i++) {
                detrended[i] = signal[i] - regression.predict(i);
            }
            return detrended;
        }

        private static double calculateMdfFromPsdArray(double[] psd, double fs, int nfft) {
            if (psd == null || psd.length == 0) return Double.NaN;
            double totalPower = 0;
            for (double p_val : psd) {
                totalPower += p_val;
            }
            if (totalPower < 1e-12) return Double.NaN;

            double cumulativePower = 0;
            for (int i = 0; i < psd.length; i++) {
                cumulativePower += psd[i];
                if (cumulativePower >= totalPower / 2.0) {
                    // Frequency resolution: fs / nfft
                    // Frequency of bin i: i * (fs / nfft)
                    return (double)i * (fs / nfft);
                }
            }
            return Double.NaN; // Should be found if totalPower > 0
        }
    }
}