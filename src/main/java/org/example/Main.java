package org.example;

import java.util.Map;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

// Import Configs
import org.example.config.DBConfig;
import org.example.config.FlinkJobConfig;
import org.example.config.KafkaConfig;
import org.example.config.ProcessingParamsConfig;
import org.example.models.ChannelSpecificFeatures;
// Import Models
import org.example.models.EMGReading;
import org.example.models.EmgFeatureMessage;
import org.example.models.EyeGazeReading;
import org.example.models.FeatureDbRow;
import org.example.models.MoCapReading;

// Import Source Providers
import org.example.sources.provider.EMGKafkaSourceProvider;
import org.example.sources.provider.EmgFeatureKafkaSourceProvider;
import org.example.sources.provider.EyeGazeKafkaSourceProvider;
import org.example.sources.provider.MoCapKafkaSourceProvider;

import org.example.processing.eyegaze.EyeGazeAttentionProcessor;
import org.example.processing.mocap.MoCapSlidingWindowAlertProcessor;
import org.example.processing.mocap.MoCapRulaProcessor;

// Import DB Sinks
import org.example.sinks.db.AvgAngleAlertDbSink;
import org.example.sinks.db.EMGRawDbSink;
import org.example.sinks.db.EyeGazeAttentionAlertDbSink;
import org.example.sinks.db.EyeGazeRawDbSink;
import org.example.sinks.db.MoCapRawDbSink;
import org.example.sinks.db.RulaScoreDbSink;

// Import Kafka Sinks
import org.example.sinks.kafka.EyeGazeAlertKafkaSink;
import org.example.sinks.kafka.MoCapErgonomicsAlertKafkaSink;

import org.apache.flink.util.Collector; 
import org.example.sinks.db.ExtractedFeaturesDbSink;
import java.sql.Timestamp;


// Other imports
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {
    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {

        logger.info("#############################################");
        logger.info("Getting Flink Execution Environment...");
        logger.info("  (Note: When using 'flink run', host/port/jar are determined by cluster config)");
        logger.info("  Configured Master (from FlinkJobConfig): {}:{}", FlinkJobConfig.FLINK_MASTER_HOST, FlinkJobConfig.FLINK_MASTER_PORT);
        logger.info("  Configured Jar Path (from FlinkJobConfig): {}", FlinkJobConfig.JAR_PATH);
        logger.info("  Configured Kafka Brokers (from FlinkJobConfig): {}", FlinkJobConfig.KAFKA_BROKERS);
        logger.info("#############################################");

        // Get the environment automatically configured by Flink's runtime/CLI
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        try {
            logger.info("#############################################");
            logger.info("Building Flink Data Pipeline...");
            logger.info("#############################################");

            // --- Source: MoCap Data ---
            KafkaSource<MoCapReading> moCapSource = MoCapKafkaSourceProvider.getKafkaSource();
            DataStream<MoCapReading> moCapStream = env.fromSource(moCapSource, WatermarkStrategy.noWatermarks(), "MoCapKafkaSource")
                                                     .filter(value -> value != null && value.getThingid() != null && !value.getThingid().isEmpty())
                                                     .name("FilterValidMoCap");

            // --- Source: EMG Data ---
            KafkaSource<EMGReading> emgSource = EMGKafkaSourceProvider.getEMGKafkaSource(FlinkJobConfig.KAFKA_BROKERS, KafkaConfig.EMG_GROUP_ID);
            DataStream<EMGReading> emgStream = env.fromSource(emgSource, WatermarkStrategy.noWatermarks(), "EMGKafkaSource")
                                                  .filter(value -> value != null && value.getThingid() != null && value.getTimestamp() != null && !value.getTimestamp().isEmpty())
                                                  .name("FilterValidEMG");

            // --- Source: Eye Gaze Attention ---
            KafkaSource<EyeGazeReading> gazeSource = EyeGazeKafkaSourceProvider.getEyeGazeKafkaSource(FlinkJobConfig.KAFKA_BROKERS, KafkaConfig.EYEGAZE_SOURCE_TOPIC, KafkaConfig.EYEGAZE_GROUP_ID);
            DataStream<EyeGazeReading> gazeStream = env.fromSource(gazeSource, WatermarkStrategy.noWatermarks(), "GazeAttentionKafkaSource")
                                                       .filter(value -> value != null && value.getThingid() != null && !value.getThingid().isEmpty())
                                                       .name("FilterValidGaze");
            // --- Consume Extracted Features from Python and Sink to DB ---
            logger.info("Configuring Flink to consume and deserialize extracted EMG features from Python at the source...");
            KafkaSource<EmgFeatureMessage> extractedFeaturesSource = EmgFeatureKafkaSourceProvider.getEmgFeatureKafkaSource(
                FlinkJobConfig.KAFKA_BROKERS,
                KafkaConfig.FEATURES_VISUALIZATION_TOPIC,
                "flink-extracted-features-consumer-group-" + System.currentTimeMillis()
            );
            DataStream<EmgFeatureMessage> emgFeaturesStream = env.fromSource(
                extractedFeaturesSource,
                WatermarkStrategy.noWatermarks(),
                "EmgFeatureSource" 
            );

            DataStream<FeatureDbRow> dbRowsStream = emgFeaturesStream.flatMap(new FlatMapFunction<EmgFeatureMessage, FeatureDbRow>() {
                @Override
                public void flatMap(EmgFeatureMessage message, Collector<FeatureDbRow> out) throws Exception {
                    if (message.getChannelsFeatures() == null) return;

                    Timestamp eventTime = new Timestamp(message.getSourceTimestampMs());
                    String thingId = message.getThingId();
                    Long sourceTimestampMs = message.getSourceTimestampMs();

                    for (Map.Entry<String, ChannelSpecificFeatures> entry : message.getChannelsFeatures().entrySet()) {
                        String channelName = entry.getKey();
                        ChannelSpecificFeatures features = entry.getValue();

                        out.collect(new FeatureDbRow(
                            eventTime,
                            thingId,
                            channelName,
                            features.getRms(),
                            features.getMdf(),
                            features.getMnf(),
                            features.getMnp(),
                            features.getRmsPercentMvc(),
                            sourceTimestampMs
                        ));
                    }
                }
            }).name("MapToFeatureDbRows");

            // --- MoCap Processing ---
            logger.info("Configuring MoCap Processing (Sliding Window Alerts and RULA)..."); 
            DataStream<String> slidingWindowAlerts = MoCapSlidingWindowAlertProcessor.processSlidingWindowAlerts(moCapStream);

            DataStream<String> rulaScoreJsonStream = moCapStream
                .map(new MoCapRulaProcessor.RulaScoreMapFunction())
                .filter(json -> json != null && !json.isEmpty())
                .name("CalculateRULAScore");


            // --- Eye Gaze Processing ---
            logger.info("Configuring Eye Gaze Attention Processing...");
            DataStream<String> gazeAlerts = EyeGazeAttentionProcessor.processGazeAttention(
                    gazeStream,
                    ProcessingParamsConfig.EYE_GAZE_PROLONGED_INATTENTION_DURATION_THRESHOLD
            );

            // --- Sinks ---
            logger.info("Configuring Sinks (Kafka and Database)...");
            String dbUrlBase = DBConfig.DB_URL;
            String dbUser = DBConfig.DB_USER;
            String dbPassword = DBConfig.DB_PASSWORD;
            // Kafka Sinks
            slidingWindowAlerts.sinkTo(MoCapErgonomicsAlertKafkaSink.getKafkaSink(FlinkJobConfig.KAFKA_BROKERS, KafkaConfig.MOCAP_AVERAGE_ANGLE_ALERTS_SINK_TOPIC))
                            .name("MocapSlidingWindowAlertKafkaSink");
            rulaScoreJsonStream.sinkTo(MoCapErgonomicsAlertKafkaSink.getKafkaSink(FlinkJobConfig.KAFKA_BROKERS, KafkaConfig.RULA_SCORES_SINK_TOPIC))
                               .name("RulaScoreKafkaSink");
            gazeAlerts.sinkTo(EyeGazeAlertKafkaSink.getKafkaSink(FlinkJobConfig.KAFKA_BROKERS, KafkaConfig.EYEGAZE_ATTENTION_ALERTS_SINK_TOPIC))
                      .name("GazeAttentionAlertKafkaSink");


            // Database Sinks - Raw Data
            moCapStream.sinkTo(new MoCapRawDbSink(dbUrlBase + DBConfig.MOCAP_DB_NAME, DBConfig.MOCAP_RAW_DATA_TABLE, dbUser, dbPassword))
                       .name("RawMoCapDbSink");
            emgStream.sinkTo(new EMGRawDbSink(dbUrlBase, DBConfig.EMG_DB_NAME, dbUser, dbPassword))
                     .name("RawEMGDbSink");
            gazeStream.sinkTo(new EyeGazeRawDbSink(dbUrlBase + DBConfig.EYEGAZE_DB_NAME, DBConfig.EYEGAZE_RAW_DATA_TABLE, dbUser, dbPassword))
                      .name("RawEyeGazeDbSink");
            // Database Sinks - Processed Data
            slidingWindowAlerts.sinkTo(new AvgAngleAlertDbSink(dbUrlBase + DBConfig.MOCAP_PROCESSED_DB_NAME, DBConfig.MOCAP_AVERAGE_ANGLES_ALERTS_TABLE, dbUser, dbPassword))
                  .name("SlidingWindowAlertDbSink");
            rulaScoreJsonStream.sinkTo(new RulaScoreDbSink(dbUrlBase + DBConfig.MOCAP_PROCESSED_DB_NAME, DBConfig.RULA_SCORES_TABLE, dbUser, dbPassword))
                               .name("RulaScoreDbSink");
            dbRowsStream.sinkTo(new ExtractedFeaturesDbSink(
                                                dbUrlBase,
                                                DBConfig.EMG_PROCESSED_DB_NAME,
                                                DBConfig.EMG_EXTRACTED_FEATURES_TABLE,
                                                dbUser,
                                                dbPassword))
                                            .name("ExtractedFeaturesToDbSink")
                                            .setParallelism(1);
            gazeAlerts.sinkTo(new EyeGazeAttentionAlertDbSink(dbUrlBase + DBConfig.EYEGAZE_DB_NAME, DBConfig.EYEGAZE_ATTENTION_ALERTS_TABLE, dbUser, dbPassword))
                      .name("EyeGazeAttentionAlertDbSink");


            logger.info("#############################################");
            logger.info("Flink pipeline built successfully.");
            logger.info("Executing Flink job: HRC Real-time Monitoring (MDF EMG Fatigue via Python)");
            logger.info("#############################################");
            env.execute("HRC Real-time Monitoring (MDF EMG Fatigue via Python)");

        } catch (Exception e) {
            logger.error("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@");
            logger.error("An error occurred while building or executing the Flink job: ", e);
            logger.error("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@");
             System.exit(1);
        }
    }
}