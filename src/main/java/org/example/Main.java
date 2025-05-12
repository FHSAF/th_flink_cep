package org.example;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

// Import Configs
import org.example.config.DBConfig;
import org.example.config.FlinkJobConfig;
import org.example.config.KafkaConfig;
import org.example.config.ProcessingParamsConfig;

// Import Models
import org.example.models.EMGReading;
import org.example.models.EyeGazeReading;
import org.example.models.MoCapReading;
import org.example.models.RulaScore;

// Import Source Providers
import org.example.sources.provider.EMGKafkaSourceProvider;
import org.example.sources.provider.EyeGazeKafkaSourceProvider;
import org.example.sources.provider.MoCapKafkaSourceProvider;

// Import Processors
import org.example.processing.emg.EMGToPythonForwarder; // For sending specific EMG data to Python
// import org.example.processing.emg.EMGFatigueProcessor; // RMS Fatigue Processor - TO BE COMMENTED OUT
import org.example.processing.eyegaze.EyeGazeAttentionProcessor;
import org.example.processing.mocap.MoCapAverageAngleProcessor;
import org.example.processing.mocap.MoCapRulaProcessor;

// Import DB Sinks
import org.example.sinks.db.AvgAngleAlertDbSink;
import org.example.sinks.db.EMGFatigueAlertDbSink; // Will be used for MDF alerts from Python
import org.example.sinks.db.EMGRawDbSink;
import org.example.sinks.db.EyeGazeAttentionAlertDbSink;
import org.example.sinks.db.EyeGazeRawDbSink;
import org.example.sinks.db.MoCapRawDbSink;
import org.example.sinks.db.RulaScoreDbSink;

// Import Kafka Sinks
import org.example.sinks.kafka.EMGFatigueAlertKafkaSink; // Can be used for MDF alerts to a Kafka topic
import org.example.sinks.kafka.EyeGazeAlertKafkaSink;
import org.example.sinks.kafka.MoCapErgonomicsAlertKafkaSink;


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
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(); // <-- THIS IS THE FIX

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


            // --- MoCap Processing ---
            logger.info("Configuring MoCap Processing (Average Angles and RULA)...");
            DataStream<String> averageAngleAlerts = MoCapAverageAngleProcessor.processAverageAnglesForFeedback(moCapStream);
            DataStream<String> rulaScoreJsonStream = moCapStream
                .map(new MoCapRulaProcessor.RulaScoreMapFunction(
                        ProcessingParamsConfig.RULA_DEFAULT_LOAD_KG,
                        ProcessingParamsConfig.RULA_MUSCLE_USE_SCORE_ARM_WRIST,
                        ProcessingParamsConfig.RULA_MUSCLE_USE_SCORE_NECK_TRUNK_LEG
                ))
                .filter(json -> json != null && !json.isEmpty())
                .name("CalculateRULAScore");

            // --- EMG Processing ---
            // logger.info("Configuring EMG RMS Fatigue Processing...");
            // DataStream<String> emgRmsFatigueAlerts = EMGFatigueProcessor.processEMGFatigueRMS(
            //         emgStream,
            //         ProcessingParamsConfig.EMG_MUSCLES_TO_MONITOR // This was for RMS
            // );
            // logger.info("EMG RMS Fatigue Processing pipeline defined (currently commented out for MDF focus).");


            // logger.info("Configuring EMG Data Forwarding for Python MDF Processing...");
            // DataStream<String> emgDataForPythonMdf = EMGToPythonForwarder.forwardMuscleDataForPython(emgStream);
            
            // KafkaSink<String> pythonMdfInputSink = KafkaSink.<String>builder()
            //     .setBootstrapServers(FlinkJobConfig.KAFKA_BROKERS)
            //     .setRecordSerializer(org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema.builder()
            //             .setTopic(KafkaConfig.EMG_MDF_INPUT_TOPIC)
            //             .setValueSerializationSchema(new SimpleStringSchema())
            //             .build())
            //     .build();
            // emgDataForPythonMdf.sinkTo(pythonMdfInputSink).name("EMGDataToPythonMdfSink");
            // emgDataForPythonMdf.print("EMG_TO_PYTHON_MDF");


            // --- Eye Gaze Processing ---
            logger.info("Configuring Eye Gaze Attention Processing...");
            DataStream<String> gazeAlerts = EyeGazeAttentionProcessor.processGazeAttention(
                    gazeStream,
                    ProcessingParamsConfig.EYE_GAZE_PROLONGED_INATTENTION_DURATION_THRESHOLD
            );
            
            // --- Consume MDF Alerts from Python ---
            // logger.info("Configuring Flink to consume MDF alerts from Python...");
            // KafkaSource<String> mdfAlertsFromPythonSource = KafkaSource.<String>builder()
            //     .setBootstrapServers(FlinkJobConfig.KAFKA_BROKERS)
            //     .setTopics(KafkaConfig.EMG_MDF_FATIGUE_ALERT_FROM_PYTHON_TOPIC) // Topic Python publishes MDF alerts to
            //     .setGroupId("flink-mdf-alert-consumer-group-" + System.currentTimeMillis()) // Unique group ID for fresh start
            //     .setStartingOffsets(OffsetsInitializer.latest())
            //     .setValueOnlyDeserializer(new SimpleStringSchema())
            //     .build();

            // DataStream<String> mdfFatigueAlertsStream = env.fromSource(mdfAlertsFromPythonSource,
            //                                                       WatermarkStrategy.noWatermarks(),
            //                                                       "MdfAlertsFromPythonSource")
            //                                                   .name("FilterValidMdfAlertsFromPython");
            
            // mdfFatigueAlertsStream.print("MDF_FATIGUE_ALERT_FROM_PYTHON");


            // --- Sinks ---
            logger.info("Configuring Sinks (Kafka and Database)...");
            String dbUrlBase = DBConfig.DB_URL;
            String dbUser = DBConfig.DB_USER;
            String dbPassword = DBConfig.DB_PASSWORD;

            // Kafka Sinks for Processed Data / Alerts (Excluding Smartwatch)
            averageAngleAlerts.print("AVG_ANGLE_ALERT_MOCAP");
            averageAngleAlerts.sinkTo(MoCapErgonomicsAlertKafkaSink.getKafkaSink(FlinkJobConfig.KAFKA_BROKERS, KafkaConfig.MOCAP_AVERAGE_ANGLE_ALERTS_SINK_TOPIC))
                              .name("MocapAvgAngleAlertKafkaSink");

            rulaScoreJsonStream.print("RULA_SCORE_JSON");
            rulaScoreJsonStream.sinkTo(MoCapErgonomicsAlertKafkaSink.getKafkaSink(FlinkJobConfig.KAFKA_BROKERS, KafkaConfig.RULA_SCORES_SINK_TOPIC))
                               .name("RulaScoreKafkaSink");

            gazeAlerts.print("GAZE_ATTENTION_ALERT");
            gazeAlerts.sinkTo(EyeGazeAlertKafkaSink.getKafkaSink(FlinkJobConfig.KAFKA_BROKERS, KafkaConfig.EYEGAZE_ATTENTION_ALERTS_SINK_TOPIC))
                      .name("GazeAttentionAlertKafkaSink");
            
            // Kafka Sink for MDF Fatigue Alerts (received from Python, then re-published by Flink if needed by other Kafka consumers, e.g. MQTT bridge)
            // The Python script already publishes to KafkaConfig.EMG_MDF_FATIGUE_ALERT_FROM_PYTHON_TOPIC.
            // If the MQTT bridge directly consumes from this topic, Flink doesn't need to re-publish it.
            // If Flink needs to publish it to a *different* topic for the bridge, or if you want Flink to manage this output:
            // mdfFatigueAlertsStream.sinkTo(EMGFatigueAlertKafkaSink.getKafkaSink(FlinkJobConfig.KAFKA_BROKERS, KafkaConfig.EMG_MDF_FATIGUE_ALERT_FROM_PYTHON_TOPIC)) // or a new specific topic
            //                         .name("MdfFatigueAlertToKafkaBridgeSink");
            // For now, assuming the Python script's output topic is directly used by the bridge.


            // Database Sinks - Raw Data (Excluding Smartwatch)
            moCapStream.sinkTo(new MoCapRawDbSink(dbUrlBase + DBConfig.MOCAP_DB_NAME, DBConfig.MOCAP_RAW_DATA_TABLE, dbUser, dbPassword))
                       .name("RawMoCapDbSink");
            emgStream.sinkTo(new EMGRawDbSink(dbUrlBase, DBConfig.EMG_DB_NAME, dbUser, dbPassword))
                     .name("RawEMGDbSink");
            gazeStream.sinkTo(new EyeGazeRawDbSink(dbUrlBase + DBConfig.EYEGAZE_DB_NAME, DBConfig.EYEGAZE_RAW_DATA_TABLE, dbUser, dbPassword))
                      .name("RawEyeGazeDbSink");

            // Database Sinks - Processed Data (Excluding Smartwatch, Focusing on MDF for EMG Fatigue)
            averageAngleAlerts.sinkTo(new AvgAngleAlertDbSink(dbUrlBase + DBConfig.MOCAP_PROCESSED_DB_NAME, DBConfig.MOCAP_AVERAGE_ANGLES_ALERTS_TABLE, dbUser, dbPassword))
                              .name("AvgAngleAlertDbSink");
            rulaScoreJsonStream.sinkTo(new RulaScoreDbSink(dbUrlBase + DBConfig.MOCAP_PROCESSED_DB_NAME, DBConfig.RULA_SCORES_TABLE, dbUser, dbPassword))
                               .name("RulaScoreDbSink");
            
            // Sink for MDF Fatigue Alerts (received from Python)
            // mdfFatigueAlertsStream.sinkTo(new EMGFatigueAlertDbSink(dbUrlBase + DBConfig.EMG_PROCESSED_DB_NAME, DBConfig.EMG_FATIGUE_ALERTS_TABLE, dbUser, dbPassword))
            //                           .name("EMGMdfFatigueAlertDbSink");
            
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