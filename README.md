# Flink Application for HRC Human Factor Monitoring

This project implements the real-time stream processing component of the Master's Thesis: "Enhancing Safety through Human Factor Monitoring in Virtual Reality". It uses the Apache Flink DataStream API to process multi-modal sensor data from a Human-Robot Collaboration (HRC) training simulation, calculate ergonomic risks (RULA) and attention levels, forward EMG data for external fatigue analysis, and generate alerts for a closed-loop feedback system.

## Motivation

Human-Robot Collaboration (HRC) in industrial settings presents significant risks related to worker physical strain and cognitive fatigue. This project aims to mitigate these risks by developing an immersive VR training system that monitors human factors in real-time and provides adaptive feedback to both the human operator and the collaborating robot, thereby enhancing safety and performance. This Flink application is the core real-time analysis engine for that system.

## System Architecture Overview

This Flink application is part of a larger distributed system:

1.  **VR Simulation (Unity):** Simulates the HRC task, publishes raw gaze data, and subscribes to feedback alerts.
2.  **Sensors:** Rokoko Suit (MoCap), Myontech EMG Shirt, and an eye tracker stream data, which is marshaled onto a central Kafka bus.
3.  **Messaging:** Apache Kafka serves as the central, durable data bus for all sensor streams and alerts.
4.  **Real-time Processing (THIS PROJECT):** Apache Flink consumes Kafka streams, performs analysis, and publishes results/alerts back to Kafka.
5.  **External Python EMG Service:** A separate Python service consumes raw EMG data from Kafka, performs Median Frequency (MDF) analysis for fatigue detection, and publishes processed features and fatigue alerts back to Kafka.
6.  **Database:** TimescaleDB stores all raw and processed time-series data for historical analysis and dashboarding.
7.  **Visualization:** Grafana dashboards display real-time and historical data from TimescaleDB.
8.  **Bridges:** A component like Node-RED can bridge Kafka alert topics to other protocols (e.g., MQTT) for consumption by the VR simulation.

## Data Processing Pipelines

The Flink application is composed of three main data processing pipelines:

### 1. Motion Capture (MoCap) Ergonomics
* **Source Topic:** `k_mocap_rokoko01_angles`
* **Processing:**
    1.  **Sliding Window Posture Analysis:** Monitors specific joint angles (`torso_tilt`, `shoulder_flex_ext_right`, etc.) over sliding time windows. It generates "YELLOW" and "RED" alerts if a user maintains a non-neutral posture for a significant percentage of the window, indicating sustained strain.
    2.  **RULA Scoring:** Calculates an instantaneous RULA (Rapid Upper Limb Assessment) score for each MoCap reading. This provides a holistic ergonomic risk score based on the posture of the upper arms, lower arms, wrists, neck, and trunk.
* **Output Kafka Topics:**
    * `th_mocap_average_angle` (for sliding window alerts)
    * `th_mocap_rula_scores` (for RULA scores)
* **Output DB Tables:**
    * `th_MoCapRawData`
    * `th_MoCapAverageAngleAlerts`
    * `th_RulaScores`

### 2. Eye Gaze Attention
* **Source Topic:** `k_unity_gaze_attention`
* **Processing:**
    1.  **Prolonged Inattention:** Uses a `KeyedProcessFunction` to detect continuous periods where the user's `attention` state is `false`, triggering an alert if the duration exceeds a threshold (e.g., 5 seconds).
    2.  **Low Average Attention:** Calculates the percentage of time the user was inattentive over a larger sliding window (e.g., 1 minute). An alert is generated if this percentage is too high.
* **Output Kafka Topic:** `th_eyegaze_attention_alerts`
* **Output DB Tables:**
    * `th_EyeGazeRawData`
    * `th_EyeGazeAttentionAlerts`

### 3. Electromyography (EMG) Fatigue
* **Source Topics:**
    * `k_myontech_shirt01_emg` (Left Arm)
    * `k_myontech_shirt02_emg` (Right Arm)
    * `k_myontech_shirt03_emg` (Trunk)
    * `th_emg_libemg_features_visualization` (Processed features from Python)
* **Processing:**
    1.  **Raw Data Ingestion:** Consumes raw EMG readings from the three shirt topics and sinks them directly to corresponding tables in TimescaleDB for archival.
    2.  **Processed Feature Ingestion:** Consumes processed EMG features (RMS, MDF, etc.) calculated by an external Python service. These features are then saved to the database. This demonstrates a hybrid Flink/Python architecture for specialized analysis.
* **Output DB Tables:**
    * `th_emg_raw_data_01`, `th_emg_raw_data_02`, `th_emg_raw_data_03`
    * `th_emg_extracted_features`

## Technologies Used

- **Core Framework:** Apache Flink 1.19.0 (DataStream API)
- **Language:** Java 17
- **Build Tool:** Apache Maven
- **Messaging:** Apache Kafka Connector for Flink
- **Database:** PostgreSQL JDBC Connector (for TimescaleDB)
- **Libraries:**
  - Gson (JSON Parsing/Serialization)
  - SLF4J (Logging)

## Setup & Prerequisites

1.  **Java:** JDK 17 or later.
2.  **Maven:** Version 3.6+ installed and configured.
3.  **Apache Flink:** A running Flink cluster (v1.19.0 recommended).
4.  **Apache Kafka:** A running Kafka cluster. Ensure all topics listed in `KafkaConfig.java` are created.
5.  **TimescaleDB/PostgreSQL:** A running database instance with the schemas and tables defined in `DBConfig.java` created.
6.  **External Python EMG Service:** The service must be running and configured to consume from the raw EMG topics and produce to the features topic.
7.  **Configuration:** Update connection details in the `org.example.config` package, primarily:
    - `FlinkJobConfig.java` (Kafka broker address)
    - `DBConfig.java` (Database connection details)

## Building the Project

The project is configured to build a "fat JAR" that includes all necessary dependencies.

```bash
mvn clean package
```

This command will produce the JAR file in the `target/` directory (e.g., `target/HRC-CEP-1.0-SNAPSHOT.jar`).

## Running the Flink Job

Submit the generated JAR file to your Flink cluster using the Flink CLI.

```bash
<FLINK_HOME>/bin/flink run \
    -c org.example.Main \
    target/HRC-CEP-1.0-SNAPSHOT.jar
```

Alternatively, you can submit the job via the Flink Web UI by uploading the JAR and specifying `org.example.Main` as the entry class.

## Project Structure

```
.
├── pom.xml
└── src
    └── main
        └── java
            └── org
                └── example
                    ├── Main.java                 # Main Flink Job definition
                    ├── config/                   # Configuration files
                    │   ├── DBConfig.java
                    │   ├── FlinkJobConfig.java
                    │   ├── KafkaConfig.java
                    │   └── ProcessingParamsConfig.java
                    ├── models/                   # POJO data models
                    │   ├── EMGReading.java
                    │   ├── EmgFeatureMessage.java
                    │   ├── EyeGazeReading.java
                    │   ├── MoCapReading.java
                    │   └── RulaScore.java
                    ├── processing/               # Core stream processing logic
                    │   ├── eyegaze/
                    │   │   └── EyeGazeAttentionProcessor.java
                    │   └── mocap/
                    │       ├── MoCapRulaProcessor.java
                    │       └── MoCapSlidingWindowAlertProcessor.java
                    ├── sinks/                    # Kafka and DB Sinks
                    │   ├── db/
                    │   │   ├── AvgAngleAlertDbSink.java
                    │   │   ├── EMGRawDbSink.java
                    │   │   ├── EyeGazeAttentionAlertDbSink.java
                    │   │   ├── EyeGazeRawDbSink.java
                    │   │   ├── ExtractedFeaturesDbSink.java
                    │   │   ├── MoCapRawDbSink.java
                    │   │   └── RulaScoreDbSink.java
                    │   └── kafka/
                    │       ├── EMGFatigueAlertKafkaSink.java
                    │       ├── EyeGazeAlertKafkaSink.java
                    │       └── MoCapErgonomicsAlertKafkaSink.java
                    └── sources/                  # Kafka Source providers and Deserializers
                        ├── deserializer/
                        │   ├── EMGDeserializationSchema.java
                        │   ├── EmgFeatureDeserializationSchema.java
                        │   ├── EyeGazeDeserializationSchema.java
                        │   └── MoCapDeserializationSchema.java
                        └── provider/
                            ├── EMGKafkaSourceProvider.java
                            ├── EmgFeatureKafkaSourceProvider.java
                            ├── EyeGazeKafkaSourceProvider.java
                            └── MoCapKafkaSourceProvider.java
```
