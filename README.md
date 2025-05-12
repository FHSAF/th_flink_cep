# Flink Application for HRC Human Factor Monitoring

This project implements the real-time stream processing component of the Master's Thesis: "Enhancing Safety through Human Factor Monitoring in Virtual Reality". It uses Apache Flink (DataStream API) to process multi-modal sensor data from a Human-Robot Collaboration (HRC) training simulation, calculate ergonomic risks (RULA) and attention levels, forward EMG data for external fatigue analysis, and generate alerts for a closed-loop feedback system.

## Motivation

Human-Robot Collaboration (HRC) in industrial settings presents significant risks related to worker physical strain and cognitive fatigue. This project aims to mitigate these risks by developing an immersive VR training system that monitors human factors in real-time and provides adaptive feedback to both the human operator and the collaborating robot, thereby enhancing safety and performance. This Flink application is a core real-time analysis engine for that system.

## System Architecture Overview

This Flink application is part of a larger distributed system:

1. **VR Simulation (Unity):** Simulates the HRC task (KUKA robot, user interaction via XRI), publishes raw gaze data, and subscribes to feedback alerts/commands via MQTT.
2. **Sensors:** Rokoko Suit (MoCap), Myontech EMG Shirt, HP Omnicept G2 (Gaze) stream data, primarily via MQTT.
3. **Messaging:** MQTT Broker (edge communication), Apache Kafka (central data bus).
4. **Real-time Processing (THIS PROJECT):** Apache Flink consumes Kafka streams, performs analysis (Ergonomics - Average Angles, RULA; Gaze Attention), forwards EMG data to a Python service for Median Frequency (MDF) fatigue analysis, consumes MDF fatigue alerts from Python, and publishes results/alerts back to Kafka.
5. **External Python EMG Service:** A separate Python service consumes raw EMG data (specific muscles) from a Kafka topic, performs MDF analysis for fatigue detection, and publishes fatigue alerts to another Kafka topic.
6. **Database:** TimescaleDB stores raw and processed time-series data.
7. **Visualization:** Grafana dashboards display data from TimescaleDB.
8. **Bridges:** Node-RED (or similar) bridges Kafka alert topics back to MQTT for Unity consumption. `ros-tcp-connector` bridges Unity and ROS 2.
9. **Robot Control:** ROS 2 Jazzy with MoveIt 2 handles motion planning for the robot simulation.

## Technologies Used (Flink Application)

- **Core Framework:** Apache Flink 1.19.0 (DataStream API)
- **Language:** Java 17
- **Build Tool:** Apache Maven
- **Messaging:** Apache Kafka (via Flink Kafka Connector)
- **Database:** TimescaleDB/PostgreSQL (via Flink JDBC Connector)
- **Libraries:**
  - Gson (JSON Parsing/Serialization)
  - SLF4j (Logging)
  - JTransforms, Apache Commons Math3 (Potentially used by specific processors)

## Setup & Prerequisites

1. **Java:** JDK 17 or later.
2. **Maven:** Version 3.6+ installed and configured.
3. **Apache Flink:** A running Flink cluster (Standalone, Docker, Kubernetes, etc.). Version 1.19.0 recommended.
4. **Apache Kafka:** A running Kafka cluster (with Zookeeper). Ensure all necessary topics are created (see `KafkaConfig.java` and `Main.java`).
5. **TimescaleDB/PostgreSQL:** A running database instance with the necessary schemas and tables created (see `DBConfig.java`). Ensure hypertables are enabled if using TimescaleDB features.
6. **MQTT Broker:** A running MQTT broker (e.g., Mosquitto).
7. **Node-RED (or similar):** A running instance to bridge Kafka alert topics to MQTT.
8. **External Python EMG MDF Service:** Should consume from `emg_mdf_input` Kafka topic and produce alerts to `emg_mdf_fatigue_alerts_from_python`.
9. **(External) Unity/ROS Simulation:** The VR simulation environment publishing sensor data and subscribing to feedback.
10. **Configuration:** Update connection details and topic/table names in:
    - `FlinkJobConfig.java`
    - `KafkaConfig.java`
    - `DBConfig.java`
    - `ProcessingParamsConfig.java`

## Building the Project

```bash
mvn clean package
```

This will compile the code and create a fat JAR (including dependencies) in the target/ directory (e.g., `target/HRC-CEP-1.0-SNAPSHOT.jar`).

## Running the Flink Job

Submit the generated JAR file to your Flink cluster.

**Using Flink CLI:**

```bash
<FLINK_HOME>/bin/flink run \
    -m <JOBMANAGER_HOST>:<JOBMANAGER_REST_PORT> \
    -c org.example.Main \
    target/HRC-CEP-1.0-SNAPSHOT.jar
```

**Using Flink Web UI:**

1. Navigate to your Flink Dashboard (usually http://<JOBMANAGER_HOST>:8081)
2. Go to "Submit New Job"
3. Upload the `target/HRC-CEP-1.0-SNAPSHOT.jar` file
4. Specify the Entry Class: `org.example.Main`
5. Configure parallelism and other settings as needed
6. Click "Submit"

## Current Status & Features

**Implemented:**

- Real-time consumption of MoCap, EMG, and Eye Gaze data from Kafka.
- Ergonomic analysis: Sliding window average joint angle alerts.
- Ergonomic analysis: Instantaneous RULA scoring.
- Gaze analysis: Detection of prolonged and low-average inattention.
- Fatigue analysis: Detection based on sustained high EMG RMS.
- Sinking of raw data, RULA scores, and various alerts to TimescaleDB.
- Publishing of alerts to Kafka topics for downstream consumption (e.g., by Node-RED bridge).

**Planned:**

- Refinement of EMG fatigue thresholds.
- Refinement of RULA calculation details.
- Potentially more advanced gaze metrics.

## Project Structure

```
Flink-CEP/
HRC-CEP/
├── pom.xml
└── src/
    └── main/
        └── java/
            └── org/
                └── example/
                    ├── Main.java
                    ├── config/
                    │   ├── DBConfig.java
                    │   ├── FlinkJobConfig.java
                    │   ├── KafkaConfig.java
                    │   └── ProcessingParamsConfig.java
                    ├── models/
                    │   ├── EMGReading.java
                    │   ├── EyeGazeReading.java
                    │   ├── MoCapReading.java
                    │   └── RulaScore.java
                    ├── processing/
                    │   ├── emg/
                    │   │   └── EMGToPythonForwarder.java
                    │   ├── eyegaze/
                    │   │   └── EyeGazeAttentionProcessor.java
                    │   └── mocap/
                    │       ├── MoCapAverageAngleProcessor.java
                    │       └── MoCapRulaProcessor.java
                    ├── sinks/
                    │   ├── db/
                    │   │   ├── AvgAngleAlertDbSink.java
                    │   │   ├── EMGFatigueAlertDbSink.java
                    │   │   ├── EMGRawDbSink.java
                    │   │   ├── EyeGazeAttentionAlertDbSink.java
                    │   │   ├── EyeGazeRawDbSink.java
                    │   │   ├── MoCapRawDbSink.java
                    │   │   └── RulaScoreDbSink.java
                    │   └── kafka/
                    │       ├── EMGFatigueAlertKafkaSink.java
                    │       ├── EyeGazeAlertKafkaSink.java
                    │       └── MoCapErgonomicsAlertKafkaSink.java
                    └── sources/
                        ├── deserializer/
                        │   ├── EMGDeserializationSchema.java
                        │   ├── EyeGazeDeserializationSchema.java
                        │   └── MoCapDeserializationSchema.java
                        └── provider/
                            ├── EMGKafkaSourceProvider.java
                            ├── EyeGazeKafkaSourceProvider.java
                            └── MoCapKafkaSourceProvider.java
```
