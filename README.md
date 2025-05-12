# Flink CEP for HRC Human Factor Monitoring

This project implements the real-time stream processing component of the Master's Thesis: "Enhancing Safety through Human Factor Monitoring in Virtual Reality". It uses Apache Flink (primarily the DataStream API, with some CEP elements for specific rules like high heart rate) to process multi-modal sensor data from a Human-Robot Collaboration (HRC) training simulation, calculate ergonomic risks and fatigue indicators, and generate alerts for a closed-loop feedback system.

## Motivation

Human-Robot Collaboration (HRC) in industrial settings presents significant risks related to worker physical strain and cognitive fatigue. This project aims to mitigate these risks by developing an immersive VR training system that monitors human factors in real-time and provides adaptive feedback to both the human operator and the collaborating robot, thereby enhancing safety and performance. This Flink application is the core real-time analysis engine for that system.

## System Architecture Overview

This Flink application is part of a larger distributed system:

1.  **VR Simulation (Unity):** Simulates the HRC task (KUKA robot, user interaction via XRI), publishes raw gaze data, and subscribes to feedback alerts/commands via MQTT.
2.  **Sensors:** Rokoko Suit (MoCap), Myontech EMG Shirt, Smartwatch (HR/ECG), HP Omnicept G2 (Gaze) stream data, primarily via MQTT.
3.  **Messaging:** MQTT Broker (edge communication), Apache Kafka (central data bus).
4.  **Real-time Processing (THIS PROJECT):** Apache Flink consumes Kafka streams, performs analysis (Ergonomics, REBA, Gaze, EMG RMS Fatigue), and publishes results/alerts back to Kafka. A separate Python service handles EMG MDF analysis.
5.  **Database:** TimescaleDB stores raw and processed time-series data.
6.  **Visualization:** Grafana dashboards display data from TimescaleDB.
7.  **Bridges:** Node-RED (or similar) bridges Kafka alert topics back to MQTT for Unity consumption. `ros-tcp-connector` bridges Unity and ROS 2.
8.  **Robot Control:** ROS 2 Jazzy with MoveIt 2 handles motion planning for the robot simulation.

## Technologies Used (Flink CEP Component)

* **Core Framework:** Apache Flink 1.19.0 (DataStream API, CEP)
* **Language:** Java 17
* **Build Tool:** Apache Maven
* **Messaging:** Apache Kafka (via Flink Kafka Connector)
* **Database:** TimescaleDB/PostgreSQL (via Flink JDBC Connector)
* **Libraries:**
    * Gson (JSON Parsing/Serialization)
    * SLF4j (Logging)
    * JTransforms, Apache Commons Math3

## Setup & Prerequisites

1.  **Java:** JDK 17 or later.
2.  **Maven:** Version 3.6+ installed and configured.
3.  **Apache Flink:** A running Flink cluster (Standalone, Docker, Kubernetes, etc.) accessible from where you submit the job. Version 1.19.0 recommended.
4.  **Apache Kafka:** A running Kafka cluster (with Zookeeper).
5.  **TimescaleDB/PostgreSQL:** A running database instance with the necessary schemas and tables created (see `DBConfig.java` and sink implementations for table names/structures). Ensure hypertables are enabled if using TimescaleDB features.
6.  **MQTT Broker:** A running MQTT broker (e.g., Mosquitto).
7.  **Node-RED (or similar):** A running instance configured to bridge specific Kafka alert topics to corresponding MQTT topics for Unity.
8.  **(External) Python EMG Service:** The separate Python service for MDF analysis needs to be running, consuming from raw EMG Kafka topics and producing to the `emgFatigueAlert` Kafka topic.
9.  **(External) Unity/ROS Simulation:** The VR simulation environment publishing sensor data and subscribing to feedback.
10. **Configuration:** Update connection details (IP addresses, ports, credentials) and topic/table names in:
    * `src/main/java/org/example/config/KafkaConfig.java`
    * `src/main/java/org/example/config/DBConfig.java`

## Building the Project

Navigate to the project's root directory (`Flink-CEP/`) in your terminal and run:

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

- Real-time consumption of MoCap, EMG, Smartwatch HR, and Eye Gaze data from Kafka.
- Ergonomic analysis: Sliding window average joint angle alerts.
- Ergonomic analysis: Instantaneous REBA scoring.
- Gaze analysis: Detection of prolonged and low-average inattention.
- Fatigue analysis: Detection based on sustained high EMG RMS.
- Smartwatch analysis: Basic CEP rule for consecutive high HR, sliding window average HR calculation.
- Sinking of raw data, REBA scores, and various alerts to TimescaleDB.
- Publishing of alerts to Kafka topics for downstream consumption (e.g., by Node-RED bridge).

**Planned:**

- HRV-based stress/cognitive load analysis (Flink).
- Refinement of EMG fatigue thresholds.
- Refinement of REBA calculation details.
- Potentially more advanced gaze metrics.

## Project Structure

```
Flink-CEP/
├── pom.xml
└── src/
    └── main/
        └── java/
            └── org/
                └── example/
                    ├── Main.java
                    ├── config/
                    │   ├── DBConfig.java
                    │   └── KafkaConfig.java
                    ├── models/
                    │   ├── MoCapReading.java
                    │   ├── EMGReading.java
                    │   ├── SmartwatchReading.java
                    │   ├── EyeGazeReading.java
                    │   └── RebaScore.java
                    ├── processing/
                    │   ├── mocap/
                    │   │   ├── MoCapErgonomicsProcessor.java
                    │   │   └── MoCapRebaProcessor.java
                    │   ├── emg/
                    │   │   └── EMGFatigueProcessor.java
                    │   ├── smartwatch/
                    │   │   ├── SmartwatchAvgHrProcessor.java
                    │   │   └── SmartwatchHrvProcessor.java
                    │   └── eyegaze/
                    │       └── EyeGazeAttentionProcessor.java
                    ├── sinks/
                    │   ├── db/
                    │   │   ├── MoCapRawDbSink.java
                    │   │   ├── RebaScoreDbSink.java
                    │   │   ├── AvgAngleAlertDbSink.java
                    │   │   ├── EMGRawDbSink.java
                    │   │   ├── EMGFatigueAlertDbSink.java
                    │   │   ├── SmartwatchRawDbSink.java
                    │   │   ├── SmartwatchAvgHrDbSink.java
                    │   │   ├── EyeGazeRawDbSink.java
                    │   │   └── EyeGazeAttentionAlertDbSink.java
                    │   └── kafka/
                    │       ├── MoCapErgonomicsAlertKafkaSink.java
                    │       ├── EMGFatigueAlertKafkaSink.java
                    │       ├── SmartwatchAvgHrAlertKafkaSink.java
                    │       ├── SmartwatchHrvAlertKafkaSink.java
                    │       └── EyeGazeAlertKafkaSink.java
                    └── sources/
                        ├── provider/
                        │   ├── MoCapKafkaSourceProvider.java
                        │   ├── EMGKafkaSourceProvider.java
                        │   ├── SmartwatchKafkaSourceProvider.java
                        │   └── EyeGazeKafkaSourceProvider.java
                        └── deserializer/
                            ├── MoCapDeserializationSchema.java
                            ├── EMGDeserializationSchema.java
                            ├── SmartwatchDeserializationSchema.java
                            └── EyeGazeDeserializationSchema.java
```
