// File: Flink-CEP/src/main/java/org/example/models/EMGReading.java
package org.example.models;

import java.io.Serializable;
import java.util.StringJoiner;

// Renamed from EMGSensorReading
public class EMGReading implements Serializable {

    private static final long serialVersionUID = 102L; // Unique ID

    // Field names MUST match the incoming JSON keys from Kafka
    private String thingid;
    private String timestamp; // Original timestamp string "yyyy-MM-dd HH:mm:ss"

    // Left Arm Muscles (from k_myontech_shirt01_emg)
    private double deltoids_left;
    private double triceps_left;
    private double biceps_left;
    private double wrist_extensors_left;
    private double wrist_flexor_left;

    // Right Arm Muscles (from k_myontech_shirt02_emg)
    private double deltoids_right;
    private double triceps_right;
    private double biceps_right;
    private double wrist_extensors_right;
    private double wrist_flexor_right;

    // Torso/Back/Chest Muscles (from k_myontech_shirt03_emg)
    private double trapezius_left;
    private double trapezius_right;
    private double pectoralis_left;
    private double pectoralis_right;
    private double latissimus_left;
    private double latissimus_right;

    // Default constructor (required for Flink POJO serialization)
    public EMGReading() {
    }

    // Getters and Setters for all fields (required for Flink POJO/JSON mapping)
    // (Keep all existing getters and setters from EMGSensorReading.java)
    public String getThingid() { return thingid; }
    public void setThingid(String thingid) { this.thingid = thingid; }
    public String getTimestamp() { return timestamp; }
    public void setTimestamp(String timestamp) { this.timestamp = timestamp; }
    public double getDeltoids_left() { return deltoids_left; }
    public void setDeltoids_left(double deltoids_left) { this.deltoids_left = deltoids_left; }
    public double getTriceps_left() { return triceps_left; }
    public void setTriceps_left(double triceps_left) { this.triceps_left = triceps_left; }
    public double getBiceps_left() { return biceps_left; }
    public void setBiceps_left(double biceps_left) { this.biceps_left = biceps_left; }
    public double getWrist_extensors_left() { return wrist_extensors_left; }
    public void setWrist_extensors_left(double wrist_extensors_left) { this.wrist_extensors_left = wrist_extensors_left; }
    public double getWrist_flexor_left() { return wrist_flexor_left; }
    public void setWrist_flexor_left(double wrist_flexor_left) { this.wrist_flexor_left = wrist_flexor_left; }
    public double getDeltoids_right() { return deltoids_right; }
    public void setDeltoids_right(double deltoids_right) { this.deltoids_right = deltoids_right; }
    public double getTriceps_right() { return triceps_right; }
    public void setTriceps_right(double triceps_right) { this.triceps_right = triceps_right; }
    public double getBiceps_right() { return biceps_right; }
    public void setBiceps_right(double biceps_right) { this.biceps_right = biceps_right; }
    public double getWrist_extensors_right() { return wrist_extensors_right; }
    public void setWrist_extensors_right(double wrist_extensors_right) { this.wrist_extensors_right = wrist_extensors_right; }
    public double getWrist_flexor_right() { return wrist_flexor_right; }
    public void setWrist_flexor_right(double wrist_flexor_right) { this.wrist_flexor_right = wrist_flexor_right; }
    public double getTrapezius_left() { return trapezius_left; }
    public void setTrapezius_left(double trapezius_left) { this.trapezius_left = trapezius_left; }
    public double getTrapezius_right() { return trapezius_right; }
    public void setTrapezius_right(double trapezius_right) { this.trapezius_right = trapezius_right; }
    public double getPectoralis_left() { return pectoralis_left; }
    public void setPectoralis_left(double pectoralis_left) { this.pectoralis_left = pectoralis_left; }
    public double getPectoralis_right() { return pectoralis_right; }
    public void setPectoralis_right(double pectoralis_right) { this.pectoralis_right = pectoralis_right; }
    public double getLatissimus_left() { return latissimus_left; }
    public void setLatissimus_left(double latissimus_left) { this.latissimus_left = latissimus_left; }
    public double getLatissimus_right() { return latissimus_right; }
    public void setLatissimus_right(double latissimus_right) { this.latissimus_right = latissimus_right; }


    // toString method for debugging
    @Override
    public String toString() {
        // (Keep the existing toString method from EMGSensorReading.java)
        return new StringJoiner(", ", EMGReading.class.getSimpleName() + "[", "]")
                .add("thingid='" + thingid + "'")
                .add("timestamp='" + timestamp + "'")
                .add("deltoids_left=" + deltoids_left)
                // ... include all other fields ...
                .add("latissimus_right=" + latissimus_right)
                .toString();
    }
}