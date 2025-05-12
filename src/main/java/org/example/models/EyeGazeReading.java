// File: Flink-CEP/src/main/java/org/example/models/EyeGazeReading.java
package org.example.models;

import java.io.Serializable;
import java.util.Objects;

// Renamed from EyeGazeSensorReading
public class EyeGazeReading implements Serializable {

    private static final long serialVersionUID = 104L; // Unique ID

    // Field names MUST match the incoming JSON keys from Kafka
    private String thingid;
    private String timestamp; // ISO 8601 format "yyyy-MM-ddTHH:mm:ss.fffffffZ" from C#
    private boolean attention; // true if looking at target, false otherwise

    // Default constructor for Flink/Serialization
    public EyeGazeReading() {
    }

    // Getters and Setters
    // (Keep all existing getters and setters from EyeGazeSensorReading.java)
    public String getThingid() { return thingid; }
    public void setThingid(String thingid) { this.thingid = thingid; }
    public String getTimestamp() { return timestamp; }
    public void setTimestamp(String timestamp) { this.timestamp = timestamp; }
    public boolean isAttention() { return attention; } // Use "is" prefix for boolean getter
    public void setAttention(boolean attention) { this.attention = attention; }


    // --- Optional: equals, hashCode, toString for debugging/testing ---
    // (Keep all existing equals, hashCode, toString methods from EyeGazeSensorReading.java)
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        EyeGazeReading that = (EyeGazeReading) o;
        return attention == that.attention &&
               Objects.equals(thingid, that.thingid) &&
               Objects.equals(timestamp, that.timestamp);
    }
    @Override
    public int hashCode() { return Objects.hash(thingid, timestamp, attention); }
    @Override
    public String toString() {
        return "EyeGazeReading{" +
               "thingid='" + thingid + '\'' +
               ", timestamp='" + timestamp + '\'' +
               ", attention=" + attention +
               '}';
    }
}