// File: Flink-CEP/src/main/java/org/example/models/MoCapReading.java
package org.example.models;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.Serializable; // Import Serializable

// Renamed from SensorReading - Represents Rokoko MoCap data
public class MoCapReading implements Serializable { // Implement Serializable
    private static final long serialVersionUID = 101L; // Unique ID for this version
    private static final Logger logger = LoggerFactory.getLogger(MoCapReading.class);

    // Field names MUST match the incoming JSON keys from Kafka
    private String thingid;
    private double elbow_flex_ext_left;
    private double elbow_flex_ext_right;
    private double shoulder_flex_ext_left;
    private double shoulder_flex_ext_right;
    private double shoulder_abd_add_left;
    private double shoulder_abd_add_right;
    private double lowerarm_pron_sup_left;
    private double lowerarm_pron_sup_right;
    private double upperarm_rotation_left;
    private double upperarm_rotation_right;
    private double hand_flex_ext_left;
    private double hand_flex_ext_right;
    private double hand_radial_ulnar_left;
    private double hand_radial_ulnar_right;
    private double neck_flex_ext;
    private double neck_torsion;
    private double head_tilt;
    private double torso_tilt;
    private double torso_side_tilt;
    private double back_curve;
    private double back_torsion;
    private double knee_flex_ext_left;
    private double knee_flex_ext_right;
    private String timestamp; // Keep as String for initial parsing

    // Default constructor required by Flink
    public MoCapReading() {}

    // Constructor with all fields (optional, but useful)
    public MoCapReading(String thingid,
                         double elbow_flex_ext_left, double elbow_flex_ext_right,
                         double shoulder_flex_ext_left, double shoulder_flex_ext_right,
                         double shoulder_abd_add_left, double shoulder_abd_add_right,
                         double lowerarm_pron_sup_left, double lowerarm_pron_sup_right,
                         double upperarm_rotation_left, double upperarm_rotation_right,
                         double hand_flex_ext_left, double hand_flex_ext_right,
                         double hand_radial_ulnar_left, double hand_radial_ulnar_right,
                         double neck_flex_ext, double neck_torsion, double head_tilt,
                         double torso_tilt, double torso_side_tilt,
                         double back_curve, double back_torsion,
                         double knee_flex_ext_left, double knee_flex_ext_right,
                         String timestamp) {
        this.thingid = thingid;
        this.elbow_flex_ext_left = elbow_flex_ext_left;
        this.elbow_flex_ext_right = elbow_flex_ext_right;
        this.shoulder_flex_ext_left = shoulder_flex_ext_left;
        this.shoulder_flex_ext_right = shoulder_flex_ext_right;
        this.shoulder_abd_add_left = shoulder_abd_add_left;
        this.shoulder_abd_add_right = shoulder_abd_add_right;
        this.lowerarm_pron_sup_left = lowerarm_pron_sup_left;
        this.lowerarm_pron_sup_right = lowerarm_pron_sup_right;
        this.upperarm_rotation_left = upperarm_rotation_left;
        this.upperarm_rotation_right = upperarm_rotation_right;
        this.hand_flex_ext_left = hand_flex_ext_left;
        this.hand_flex_ext_right = hand_flex_ext_right;
        this.hand_radial_ulnar_left = hand_radial_ulnar_left;
        this.hand_radial_ulnar_right = hand_radial_ulnar_right;
        this.neck_flex_ext = neck_flex_ext;
        this.neck_torsion = neck_torsion;
        this.head_tilt = head_tilt;
        this.torso_tilt = torso_tilt;
        this.torso_side_tilt = torso_side_tilt;
        this.back_curve = back_curve;
        this.back_torsion = back_torsion;
        this.knee_flex_ext_left = knee_flex_ext_left;
        this.knee_flex_ext_right = knee_flex_ext_right;
        this.timestamp = timestamp;
        // Removed logger call from constructor to avoid excessive logging
    }

    // --- Getters ---
    public String getThingid() { return thingid; }
    public double getElbowFlexExtLeft() { return elbow_flex_ext_left; }
    public double getElbowFlexExtRight() { return elbow_flex_ext_right; }
    public double getShoulderFlexExtLeft() { return shoulder_flex_ext_left; }
    public double getShoulderFlexExtRight() { return shoulder_flex_ext_right; }
    public double getShoulderAbdAddLeft() { return shoulder_abd_add_left; }
    public double getShoulderAbdAddRight() { return shoulder_abd_add_right; }
    public double getLowerarmPronSupLeft() { return lowerarm_pron_sup_left; }
    public double getLowerarmPronSupRight() { return lowerarm_pron_sup_right; }
    public double getUpperarmRotationLeft() { return upperarm_rotation_left; }
    public double getUpperarmRotationRight() { return upperarm_rotation_right; }
    public double getHandFlexExtLeft() { return hand_flex_ext_left; }
    public double getHandFlexExtRight() { return hand_flex_ext_right; }
    public double getHandRadialUlnarLeft() { return hand_radial_ulnar_left; }
    public double getHandRadialUlnarRight() { return hand_radial_ulnar_right; }
    public double getNeckFlexExt() { return neck_flex_ext; }
    public double getNeckTorsion() { return neck_torsion; }
    public double getHeadTilt() { return head_tilt; }
    public double getTorsoTilt() { return torso_tilt; }
    public double getTorsoSideTilt() { return torso_side_tilt; }
    public double getBackCurve() { return back_curve; }
    public double getBackTorsion() { return back_torsion; }
    public double getKneeFlexExtLeft() { return knee_flex_ext_left; }
    public double getKneeFlexExtRight() { return knee_flex_ext_right; }
    public String getTimestamp() { return timestamp; }

    // --- Setters ---
    public void setThingid(String thingid) { this.thingid = thingid; }
    public void setElbowFlexExtLeft(double val) { this.elbow_flex_ext_left = val; }
    public void setElbowFlexExtRight(double val) { this.elbow_flex_ext_right = val; }
    public void setShoulderFlexExtLeft(double val) { this.shoulder_flex_ext_left = val; }
    public void setShoulderFlexExtRight(double val) { this.shoulder_flex_ext_right = val; }
    public void setShoulderAbdAddLeft(double val) { this.shoulder_abd_add_left = val; }
    public void setShoulderAbdAddRight(double val) { this.shoulder_abd_add_right = val; }
    public void setLowerarmPronSupLeft(double val) { this.lowerarm_pron_sup_left = val; }
    public void setLowerarmPronSupRight(double val) { this.lowerarm_pron_sup_right = val; }
    public void setUpperarmRotationLeft(double val) { this.upperarm_rotation_left = val; }
    public void setUpperarmRotationRight(double val) { this.upperarm_rotation_right = val; }
    public void setHandFlexExtLeft(double val) { this.hand_flex_ext_left = val; }
    public void setHandFlexExtRight(double val) { this.hand_flex_ext_right = val; }
    public void setHandRadialUlnarLeft(double val) { this.hand_radial_ulnar_left = val; }
    public void setHandRadialUlnarRight(double val) { this.hand_radial_ulnar_right = val; }
    public void setNeckFlexExt(double val) { this.neck_flex_ext = val; }
    public void setNeckTorsion(double val) { this.neck_torsion = val; }
    public void setHeadTilt(double val) { this.head_tilt = val; }
    public void setTorsoTilt(double val) { this.torso_tilt = val; }
    public void setTorsoSideTilt(double val) { this.torso_side_tilt = val; }
    public void setBackCurve(double val) { this.back_curve = val; }
    public void setBackTorsion(double val) { this.back_torsion = val; }
    public void setKneeFlexExtLeft(double val) { this.knee_flex_ext_left = val; }
    public void setKneeFlexExtRight(double val) { this.knee_flex_ext_right = val; }
    public void setTimestamp(String timestamp) { this.timestamp = timestamp; }

    // toString (optional, for debugging)
    @Override
    public String toString() {
        // (Keep the existing toString method from SensorReading.java)
        return "MoCapReading{" +
               "thingid='" + thingid + '\'' +
               ", elbow_flex_ext_left=" + elbow_flex_ext_left +
               // ... include all other fields ...
               ", timestamp=" + timestamp +
               '}';
    }
}