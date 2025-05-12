package org.example.models;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class RulaScore implements Serializable {
    private static final long serialVersionUID = 201L; // New unique ID

    public String thingId;
    public String timestamp; // Original timestamp string from MoCapReading

    // Component Scores - Section A: Arm & Wrist
    public int upperArmScore;
    public int lowerArmScore;
    public int wristScore;
    public int wristTwistScore;
    public int postureScoreA; // From Table A
    public int muscleUseScoreAW; // Arm & Wrist
    public int forceLoadScoreAW; // Arm & Wrist
    public int wristAndArmScore; // Final for Section A

    // Component Scores - Section B: Neck, Trunk, Legs
    public int neckScore;
    public int trunkScore;
    public int legScore;
    public int postureScoreB; // From Table B
    public int muscleUseScoreNTL; // Neck, Trunk, Legs
    public int forceLoadScoreNTL; // Neck, Trunk, Legs
    public int neckTrunkLegScore; // Final for Section B

    // Final RULA Score
    public int finalRulaScore; // From Table C
    public String riskLevel;

    // Optional: Details for debugging/analysis
    public Map<String, Object> calculationDetails = new HashMap<>();

    public RulaScore() {}

    // Getters and Setters can be added if needed for specific frameworks,
    // but Flink typically works with public fields or discovers getters/setters by convention.

    @Override
    public String toString() {
        return "RulaScore{" +
                "thingId='" + thingId + '\'' +
                ", timestamp='" + timestamp + '\'' +
                ", upperArmScore=" + upperArmScore +
                ", lowerArmScore=" + lowerArmScore +
                ", wristScore=" + wristScore +
                ", wristTwistScore=" + wristTwistScore +
                ", postureScoreA=" + postureScoreA +
                ", muscleUseScoreAW=" + muscleUseScoreAW +
                ", forceLoadScoreAW=" + forceLoadScoreAW +
                ", wristAndArmScore=" + wristAndArmScore +
                ", neckScore=" + neckScore +
                ", trunkScore=" + trunkScore +
                ", legScore=" + legScore +
                ", postureScoreB=" + postureScoreB +
                ", muscleUseScoreNTL=" + muscleUseScoreNTL +
                ", forceLoadScoreNTL=" + forceLoadScoreNTL +
                ", neckTrunkLegScore=" + neckTrunkLegScore +
                ", finalRulaScore=" + finalRulaScore +
                ", riskLevel='" + riskLevel + '\'' +
                ", calculationDetails=" + calculationDetails +
                '}';
    }
}