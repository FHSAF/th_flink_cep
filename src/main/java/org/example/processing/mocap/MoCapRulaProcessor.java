package org.example.processing.mocap;

import org.apache.flink.api.common.functions.MapFunction;
import org.example.config.ProcessingParamsConfig;
import org.example.models.MoCapReading;
import org.example.models.RulaScore; // Ensure this model exists
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class MoCapRulaProcessor {

    private static final Logger logger = LoggerFactory.getLogger(MoCapRulaProcessor.class);

    // --- RULA Scoring Tables (derived from McAtamney & Corlett, 1993) ---

    // Table A: Posture Score for Arm & Wrist
    // Dimensions: [Upper Arm Score-1][Lower Arm Score-1][Wrist Score-1][Wrist Twist Score-1]
    private static final int[][][][] RULA_TABLE_A = {
        // Upper Arm Score = 1
        {
            // Lower Arm Score = 1
            {
                // Wrist Score = 1
                {1, 2}, // Wrist Twist 1, 2
                // Wrist Score = 2
                {2, 2},
                // Wrist Score = 3
                {2, 3},
                // Wrist Score = 4
                {3, 3}
            },
            // Lower Arm Score = 2
            {
                // Wrist Score = 1
                {2, 3},
                // Wrist Score = 2
                {2, 3},
                // Wrist Score = 3
                {3, 3},
                // Wrist Score = 4
                {3, 4}
            },
            // Lower Arm Score = 3
            {
                // Wrist Score = 1
                {3, 3}, // Adjusted based on typical RULA charts (original paper implies fewer LA categories directly in table)
                // Wrist Score = 2
                {3, 3},
                // Wrist Score = 3
                {3, 4},
                // Wrist Score = 4
                {4, 4}
            }
        },
        // Upper Arm Score = 2
        {
            // Lower Arm Score = 1
            {
                {2, 3}, {3, 3}, {3, 4}, {4, 4}
            },
            // Lower Arm Score = 2
            {
                {3, 3}, {3, 4}, {4, 4}, {4, 4}
            },
            // Lower Arm Score = 3
            {
                {3, 4}, {4, 4}, {4, 4}, {5, 5}
            }
        },
        // Upper Arm Score = 3
        {
            // Lower Arm Score = 1
            {
                {3, 4}, {4, 4}, {4, 4}, {5, 5}
            },
            // Lower Arm Score = 2
            {
                {4, 4}, {4, 4}, {4, 5}, {5, 5}
            },
            // Lower Arm Score = 3
            {
                {4, 4}, {4, 5}, {5, 5}, {5, 5} // Adjusted
            }
        },
        // Upper Arm Score = 4
        {
            // Lower Arm Score = 1
            {
                {4, 4}, {4, 5}, {5, 5}, {5, 6}
            },
            // Lower Arm Score = 2
            {
                {4, 5}, {5, 5}, {5, 6}, {6, 6}
            },
            // Lower Arm Score = 3
            {
                {5, 5}, {5, 6}, {6, 6}, {6, 7}
            }
        },
        // Upper Arm Score = 5
        {
            // Lower Arm Score = 1
            {
                {5, 5}, {5, 6}, {6, 7}, {7, 7}
            },
            // Lower Arm Score = 2
            {
                {5, 6}, {6, 6}, {6, 7}, {7, 7}
            },
            // Lower Arm Score = 3
            {
                {6, 6}, {6, 7}, {7, 7}, {7, 8}
            }
        },
        // Upper Arm Score = 6
        {
            // Lower Arm Score = 1
            {
                {7, 7}, {7, 7}, {7, 8}, {8, 9}
            },
            // Lower Arm Score = 2
            {
                {8, 8}, {8, 8}, {8, 9}, {9, 9}
            },
            // Lower Arm Score = 3
            {
                {9, 9}, {9, 9}, {9, 9}, {9, 9}
            }
        }
    };

    // Table B: Posture Score for Neck, Trunk & Legs
    // Dimensions: [Neck Score-1][Trunk Score-1][Leg Score-1]
    private static final int[][][] RULA_TABLE_B = {
        // Neck Score 1
        {{1, 3}, {2, 3}, {3, 4}, {5, 5}, {6, 6}, {7, 7}}, // Trunk 1-6, Leg 1&2
        // Neck Score 2
        {{2, 3}, {2, 3}, {4, 5}, {5, 5}, {6, 7}, {7, 7}},
        // Neck Score 3
        {{3, 3}, {3, 4}, {4, 5}, {5, 6}, {6, 7}, {7, 7}},
        // Neck Score 4
        {{5, 5}, {5, 6}, {6, 7}, {7, 7}, {7, 7}, {8, 8}},
        // Neck Score 5
        {{7, 7}, {7, 7}, {7, 8}, {8, 8}, {8, 8}, {8, 8}},
        // Neck Score 6
        {{8, 8}, {8, 8}, {8, 8}, {8, 9}, {9, 9}, {9, 9}}
    };


    // Table C: Grand Score from Score C (Wrist & Arm) and Score D (Neck, Trunk & Leg)
    // Dimensions: [Score C-1][Score D-1] (Scores C & D can range from 1 up to potentially 9+ for Score C and 7+ for Score D before capping for table lookup)
    // Table C in paper goes up to Score C=8, Score D=7+
    private static final int[][] RULA_TABLE_C = {
        // Score C=1       Score D -> 1, 2, 3, 4, 5, 6, 7+
        {1, 2, 3, 3, 4, 5, 5},
        // Score C=2
        {2, 2, 3, 4, 4, 5, 5},
        // Score C=3
        {3, 3, 3, 4, 4, 5, 6},
        // Score C=4
        {3, 3, 3, 4, 5, 6, 6},
        // Score C=5
        {4, 4, 4, 5, 6, 7, 7},
        // Score C=6
        {4, 4, 5, 6, 6, 7, 7},
        // Score C=7
        {5, 5, 6, 6, 7, 7, 7},
        // Score C=8+
        {5, 5, 6, 7, 7, 7, 7}
    };


    private RulaScore calculateRulaScoreInternal(MoCapReading s, double loadKg, int muscleUseScoreConfigAW, int muscleUseScoreConfigNTL) {
        if (s == null) return null;

        RulaScore score = new RulaScore();
        score.thingId = s.getThingid();
        score.timestamp = s.getTimestamp();
        Map<String, Object> details = score.calculationDetails;

        // Determine which arm to use based on configuration
        ProcessingParamsConfig.MonitoredArm monitoredArm = ProcessingParamsConfig.MOCAP_MONITORED_ARM;
        details.put("monitored_arm", monitoredArm.toString());

        // === SECTION A: ARM & WRIST ANALYSIS ===
        score.upperArmScore = calculateUpperArmScore(s, monitoredArm, details);
        score.lowerArmScore = calculateLowerArmScore(s, monitoredArm, details);
        score.wristScore = calculateWristScore(s, monitoredArm, details);
        score.wristTwistScore = calculateWristTwistScore(s, monitoredArm, details);

        score.postureScoreA = lookupRulaTableA(score.upperArmScore, score.lowerArmScore, score.wristScore, score.wristTwistScore);
        details.put("posture_score_A_lookup", score.postureScoreA);

        score.muscleUseScoreAW = calculateMuscleUseScore(muscleUseScoreConfigAW, details, "AW"); // Use configured value
        score.forceLoadScoreAW = calculateForceLoadScore(loadKg, details, "AW");

        score.wristAndArmScore = score.postureScoreA + score.muscleUseScoreAW + score.forceLoadScoreAW;
        details.put("wrist_and_arm_score_C", score.wristAndArmScore);

        // === SECTION B: NECK, TRUNK & LEG ANALYSIS ===
        score.neckScore = calculateNeckPostureScore(s, details);
        score.trunkScore = calculateTrunkPostureScore(s, details);
        score.legScore = calculateLegPostureScore(s, details);

        score.postureScoreB = lookupRulaTableB(score.neckScore, score.trunkScore, score.legScore);
        details.put("posture_score_B_lookup", score.postureScoreB);

        score.muscleUseScoreNTL = calculateMuscleUseScore(muscleUseScoreConfigNTL, details, "NTL"); // Use configured value
        score.forceLoadScoreNTL = calculateForceLoadScore(loadKg, details, "NTL");

        score.neckTrunkLegScore = score.postureScoreB + score.muscleUseScoreNTL + score.forceLoadScoreNTL;
        details.put("neck_trunk_leg_score_D", score.neckTrunkLegScore);

        // === FINAL SCORE ===
        score.finalRulaScore = lookupRulaTableC(score.wristAndArmScore, score.neckTrunkLegScore);
        details.put("final_rula_score_lookup", score.finalRulaScore);

        score.riskLevel = getRulaRiskLabel(score.finalRulaScore);

        return score;
    }

    // --- RULA Scoring Functions based on McAtamney & Corlett, 1993 ---

    private int calculateUpperArmScore(MoCapReading s, ProcessingParamsConfig.MonitoredArm arm, Map<String, Object> details) {
        double flexExt = (arm == ProcessingParamsConfig.MonitoredArm.RIGHT) ? s.getShoulderFlexExtRight() : s.getShoulderFlexExtLeft();
        double abdAdd = (arm == ProcessingParamsConfig.MonitoredArm.RIGHT) ? s.getShoulderAbdAddRight() : s.getShoulderAbdAddLeft();
        // RULA does not explicitly use upper arm rotation from MoCapReading for the base score,
        // but it's a component of overall arm posture often considered in detailed ergonomic analyses.
        // For RULA, shoulder elevation and abduction are adjustments.

        int score;
        if (flexExt >= -20 && flexExt <= 20) score = 1;
        else if (flexExt < -20 || (flexExt > 20 && flexExt <= 45)) score = 2;
        else if (flexExt > 45 && flexExt <= 90) score = 3;
        else if (flexExt > 90) score = 4;
        else score = 1; // Default / Should not happen if angles are valid

        details.put("upper_arm_flex_ext_degrees_" + arm, flexExt);
        details.put("upper_arm_base_score", score);

        // Adjustments
        // Assuming MoCap provides direct abduction angle. RULA: "Add 1 if upper arm is abducted"
        if (Math.abs(abdAdd) > 5) { // Assuming > 5 deg is "abducted" for RULA adjustment purposes
            score += 1;
            details.put("upper_arm_abducted_adj", 1);
        }
        // Shoulder raised: This might need specific sensor data or visual assessment. For MoCap, it's hard to infer without specific markers. Assuming not raised for now.
        // details.put("upper_arm_shoulder_raised_adj", 0);
        // Arm supported/leaning: Assuming not supported for general case from MoCap.
        // details.put("upper_arm_supported_adj", 0);

        return Math.max(1, Math.min(score, 6)); // RULA Upper Arm score typically caps at 6 after adjustments
    }

    private int calculateLowerArmScore(MoCapReading s, ProcessingParamsConfig.MonitoredArm arm, Map<String, Object> details) {
        double elbowFlex = (arm == ProcessingParamsConfig.MonitoredArm.RIGHT) ? s.getElbowFlexExtRight() : s.getElbowFlexExtLeft();
        int score;

        if (elbowFlex >= 60 && elbowFlex <= 100) score = 1;
        else score = 2;

        details.put("lower_arm_flex_degrees_" + arm, elbowFlex);
        details.put("lower_arm_base_score", score);

        // Adjustment: If working across midline or out to side
        // This is difficult to determine purely from isolated joint angles without knowing the body's reference frame
        // and task context. For a simplified MoCap implementation, this might be omitted or require more complex logic.
        // Assuming no adjustment for now.
        // details.put("lower_arm_midline_adj", 0);

        return Math.max(1, Math.min(score, 3)); // RULA Lower Arm score typically caps
    }

    private int calculateWristScore(MoCapReading s, ProcessingParamsConfig.MonitoredArm arm, Map<String, Object> details) {
        double wristFlexExt = (arm == ProcessingParamsConfig.MonitoredArm.RIGHT) ? s.getHandFlexExtRight() : s.getHandFlexExtLeft();
        double wristDev = (arm == ProcessingParamsConfig.MonitoredArm.RIGHT) ? s.getHandRadialUlnarRight() : s.getHandRadialUlnarLeft();
        int score;

        if (Math.abs(wristFlexExt) == 0) score = 1; // Neutral
        else if (Math.abs(wristFlexExt) > 0 && Math.abs(wristFlexExt) <= 15) score = 2;
        else score = 3; // > 15 degrees

        details.put("wrist_flex_ext_degrees_" + arm, wristFlexExt);
        details.put("wrist_base_score", score);

        // Adjustment: if wrist is bent from midline (ulnar/radial deviation)
        if (Math.abs(wristDev) > 0) { // Original RULA just says "if wrist is bent away from midline" -> any deviation.
            score += 1;
            details.put("wrist_deviation_adj", 1);
            details.put("wrist_deviation_degrees_" + arm, wristDev);
        } else {
            details.put("wrist_deviation_adj", 0);
        }
        return Math.max(1, Math.min(score, 4)); // RULA Wrist score typically caps
    }

    private int calculateWristTwistScore(MoCapReading s, ProcessingParamsConfig.MonitoredArm arm, Map<String, Object> details) {
        // Pronation/Supination. MoCapReading has `lowerarm_pron_sup_left/right`.
        // Score 1: Mainly in mid-range of twist.
        // Score 2: At or near the end of twisting range.
        double twist = (arm == ProcessingParamsConfig.MonitoredArm.RIGHT) ? s.getLowerarmPronSupRight() : s.getLowerarmPronSupLeft();
        int score;

        // Defining "mid-range" vs "end of range" from raw angle data can be tricky.
        // Let's assume neutral is 0, full pronation ~ -90, full supination ~ +90.
        // "Mid-range" might be +/- 45 degrees from neutral.
        if (Math.abs(twist) <= 45) score = 1; // Example: mid-range
        else score = 2; // Near or at end of range

        details.put("wrist_twist_degrees_" + arm, twist);
        details.put("wrist_twist_score", score);
        return score; // Score is 1 or 2
    }

    private int calculateNeckPostureScore(MoCapReading s, Map<String, Object> details) {
        double neckFlex = s.getNeckFlexExt();
        double neckTwist = s.getNeckTorsion();
        double neckSideBend = s.getHeadTilt(); // Assuming HeadTilt represents neck side bending for RULA
        int score;

        if (neckFlex >= 0 && neckFlex <= 10) score = 1;
        else if (neckFlex > 10 && neckFlex <= 20) score = 2;
        else if (neckFlex > 20) score = 3;
        else score = 4; // In extension (neckFlex < 0)

        details.put("neck_flex_ext_degrees", neckFlex);
        details.put("neck_base_score", score);

        if (Math.abs(neckTwist) > 5) { // Assuming > 5 deg is "twisted"
            score += 1;
            details.put("neck_twisted_adj", 1);
        }
        if (Math.abs(neckSideBend) > 5) { // Assuming > 5 deg is "side-bent"
            score += 1;
            details.put("neck_side_bend_adj", 1);
        }
        return Math.max(1, Math.min(score, 6)); // RULA Neck score caps
    }

    private int calculateTrunkPostureScore(MoCapReading s, Map<String, Object> details) {
        double trunkFlex = s.getTorsoTilt(); // Assuming TorsoTilt is forward flexion
        double trunkTwist = s.getBackTorsion();
        double trunkSideBend = s.getTorsoSideTilt();
        int score;

        if (trunkFlex == 0) score = 1; // Upright and well supported (assuming standing, RULA gives 2 if sitting unless well supported)
                                      // The original paper: Score 1: "when sitting and well supported with a hip-trunk angle of 90 deg or more"
                                      // or "if standing with body weight evenly distributed"
                                      // For a general MoCap system, assuming standing, 0 degrees flexion is score 1.
                                      // If sitting, it's typically a 2 unless further info. We'll use standing interpretation.
        else if (trunkFlex > 0 && trunkFlex <= 20) score = 2;
        else if (trunkFlex > 20 && trunkFlex <= 60) score = 3;
        else if (trunkFlex > 60) score = 4;
        else score = 2; // For any extension (trunkFlex < 0), RULA usually scores it as 2 (like 0-20 flexion)

        // Adjustment from original RULA for supported seated trunk: "I also if trunk is well supported while seated"
        // This is not applicable here as we assume standing from MoCap or it's hard to tell.

        details.put("trunk_flex_ext_degrees", trunkFlex);
        details.put("trunk_base_score", score);

        if (Math.abs(trunkTwist) > 5) { // Assuming > 5 deg is "twisted"
            score += 1;
            details.put("trunk_twisted_adj", 1);
        }
        if (Math.abs(trunkSideBend) > 5) { // Assuming > 5 deg is "side-bent"
            score += 1;
            details.put("trunk_side_bend_adj", 1);
        }
        return Math.max(1, Math.min(score, 6)); // RULA Trunk score caps
    }

    private int calculateLegPostureScore(MoCapReading s, Map<String, Object> details) {
        // Score 1: Legs and feet well supported, weight evenly balanced (standing or seated).
        // Score 2: If not.
        // From MoCap alone, it's hard to judge "well supported" or "evenly balanced" in detail.
        // A common simplification for standing tasks is to check if knees are roughly straight.
        // RULA's leg score is simple. If standing and stable, often a 1. If unstable or one leg supporting, a 2.
        // Let's assume if both knees are reasonably extended, score is 1. Otherwise, 2.
        // This part might need refinement based on available MoCap data or assumptions.
        // For simplicity, as per original RULA for general standing:
        details.put("leg_posture_score", 1); // Assuming balanced standing as a default
        return 1; // Or 2 if there's reason to believe unstable/unsupported from MoCap (e.g. significant knee flexion diff)
    }

    private int calculateMuscleUseScore(int configuredMuscleScore, Map<String, Object> details, String section) {
        // RULA: Score +1 if posture is mainly static (held > 1 min) OR repeated > 4 times/min.
        // This information (duration, repetition frequency) is not directly in a single MoCapReading.
        // This would require analyzing a stream of MoCapReadings over time.
        // For an instantaneous RULA score from a single MoCapReading, we rely on the configured input.
        details.put("muscle_use_score_input_" + section, configuredMuscleScore);
        return configuredMuscleScore; // 0 or 1 based on external configuration/assessment
    }

    private int calculateForceLoadScore(double loadKg, Map<String, Object> details, String section) {
        // RULA Force/Load Score:
        // 0: < 2kg, intermittent
        // 1: 2kg to 10kg, intermittent
        // 2: 2kg to 10kg, static or repeated OR >10kg, intermittent
        // 3: >10kg, static or repeated OR shock/rapid build up
        // This also requires knowing if the load is intermittent, static, or repeated, which isn't in a single MoCapReading.
        // We'll base it on loadKg from config and assume "intermittent" as a default if type of exertion isn't specified.
        int score;
        if (loadKg < 2) {
            score = 0;
        } else if (loadKg <= 10) {
            score = 1; // Assuming intermittent by default for this load range
            // If known to be static/repeated, this would be 2.
        } else { // > 10kg
            score = 2; // Assuming intermittent by default for this load range
            // If known to be static/repeated, this would be 3.
        }
        // Shock/rapid build-up would make it 3, but hard to tell from static MoCap.

        details.put("force_load_kg_" + section, loadKg);
        details.put("force_load_score_base_" + section, score); // This is a simplification.
        return score;
    }

    private int lookupRulaTableA(int uArm, int lArm, int wrist, int twist) {
        // Adjust scores to be 0-indexed for array access, ensuring they are within bounds
        int ua = Math.max(0, Math.min(uArm - 1, RULA_TABLE_A.length - 1));
        int la = Math.max(0, Math.min(lArm - 1, RULA_TABLE_A[0].length - 1));
        int wr = Math.max(0, Math.min(wrist - 1, RULA_TABLE_A[0][0].length - 1));
        int wt = Math.max(0, Math.min(twist - 1, RULA_TABLE_A[0][0][0].length - 1));
        return RULA_TABLE_A[ua][la][wr][wt];
    }

    private int lookupRulaTableB(int neck, int trunk, int legs) {
        int n = Math.max(0, Math.min(neck - 1, RULA_TABLE_B.length - 1));
        int t = Math.max(0, Math.min(trunk - 1, RULA_TABLE_B[0].length - 1));
        int l = Math.max(0, Math.min(legs - 1, RULA_TABLE_B[0][0].length - 1));
        return RULA_TABLE_B[n][t][l];
    }

    private int lookupRulaTableC(int scoreC, int scoreD) {
        int c = Math.max(0, Math.min(scoreC - 1, RULA_TABLE_C.length - 1));
        int d = Math.max(0, Math.min(scoreD - 1, RULA_TABLE_C[0].length - 1));
        return RULA_TABLE_C[c][d];
    }

    private String getRulaRiskLabel(int finalRulaScore) {
        if (finalRulaScore <= 2) return "Acceptable posture.";
        if (finalRulaScore <= 4) return "Further investigation needed; changes may be required.";
        if (finalRulaScore <= 6) return "Investigation and changes required soon.";
        return "Investigation and changes required immediately."; // Score 7+
    }

    public static class RulaScoreMapFunction implements MapFunction<MoCapReading, String> {
        private transient MoCapRulaProcessor calculator;
        private transient Gson gson;
        private final double loadKg;
        private final int muscleUseScoreAW;
        private final int muscleUseScoreNTL;

        public RulaScoreMapFunction(double loadKg, int muscleUseScoreAW, int muscleUseScoreNTL) {
            this.loadKg = loadKg;
            this.muscleUseScoreAW = muscleUseScoreAW;
            this.muscleUseScoreNTL = muscleUseScoreNTL;
        }

        private void ensureInitialized() {
            if (calculator == null) { calculator = new MoCapRulaProcessor(); }
            if (gson == null) { gson = new GsonBuilder().setPrettyPrinting().create(); } // .setPrettyPrinting() is optional
        }

        @Override
        public String map(MoCapReading moCapReading) throws Exception {
            ensureInitialized();
            if (moCapReading == null) {
                logger.warn("RulaScoreMapFunction received null MoCapReading.");
                return null;
            }
            try {
                RulaScore rulaScore = calculator.calculateRulaScoreInternal(moCapReading, this.loadKg, this.muscleUseScoreAW, this.muscleUseScoreNTL);
                if (rulaScore != null) {
                    return gson.toJson(rulaScore);
                } else {
                    return null;
                }
            } catch (Exception e) {
                logger.error("Error mapping MoCapReading to RULA score for thingId {}: {}", moCapReading.getThingid(), e.getMessage(), e);
                return null; // Or throw, depending on desired error handling
            }
        }
    }
}