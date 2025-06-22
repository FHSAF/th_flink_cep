package org.example.processing.mocap;

import org.apache.flink.api.common.functions.MapFunction;
import org.example.config.ProcessingParamsConfig;
import org.example.models.MoCapReading;
import org.example.models.RulaScore;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Implements the RULA (Rapid Upper Limb Assessment) method based on the original paper
 * by McAtamney & Corlett (1993), Applied Ergonomics 24(2), 91-99.
 * This processor calculates a RULA score from an instantaneous MoCapReading,
 * making necessary assumptions where temporal information (like duration or repetition) is required.
 */
public class MoCapRulaProcessor {

    private static final Logger logger = LoggerFactory.getLogger(MoCapRulaProcessor.class);

    // --- RULA Scoring Tables (derived from McAtamney & Corlett, 1993) ---

    // Table A: Posture Score for Arm & Wrist
    // Source: McAtamney & Corlett, 1993, Table 1
    // Dimensions: [Upper Arm Score-1][Lower Arm Score-1][Wrist Score-1][Wrist Twist Score-1]
    private static final int[][][][] RULA_TABLE_A = {
        // Upper Arm Score = 1
        {{{1, 2}, {2, 2}, {2, 3}, {3, 3}}, {{2, 3}, {2, 3}, {3, 3}, {3, 4}}, {{3, 3}, {3, 3}, {3, 4}, {4, 4}}},
        // Upper Arm Score = 2
        {{{2, 3}, {3, 3}, {3, 4}, {4, 4}}, {{3, 3}, {3, 4}, {4, 4}, {4, 4}}, {{3, 4}, {4, 4}, {4, 4}, {5, 5}}},
        // Upper Arm Score = 3
        {{{3, 4}, {4, 4}, {4, 4}, {5, 5}}, {{4, 4}, {4, 4}, {4, 5}, {5, 5}}, {{4, 4}, {4, 5}, {5, 5}, {5, 5}}},
        // Upper Arm Score = 4
        {{{4, 4}, {4, 5}, {5, 5}, {5, 6}}, {{4, 5}, {5, 5}, {5, 6}, {6, 6}}, {{5, 5}, {5, 6}, {6, 6}, {6, 7}}},
        // Upper Arm Score = 5
        {{{5, 5}, {5, 6}, {6, 7}, {7, 7}}, {{5, 6}, {6, 6}, {6, 7}, {7, 7}}, {{6, 6}, {6, 7}, {7, 7}, {7, 8}}},
        // Upper Arm Score = 6
        {{{7, 7}, {7, 7}, {7, 8}, {8, 9}}, {{8, 8}, {8, 8}, {8, 9}, {9, 9}}, {{9, 9}, {9, 9}, {9, 9}, {9, 9}}}
    };

    // Table B: Posture Score for Neck, Trunk & Legs
    // Source: McAtamney & Corlett, 1993, Table 2
    // Dimensions: [Neck Score-1][Trunk Score-1][Leg Score-1]
    private static final int[][][] RULA_TABLE_B = {
        // Neck Score 1
        {{1, 3}, {2, 3}, {3, 4}, {5, 5}, {6, 6}, {7, 7}},
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


    // Table C: Grand Score Table
    // Source: McAtamney & Corlett, 1993, Figure 6
    // Dimensions: [Score C-1][Score D-1]
    private static final int[][] RULA_TABLE_C = {
        // Score D -> 1, 2, 3, 4, 5, 6, 7+
        {1, 2, 3, 3, 4, 5, 5}, // Score C=1
        {2, 2, 3, 4, 4, 5, 5}, // Score C=2
        {3, 3, 3, 4, 4, 5, 6}, // Score C=3
        {3, 3, 3, 4, 5, 6, 6}, // Score C=4
        {4, 4, 4, 5, 6, 7, 7}, // Score C=5
        {4, 4, 5, 6, 6, 7, 7}, // Score C=6
        {5, 5, 6, 6, 7, 7, 7}, // Score C=7
        {5, 5, 6, 7, 7, 7, 7}  // Score C=8+
    };


    private RulaScore calculateRulaScoreInternal(MoCapReading s, double loadKg, int muscleUseScoreConfig, boolean isLoadStaticOrRepeated) {
        if (s == null) return null;

        RulaScore score = new RulaScore();
        score.thingId = s.getThingid();
        score.timestamp = s.getTimestamp();
        Map<String, Object> details = score.calculationDetails;

        ProcessingParamsConfig.MonitoredArm monitoredArm = ProcessingParamsConfig.MOCAP_MONITORED_ARM;
        details.put("monitored_arm", monitoredArm.toString());

        // === SECTION A: ARM & WRIST ANALYSIS ===
        score.upperArmScore = calculateUpperArmScore(s, monitoredArm, details);
        score.lowerArmScore = calculateLowerArmScore(s, monitoredArm, details);
        score.wristScore = calculateWristScore(s, monitoredArm, details);
        score.wristTwistScore = calculateWristTwistScore(s, monitoredArm, details);

        score.postureScoreA = lookupRulaTableA(score.upperArmScore, score.lowerArmScore, score.wristScore, score.wristTwistScore);
        details.put("posture_score_A_lookup", score.postureScoreA);

        score.muscleUseScoreAW = calculateMuscleUseScore(muscleUseScoreConfig, details, "AW");
        score.forceLoadScoreAW = calculateForceLoadScore(loadKg, isLoadStaticOrRepeated, details, "AW");

        score.wristAndArmScore = score.postureScoreA + score.muscleUseScoreAW + score.forceLoadScoreAW;
        details.put("wrist_and_arm_score_C", score.wristAndArmScore);

        // === SECTION B: NECK, TRUNK & LEG ANALYSIS ===
        score.neckScore = calculateNeckPostureScore(s, details);
        score.trunkScore = calculateTrunkPostureScore(s, details);
        score.legScore = calculateLegPostureScore(s, details);

        score.postureScoreB = lookupRulaTableB(score.neckScore, score.trunkScore, score.legScore);
        details.put("posture_score_B_lookup", score.postureScoreB);

        score.muscleUseScoreNTL = calculateMuscleUseScore(muscleUseScoreConfig, details, "NTL");
        score.forceLoadScoreNTL = calculateForceLoadScore(loadKg, isLoadStaticOrRepeated, details, "NTL");

        score.neckTrunkLegScore = score.postureScoreB + score.muscleUseScoreNTL + score.forceLoadScoreNTL;
        details.put("neck_trunk_leg_score_D", score.neckTrunkLegScore);

        // === FINAL SCORE ===
        score.finalRulaScore = lookupRulaTableC(score.wristAndArmScore, score.neckTrunkLegScore);
        details.put("final_rula_score_lookup", score.finalRulaScore);
        score.riskLevel = getRulaRiskLabel(score.finalRulaScore);

        return score;
    }

    private int calculateUpperArmScore(MoCapReading s, ProcessingParamsConfig.MonitoredArm arm, Map<String, Object> details) {
        double flexExt = (arm == ProcessingParamsConfig.MonitoredArm.RIGHT) ? s.getShoulderFlexExtRight() : s.getShoulderFlexExtLeft();
        double abdAdd = (arm == ProcessingParamsConfig.MonitoredArm.RIGHT) ? s.getShoulderAbdAddRight() : s.getShoulderAbdAddLeft();
        int score;

        if (flexExt >= 0 && flexExt <= 20) score = 1;
        else if (flexExt > 20 && flexExt <= 45) score = 2;
        else if (flexExt < 0 && flexExt >= -20) score = 2; // Extension 0-20 degrees
        else if (flexExt < -20) score = 2; // Extension > 20 degrees
        else if (flexExt > 45 && flexExt <= 90) score = 3;
        else score = 4; // > 90 flexion

        details.put("upper_arm_flex_ext_degrees", flexExt);
        details.put("upper_arm_base_score", score);

        // Adjustments from McAtamney & Corlett, 1993
        if (Math.abs(abdAdd) > 5) { // Add 1 if upper arm is abducted
            score += 1;
            details.put("upper_arm_abducted_adj", 1);
        }
        // Shoulder raised: Add 1. Cannot be determined from MoCap, assumed 0.
        // Arm supported or leaning: Subtract 1. Cannot be determined, assumed 0.

        return score;
    }

    private int calculateLowerArmScore(MoCapReading s, ProcessingParamsConfig.MonitoredArm arm, Map<String, Object> details) {
        double elbowFlex = (arm == ProcessingParamsConfig.MonitoredArm.RIGHT) ? s.getElbowFlexExtRight() : s.getElbowFlexExtLeft();
        int score;

        if (elbowFlex >= 60 && elbowFlex <= 100) score = 1;
        else score = 2; // For < 60 or > 100 degrees

        details.put("lower_arm_flex_degrees", elbowFlex);
        details.put("lower_arm_base_score", score);

        // Adjustment: Add 1 if working across midline or out to side.
        // Cannot be determined from isolated MoCap, assumed 0.
        return score;
    }

    private int calculateWristScore(MoCapReading s, ProcessingParamsConfig.MonitoredArm arm, Map<String, Object> details) {
        double wristFlexExt = (arm == ProcessingParamsConfig.MonitoredArm.RIGHT) ? s.getHandFlexExtRight() : s.getHandFlexExtLeft();
        double wristDev = (arm == ProcessingParamsConfig.MonitoredArm.RIGHT) ? s.getHandRadialUlnarRight() : s.getHandRadialUlnarLeft();
        int score;

        if (Math.abs(wristFlexExt) < 1) score = 1; // Neutral
        else if (Math.abs(wristFlexExt) <= 15) score = 2;
        else score = 3; // > 15 degrees

        details.put("wrist_flex_ext_degrees", wristFlexExt);
        details.put("wrist_base_score", score);

        // Adjustment: Add 1 if wrist is in ulnar or radial deviation
        if (Math.abs(wristDev) > 5) { // Assuming >5 degrees is significant deviation
            score += 1;
            details.put("wrist_deviation_adj", 1);
            details.put("wrist_deviation_degrees", wristDev);
        }
        return score;
    }

    private int calculateWristTwistScore(MoCapReading s, ProcessingParamsConfig.MonitoredArm arm, Map<String, Object> details) {
        double twist = (arm == ProcessingParamsConfig.MonitoredArm.RIGHT) ? s.getLowerarmPronSupRight() : s.getLowerarmPronSupLeft();
        int score = (Math.abs(twist) >= 45) ? 2 : 1; // 1 if in mid-range, 2 if at/near end of range
        details.put("wrist_twist_degrees", twist);
        details.put("wrist_twist_score", score);
        return score;
    }

    private int calculateNeckPostureScore(MoCapReading s, Map<String, Object> details) {
        double neckFlex = s.getNeckFlexExt();
        double neckTwist = s.getNeckTorsion();
        double neckSideBend = s.getHeadTilt();
        int score;

        if (neckFlex >= 0 && neckFlex <= 10) score = 1;
        else if (neckFlex > 10 && neckFlex <= 20) score = 2;
        else if (neckFlex > 20) score = 3;
        else score = 4; // In extension

        details.put("neck_flex_ext_degrees", neckFlex);
        details.put("neck_base_score", score);

        if (Math.abs(neckTwist) > 5) { score += 1; details.put("neck_twisted_adj", 1); }
        if (Math.abs(neckSideBend) > 5) { score += 1; details.put("neck_side_bend_adj", 1); }

        return score;
    }

    private int calculateTrunkPostureScore(MoCapReading s, Map<String, Object> details) {
        double trunkFlex = s.getTorsoTilt();
        double trunkTwist = s.getBackTorsion();
        double trunkSideBend = s.getTorsoSideTilt();
        int score;

        // Per RULA paper, Score 1 is for sitting & well-supported. Standing upright is Score 2.
        if (trunkFlex >= 0 && trunkFlex <= 20) score = 2;
        else if (trunkFlex > 20 && trunkFlex <= 60) score = 3;
        else if (trunkFlex > 60) score = 4;
        else score = 2;

        details.put("trunk_flex_ext_degrees", trunkFlex);
        details.put("trunk_base_score", score);

        if (Math.abs(trunkTwist) > 5) { score += 1; details.put("trunk_twisted_adj", 1); }
        if (Math.abs(trunkSideBend) > 5) { score += 1; details.put("trunk_side_bend_adj", 1); }
        return score;
    }

    private int calculateLegPostureScore(MoCapReading s, Map<String, Object> details) {
        // Score 1: Legs supported and balanced. Score 2: Not.
        // This is a simplification; MoCap cannot easily tell if feet are supported.
        // Assuming balanced standing posture as a default.
        details.put("leg_posture_score", 1);
        return 1;
    }

    private int calculateMuscleUseScore(int configuredMuscleScore, Map<String, Object> details, String section) {
        // RULA: Score +1 if posture is mainly static (held > 1 min) OR repeated > 4 times/min.
        // This requires temporal analysis, so we rely on a configured value.
        details.put("muscle_use_score_input_" + section, configuredMuscleScore);
        return configuredMuscleScore;
    }

    private int calculateForceLoadScore(double loadKg, boolean isStaticOrRepeated, Map<String, Object> details, String section) {
        int score;
        if (loadKg < 2) {
            score = 0; // Load < 2kg intermittent
        } else if (loadKg <= 10) {
            score = isStaticOrRepeated ? 2 : 1; // 1 for intermittent, 2 for static/repeated
        } else { // > 10kg
            score = isStaticOrRepeated ? 3 : 2; // 2 for intermittent, 3 for static/repeated
        }
        // Shock forces would add to score 3, but this is hard to determine from a single MoCap reading.

        details.put("force_load_kg_" + section, loadKg);
        details.put("is_load_static_or_repeated_" + section, isStaticOrRepeated);
        details.put("force_load_score_final_" + section, score);
        return score;
    }

    private int lookupRulaTableA(int uArm, int lArm, int wrist, int twist) {
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
        if (finalRulaScore <= 2) return "Action Level 1: Acceptable posture.";
        if (finalRulaScore <= 4) return "Action Level 2: Further investigation needed, changes may be required.";
        if (finalRulaScore <= 6) return "Action Level 3: Investigation and changes required soon.";
        return "Action Level 4: Investigation and changes required immediately."; // Score 7+
    }

    public static class RulaScoreMapFunction implements MapFunction<MoCapReading, String> {
        private transient MoCapRulaProcessor calculator;
        private transient Gson gson;

        public RulaScoreMapFunction() {

        }

        private void ensureInitialized() {
            if (calculator == null) { calculator = new MoCapRulaProcessor(); }
            if (gson == null) { gson = new GsonBuilder().setPrettyPrinting().create(); }
        }

        @Override
        public String map(MoCapReading moCapReading) throws Exception {
            ensureInitialized();
            if (moCapReading == null) {
                logger.warn("RulaScoreMapFunction received null MoCapReading.");
                return null;
            }
            try {
                RulaScore rulaScore = calculator.calculateRulaScoreInternal(
                    moCapReading,
                    ProcessingParamsConfig.RULA_LOAD_KG,
                    ProcessingParamsConfig.RULA_MUSCLE_USE_SCORE,
                    ProcessingParamsConfig.RULA_IS_LOAD_STATIC_OR_REPEATED
                );
                return (rulaScore != null) ? gson.toJson(rulaScore) : null;
            } catch (Exception e) {
                logger.error("Error mapping MoCapReading to RULA score for thingId {}: {}", moCapReading.getThingid(), e.getMessage(), e);
                return null;
            }
        }
    }
}