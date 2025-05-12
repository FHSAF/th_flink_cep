package org.example.sinks.db;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.example.models.RulaScore; // Import RulaScore
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

public class RulaScoreDbSink implements Sink<String> {
    private static final Logger logger = LoggerFactory.getLogger(RulaScoreDbSink.class);
    private static final DateTimeFormatter ISO_FORMATTER = DateTimeFormatter.ISO_OFFSET_DATE_TIME;

    private final String jdbcUrl;
    private final String username;
    private final String password;
    private final String tableName;

    public RulaScoreDbSink(String jdbcUrl, String tableName, String username, String password) {
        this.jdbcUrl = jdbcUrl;
        this.tableName = tableName;
        this.username = username;
        this.password = password;
    }

    @Override
    public SinkWriter<String> createWriter(WriterInitContext context) throws IOException {
        return new RulaScoreDbSinkWriter(jdbcUrl, tableName, username, password);
    }

    @Deprecated
    @Override
    public SinkWriter<String> createWriter(InitContext context) throws IOException {
        logger.warn("RulaScoreDbSink: Using deprecated createWriter(Sink.InitContext).");
        return new RulaScoreDbSinkWriter(jdbcUrl, tableName, username, password);
    }

    private static class RulaScoreDbSinkWriter implements SinkWriter<String>, Serializable {
        private static final long serialVersionUID = 601L; // New unique ID

        private final String insertSql;
        private final String jdbcUrl;
        private final String username;
        private final String password;
        private transient Connection connection;
        private transient PreparedStatement statement;
        private transient Gson gson;

        public RulaScoreDbSinkWriter(String jdbcUrl, String tableName, String username, String password) throws IOException {
            this.jdbcUrl = jdbcUrl;
            this.username = username;
            this.password = password;
            this.insertSql = "INSERT INTO " + tableName + " (" +
                "time, thingid, timestamp_str, " +
                "upper_arm_score, lower_arm_score, wrist_score, wrist_twist_score, posture_score_a, " +
                "muscle_use_score_aw, force_load_score_aw, wrist_and_arm_score, " +
                "neck_score, trunk_score, leg_score, posture_score_b, " +
                "muscle_use_score_ntl, force_load_score_ntl, neck_trunk_leg_score, " +
                "final_rula_score, risk_level, calculation_details_json" + // Added calculation_details_json
                ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?::JSONB) " +
                "ON CONFLICT (time, thingid) DO NOTHING"; // Assuming time + thingid is unique
            initializeJdbc();
        }

        private void initializeJdbc() throws IOException {
             try {
                if (this.gson == null) { this.gson = new Gson(); }
                this.connection = DriverManager.getConnection(jdbcUrl, username, password);
                this.statement = connection.prepareStatement(this.insertSql);
                logger.info("RulaScore Sink: Successfully connected to the database {}.", jdbcUrl);
            } catch (SQLException e) {
                logger.error("RulaScore Sink: Failed to establish JDBC connection to {}", jdbcUrl, e);
                throw new IOException("Failed to establish JDBC connection", e);
            }
        }

        @Override
        public void write(String jsonRecord, Context context) throws IOException {
            if (jsonRecord == null || jsonRecord.isEmpty()) {
                logger.debug("RulaScore Sink: Received null or empty record. Skipping.");
                return;
            }
            checkConnection();
            RulaScore record = null;
            Timestamp sqlTimestamp = null;
            String originalTimestampStr = null;

            try {
                record = gson.fromJson(jsonRecord, RulaScore.class);
                if (record == null || record.thingId == null) {
                    logger.warn("RulaScore Sink: Failed to parse RulaScore from JSON or missing thingId: {}", jsonRecord);
                    return;
                }
                originalTimestampStr = record.timestamp;

                if (originalTimestampStr != null) {
                    Instant instant = Instant.from(ISO_FORMATTER.parse(originalTimestampStr));
                    sqlTimestamp = Timestamp.from(instant);
                } else {
                     logger.warn("RulaScore Sink: Missing timestamp string in parsed RulaScore for {}. Storing NULL for time.", record.thingId);
                }

            } catch (JsonSyntaxException e) {
                 logger.error("RulaScore Sink: Error parsing JSON for RulaScore: {}", jsonRecord, e);
                 return;
            } catch (DateTimeParseException e) {
                 logger.warn("RulaScore Sink: Failed to parse ISO timestamp '{}' for RulaScore {}. Storing NULL for time.", originalTimestampStr, record.thingId, e);
                 sqlTimestamp = null; // Ensure it's null if parsing fails
            } catch (Exception e) { // Catch any other unexpected errors during parsing
                 logger.error("RulaScore Sink: Unexpected error during RulaScore parsing for {}: {}", jsonRecord, e);
                 return;
            }

            if (record == null) return; // Should have been caught above, but defensive check
            if (sqlTimestamp == null && originalTimestampStr != null) { // If parsing failed but we had a timestamp string
                logger.error("RulaScore Sink: Skipping insert for {} due to failed timestamp parsing (time column cannot be NULL). Original: {}", record.thingId, originalTimestampStr);
                return;
            }
             if (sqlTimestamp == null && originalTimestampStr == null) {
                 logger.error("RulaScore Sink: Skipping insert for {} due to missing timestamp. Original: {}", record.thingId, originalTimestampStr);
                return;
             }


            try {
                statement.setTimestamp(1, sqlTimestamp);
                statement.setString(2, record.thingId);
                statement.setString(3, originalTimestampStr);

                statement.setInt(4, record.upperArmScore);
                statement.setInt(5, record.lowerArmScore);
                statement.setInt(6, record.wristScore);
                statement.setInt(7, record.wristTwistScore);
                statement.setInt(8, record.postureScoreA);
                statement.setInt(9, record.muscleUseScoreAW);
                statement.setInt(10, record.forceLoadScoreAW);
                statement.setInt(11, record.wristAndArmScore);
                statement.setInt(12, record.neckScore);
                statement.setInt(13, record.trunkScore);
                statement.setInt(14, record.legScore);
                statement.setInt(15, record.postureScoreB);
                statement.setInt(16, record.muscleUseScoreNTL);
                statement.setInt(17, record.forceLoadScoreNTL);
                statement.setInt(18, record.neckTrunkLegScore);
                statement.setInt(19, record.finalRulaScore);
                statement.setString(20, record.riskLevel);
                statement.setString(21, gson.toJson(record.calculationDetails));


                statement.executeUpdate();
            } catch (SQLException e) {
                if (!"23505".equals(e.getSQLState())) { // PostgresSQL unique violation
                    logger.error("RulaScore Sink: Error inserting RulaScore data: {}", jsonRecord, e);
                 } else {
                    logger.debug("RulaScore Sink: Duplicate RulaScore record ignored for time={}, thingid={}", sqlTimestamp, record.thingId);
                 }
            } catch (Exception e) { // Catch any other unexpected errors during DB write
                 logger.error("RulaScore Sink: Unexpected error writing RulaScore: {}", jsonRecord, e);
            }
        }

        @Override
        public void flush(boolean endOfInput) throws IOException { /* No-op, handled by JDBC driver or batching if configured */ }

        @Override
        public void close() throws IOException {
            closeSilently();
            logger.info("RulaScore Sink: Database connection closed.");
        }

        private void checkConnection() throws IOException {
            if (connection == null) { initializeJdbc(); return; }
            try {
                if (!connection.isValid(1)) { // Check validity with a 1-second timeout
                    logger.warn("RulaScore Sink: JDBC connection is not valid. Attempting to reconnect...");
                    closeSilently(); // Close existing broken connection resources
                    initializeJdbc(); // Re-establish connection
                }
            } catch (SQLException e) {
                 logger.error("RulaScore Sink: Error checking/restoring JDBC connection.", e);
                 closeSilently(); // Close potentially broken resources
                 initializeJdbc(); // Attempt to re-establish
            }
        }

        private void closeSilently() {
            try { if (statement != null) statement.close(); } catch (SQLException ignored) {}
            try { if (connection != null) connection.close(); } catch (SQLException ignored) {}
            statement = null;
            connection = null;
            // gson is transient and re-initialized in initializeJdbc if needed
        }
    }
}