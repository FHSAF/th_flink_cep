// File: Flink-CEP/src/main/java/org/example/sinks/db/EMGFatigueAlertDbSink.java
package org.example.sinks.db;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive; // Import for checking numeric types
import com.google.gson.JsonElement; // Import for checking nulls
import com.google.gson.JsonSyntaxException;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
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
// Removed unused imports for ISO Formatter and Instant/DateTimeParseException

public class EMGFatigueAlertDbSink implements Sink<String> {
    private static final Logger logger = LoggerFactory.getLogger(EMGFatigueAlertDbSink.class);

    private final String jdbcUrl;
    private final String username;
    private final String password;
    private final String tableName;

    public EMGFatigueAlertDbSink(String jdbcUrl, String tableName, String username, String password) {
        this.jdbcUrl = jdbcUrl;
        this.tableName = tableName;
        this.username = username;
        this.password = password;
    }

    @Override
    public SinkWriter<String> createWriter(WriterInitContext context) throws IOException {
        return new EMGFatigueAlertDbSinkWriter(jdbcUrl, tableName, username, password);
    }

    @Override
    @Deprecated
    public SinkWriter<String> createWriter(InitContext context) throws IOException {
        logger.warn("EMGFatigueAlertDbSink: Using deprecated createWriter(Sink.InitContext).");
        return new EMGFatigueAlertDbSinkWriter(jdbcUrl, tableName, username, password);
    }

    private static class EMGFatigueAlertDbSinkWriter implements SinkWriter<String>, Serializable {
        private static final long serialVersionUID = 506L; // Incremented version ID

        private final String insertSql;
        private final String jdbcUrl;
        private final String username;
        private final String password;
        private transient Connection connection;
        private transient PreparedStatement statement;

        public EMGFatigueAlertDbSinkWriter(String jdbcUrl, String tableName, String username, String password) throws IOException {
             this.jdbcUrl = jdbcUrl;
             this.username = username;
             this.password = password;
             // *** UPDATED SQL INSERT Statement ***
             this.insertSql = "INSERT INTO " + tableName + " (" +
                "time, thingid, feedback_type, muscle, severity, reason, " +
                "mdf_slope_hz_per_sec, regression_r_squared, trend_window_seconds_used, " +
                "num_mdf_points_in_trend, mdf_slope_threshold, r_squared_threshold, " +
                "last_mdf_value, full_alert_json" +
                ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?::JSONB)";
             // Consider adding ON CONFLICT clause if needed, e.g.:
             // " ON CONFLICT (time, thingid, muscle) DO NOTHING";
             // " ON CONFLICT (time, thingid, muscle) DO UPDATE SET severity = EXCLUDED.severity, ...";
            initializeJdbc();
        }

         private void initializeJdbc() throws IOException {
             try {
                this.connection = DriverManager.getConnection(jdbcUrl, username, password);
                this.statement = connection.prepareStatement(this.insertSql);
                logger.info("EMGFatigueAlert Sink: Successfully connected/reconnected.");
            } catch (SQLException e) {
                logger.error("EMGFatigueAlert Sink: Failed JDBC connection", e);
                throw new IOException("Failed JDBC connection", e);
            }
         }

        // Helper to safely get Double from JsonElement
        private Double getDoubleOrNull(JsonObject obj, String key) {
            if (obj.has(key)) {
                JsonElement element = obj.get(key);
                if (element != null && !element.isJsonNull() && element.isJsonPrimitive()) {
                   JsonPrimitive primitive = element.getAsJsonPrimitive();
                   if (primitive.isNumber()) {
                       return primitive.getAsDouble();
                   }
                }
            }
            logger.debug("Field '{}' not found or not a valid double in JSON.", key);
            return null;
        }

        // Helper to safely get Integer from JsonElement
        private Integer getIntegerOrNull(JsonObject obj, String key) {
             if (obj.has(key)) {
                JsonElement element = obj.get(key);
                if (element != null && !element.isJsonNull() && element.isJsonPrimitive()) {
                   JsonPrimitive primitive = element.getAsJsonPrimitive();
                   if (primitive.isNumber()) {
                       try {
                           return primitive.getAsInt();
                       } catch (NumberFormatException e) {
                            // Handle cases where it might be a double like 0.0
                           return (int)primitive.getAsDouble();
                       }
                   }
                }
            }
            logger.debug("Field '{}' not found or not a valid integer in JSON.", key);
            return null;
        }

        // Helper to safely get String from JsonElement
        private String getStringOrNull(JsonObject obj, String key) {
            if (obj.has(key)) {
                JsonElement element = obj.get(key);
                if (element != null && !element.isJsonNull() && element.isJsonPrimitive()) {
                    JsonPrimitive primitive = element.getAsJsonPrimitive();
                    if (primitive.isString()) {
                        return primitive.getAsString();
                    }
                }
            }
             logger.debug("Field '{}' not found or not a valid string in JSON.", key);
            return null;
        }


        @Override
        public void write(String jsonRecord, Context context) throws IOException {
            if (jsonRecord == null || jsonRecord.isEmpty()) { return; }
             checkConnection();
            String thingId = "unknown";
            Timestamp alertTs = null;
            String muscle = "unknown";

            try {
                JsonObject alertJson = JsonParser.parseString(jsonRecord).getAsJsonObject();
                thingId = getStringOrNull(alertJson, "thingId"); // Use helper
                muscle = getStringOrNull(alertJson, "muscle");   // Use helper

                // *** Correctly extract timestamp from alertTriggerTimestamp ***
                Long alertTimestampMillis = null;
                if (alertJson.has("alertTriggerTimestamp")) {
                     JsonElement tsElement = alertJson.get("alertTriggerTimestamp");
                     if (tsElement != null && !tsElement.isJsonNull() && tsElement.isJsonPrimitive() && tsElement.getAsJsonPrimitive().isNumber()) {
                         alertTimestampMillis = tsElement.getAsLong();
                     }
                }

                if (alertTimestampMillis == null) {
                    logger.warn("EMGFatigueAlert Sink: Missing or invalid 'alertTriggerTimestamp' (Long) field in JSON for thingId: {}. Storing NULL for time.", thingId);
                    alertTs = null;
                } else {
                    alertTs = new Timestamp(alertTimestampMillis);
                }

                // *** Extract common and NEW MDF fields ***
                String feedbackType = getStringOrNull(alertJson, "feedbackType");
                String severity = getStringOrNull(alertJson, "severity");
                String reason = getStringOrNull(alertJson, "reason");

                Double mdfSlope = getDoubleOrNull(alertJson, "mdfSlopeHzPerSec");
                Double rSquared = getDoubleOrNull(alertJson, "regressionRSquared");
                Integer trendWindow = getIntegerOrNull(alertJson, "trendWindowSecondsUsed");
                Integer mdfPoints = getIntegerOrNull(alertJson, "numberOfMdfPointsInTrend");
                Double mdfSlopeThresh = getDoubleOrNull(alertJson, "mdfSlopeThreshold");
                Double rSquaredThresh = getDoubleOrNull(alertJson, "rSquaredThreshold");
                Double lastMdf = getDoubleOrNull(alertJson, "lastMdfValue");


                // *** Set Parameters according to NEW SQL Statement ***
                // Parameter 1: time (from alertTriggerTimestamp)
                if (alertTs != null) statement.setTimestamp(1, alertTs); else statement.setNull(1, Types.TIMESTAMP_WITH_TIMEZONE);
                // Parameter 2: thingid
                statement.setString(2, thingId != null ? thingId : "unknown");
                // Parameter 3: feedback_type
                if (feedbackType != null) statement.setString(3, feedbackType); else statement.setNull(3, Types.VARCHAR);
                // Parameter 4: muscle
                if (muscle != null) statement.setString(4, muscle); else statement.setNull(4, Types.VARCHAR);
                // Parameter 5: severity
                if (severity != null) statement.setString(5, severity); else statement.setNull(5, Types.VARCHAR);
                // Parameter 6: reason
                if (reason != null) statement.setString(6, reason); else statement.setNull(6, Types.VARCHAR);

                // Parameter 7: mdf_slope_hz_per_sec
                if (mdfSlope != null) statement.setDouble(7, mdfSlope); else statement.setNull(7, Types.DOUBLE);
                // Parameter 8: regression_r_squared
                if (rSquared != null) statement.setDouble(8, rSquared); else statement.setNull(8, Types.DOUBLE);
                // Parameter 9: trend_window_seconds_used
                if (trendWindow != null) statement.setInt(9, trendWindow); else statement.setNull(9, Types.INTEGER);
                // Parameter 10: num_mdf_points_in_trend
                if (mdfPoints != null) statement.setInt(10, mdfPoints); else statement.setNull(10, Types.INTEGER);
                // Parameter 11: mdf_slope_threshold
                if (mdfSlopeThresh != null) statement.setDouble(11, mdfSlopeThresh); else statement.setNull(11, Types.DOUBLE);
                // Parameter 12: r_squared_threshold
                if (rSquaredThresh != null) statement.setDouble(12, rSquaredThresh); else statement.setNull(12, Types.DOUBLE);
                // Parameter 13: last_mdf_value
                if (lastMdf != null) statement.setDouble(13, lastMdf); else statement.setNull(13, Types.DOUBLE);

                // Parameter 14: full_alert_json
                statement.setString(14, jsonRecord);

                statement.executeUpdate();

            } catch (JsonSyntaxException e) {
                 logger.error("EMGFatigueAlert Sink: Error parsing JSON: {}", jsonRecord, e);
            } catch (SQLException e) {
                 // Consider adding ON CONFLICT handling in SQL if duplicates are expected
                 if ("23505".equals(e.getSQLState())) { // Postgres unique violation
                     logger.warn("EMGFatigueAlert Sink: Duplicate key ignored for time={}, thingid={}, muscle={}. Consider ON CONFLICT.", alertTs, thingId, muscle);
                 } else {
                    logger.error("EMGFatigueAlert Sink: Error inserting data for thingId {}: {}", thingId, jsonRecord, e);
                 }
            } catch (Exception e) {
                 logger.error("EMGFatigueAlert Sink: Unexpected error writing for thingId {}: {}", thingId, jsonRecord, e);
            }
        }

        @Override
        public void flush(boolean endOfInput) throws IOException { /* No-op for basic JDBC */ }

        @Override
        public void close() throws IOException {
            closeSilently();
            logger.info("EMGFatigueAlert Sink: Database connection closed.");
        }

        private void checkConnection() throws IOException {
            if (connection == null) { initializeJdbc(); return; }
            try {
                if (!connection.isValid(1)) {
                    logger.warn("EMGFatigueAlert Sink: JDBC connection is not valid. Reconnecting...");
                    closeSilently();
                    initializeJdbc();
                }
            } catch (SQLException e) {
                 logger.error("EMGFatigueAlert Sink: Error checking/restoring connection.", e);
                 closeSilently();
                 initializeJdbc();
            }
        }
        private void closeSilently() {
             try { if (statement != null) statement.close(); } catch (SQLException ignored) {}
             try { if (connection != null) connection.close(); } catch (SQLException ignored) {}
             statement = null;
             connection = null;
        }
    }
}