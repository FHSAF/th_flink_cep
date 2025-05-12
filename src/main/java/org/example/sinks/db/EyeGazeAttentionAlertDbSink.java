// File: Flink-CEP/src/main/java/org/example/sinks/db/EyeGazeAttentionAlertDbSink.java
package org.example.sinks.db;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive; // Import for checking numeric types
import com.google.gson.JsonElement; // Import for checking nulls
import com.google.gson.JsonSyntaxException;

import java.io.IOException;
import java.io.Serializable;
import java.sql.*;
// Removed unused DateTimeFormatter import

public class EyeGazeAttentionAlertDbSink implements Sink<String> {
    private static final Logger logger = LoggerFactory.getLogger(EyeGazeAttentionAlertDbSink.class);

    private final String jdbcUrl;
    private final String username;
    private final String password;
    private final String tableName;

    public EyeGazeAttentionAlertDbSink(String jdbcUrl, String tableName, String username, String password) {
        this.jdbcUrl = jdbcUrl;
        this.tableName = tableName;
        this.username = username;
        this.password = password;
    }

    @Override
    public SinkWriter<String> createWriter(WriterInitContext context) throws IOException {
        return new EyeGazeAttentionAlertDbSinkWriter(jdbcUrl, tableName, username, password);
    }

    @Override
    @Deprecated
    public SinkWriter<String> createWriter(InitContext context) throws IOException {
        logger.warn("EyeGazeAttentionAlertDbSink: Using deprecated createWriter(Sink.InitContext).");
        return new EyeGazeAttentionAlertDbSinkWriter(jdbcUrl, tableName, username, password);
    }

    private static class EyeGazeAttentionAlertDbSinkWriter implements SinkWriter<String>, Serializable {
        // Use a different serialVersionUID if making incompatible changes,
        // but style changes are generally compatible. Incrementing is safe.
        private static final long serialVersionUID = 510L; // Incremented

        private final String insertSql;
        private final String jdbcUrl;
        private final String username;
        private final String password;
        private transient Connection connection;
        private transient PreparedStatement statement;

        public EyeGazeAttentionAlertDbSinkWriter(String jdbcUrl, String tableName, String username, String password) throws IOException {
            this.jdbcUrl = jdbcUrl;
            this.username = username;
            this.password = password;
            // SQL statement remains the same as the table structure is assumed unchanged
            this.insertSql = "INSERT INTO " + tableName + " (time, thingid, alert_timestamp, feedback_type, severity, reason, details_json) VALUES (?, ?, ?, ?, ?, ?, ?::JSONB)";
            // Add ON CONFLICT clause if needed, e.g.:
            // " ON CONFLICT (time, thingid, feedback_type) DO NOTHING";
            initializeJdbc();
        }

        private void initializeJdbc() throws IOException {
            try {
                this.connection = DriverManager.getConnection(jdbcUrl, username, password);
                this.statement = connection.prepareStatement(this.insertSql);
                logger.info("EyeGazeAlert Sink: Successfully connected/reconnected.");
            } catch (SQLException e) {
                logger.error("EyeGazeAlert Sink: Failed JDBC connection", e);
                throw new IOException("Failed JDBC connection", e);
            }
        }

        // --- Helper Methods for Safe JSON Extraction ---

        private Long getLongOrNull(JsonObject obj, String key) {
            if (obj.has(key)) {
                JsonElement element = obj.get(key);
                if (element != null && !element.isJsonNull() && element.isJsonPrimitive()) {
                    JsonPrimitive primitive = element.getAsJsonPrimitive();
                    if (primitive.isNumber()) {
                        try {
                            return primitive.getAsLong();
                        } catch (NumberFormatException e) {
                             logger.warn("Could not parse field '{}' as Long: {}", key, primitive.toString(), e);
                             return null;
                        }
                    }
                }
            }
            logger.debug("Field '{}' not found or not a valid long in JSON.", key);
            return null;
        }

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

        // --- End Helper Methods ---

        @Override
        public void write(String jsonRecord, Context context) throws IOException {
            if (jsonRecord == null || jsonRecord.isEmpty()) {
                return;
            }
            checkConnection();
            String thingId = "unknown";
            Timestamp alertTs = null;
            String feedbackType = "unknown_gaze_alert"; // Default

            try {
                JsonObject alertJson = JsonParser.parseString(jsonRecord).getAsJsonObject();

                // Use helpers for safer extraction
                thingId = getStringOrNull(alertJson, "thingId");
                feedbackType = getStringOrNull(alertJson, "feedbackType");

                // Extract timestamp robustly, checking both possible fields
                Long alertTimestampMillis = getLongOrNull(alertJson, "alertTimestamp");
                if (alertTimestampMillis == null) {
                    alertTimestampMillis = getLongOrNull(alertJson, "windowEndTimestamp");
                }

                if (alertTimestampMillis != null) {
                    alertTs = new Timestamp(alertTimestampMillis);
                } else {
                    logger.warn("EyeGazeAlert Sink: Missing 'alertTimestamp' or 'windowEndTimestamp' (Long) field in JSON for thingId: {}. Storing NULL for time fields.", thingId);
                    alertTs = null; // Ensure it's null if both fields are missing/invalid
                }

                String severity = getStringOrNull(alertJson, "severity");
                String reason = getStringOrNull(alertJson, "reason");

                // --- Set Parameters ---

                // Parameter 1: time (primary timestamp for hypertable)
                if (alertTs != null) statement.setTimestamp(1, alertTs); else statement.setNull(1, Types.TIMESTAMP_WITH_TIMEZONE);
                // Parameter 2: thingid
                statement.setString(2, thingId != null ? thingId : "unknown");
                // Parameter 3: alert_timestamp (can be same as 'time' or used differently if needed)
                if (alertTs != null) statement.setTimestamp(3, alertTs); else statement.setNull(3, Types.TIMESTAMP_WITH_TIMEZONE);
                // Parameter 4: feedback_type
                if (feedbackType != null) statement.setString(4, feedbackType); else statement.setNull(4, Types.VARCHAR);
                // Parameter 5: severity
                if (severity != null) statement.setString(5, severity); else statement.setNull(5, Types.VARCHAR);
                // Parameter 6: reason
                if (reason != null) statement.setString(6, reason); else statement.setNull(6, Types.VARCHAR);
                // Parameter 7: details_json (Store full raw JSON)
                statement.setString(7, jsonRecord);

                statement.executeUpdate();

            } catch (JsonSyntaxException e) {
                logger.error("EyeGazeAlert Sink: Error parsing JSON: {}", jsonRecord, e);
            } catch (SQLException e) {
                if ("23505".equals(e.getSQLState())) { // Postgres unique violation
                     logger.warn("EyeGazeAlert Sink: Duplicate key ignored for time={}, thingid={}, feedback_type={}. Consider ON CONFLICT.", alertTs, thingId, feedbackType);
                 } else {
                    logger.error("EyeGazeAlert Sink: Error inserting data for thingId {}: {}", thingId, jsonRecord, e);
                }
            } catch (Exception e) {
                logger.error("EyeGazeAlert Sink: Unexpected error writing for thingId {}: {}", thingId, jsonRecord, e);
            }
        }

        @Override
        public void flush(boolean endOfInput) throws IOException { /* No-op for basic JDBC */ }

        @Override
        public void close() throws IOException {
            closeSilently();
            logger.info("EyeGazeAlert Sink: Database connection closed.");
        }

        private void checkConnection() throws IOException {
            if (connection == null) {
                initializeJdbc();
                return;
            }
            try {
                if (!connection.isValid(1)) {
                    logger.warn("EyeGazeAlert Sink: JDBC connection is not valid. Reconnecting...");
                    closeSilently();
                    initializeJdbc();
                }
            } catch (SQLException e) {
                logger.error("EyeGazeAlert Sink: Error checking/restoring connection.", e);
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