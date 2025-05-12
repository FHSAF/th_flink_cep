
// File: Flink-CEP/src/main/java/org/example/sinks/db/EyeGazeAttentionAlertDbSink.java
package org.example.sinks.db;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;

import java.io.IOException;
import java.io.Serializable;
import java.sql.*;
import java.time.format.DateTimeFormatter;

public class EyeGazeAttentionAlertDbSink implements Sink<String> {
    private static final Logger logger = LoggerFactory.getLogger(EyeGazeAttentionAlertDbSink.class);
    // Standard ISO formatter
    private static final DateTimeFormatter ISO_FORMATTER = DateTimeFormatter.ISO_OFFSET_DATE_TIME;

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
        private static final long serialVersionUID = 509L;

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
            // Added ON CONFLICT DO NOTHING/UPDATE (choose one)
            this.insertSql = "INSERT INTO " + tableName + " (time, thingid, alert_timestamp, feedback_type, severity, reason, details_json) VALUES (?, ?, ?, ?, ?, ?, ?::JSONB) ON CONFLICT (time, thingid, feedback_type) DO NOTHING";
            // Or: ON CONFLICT (time, thingid, feedback_type) DO UPDATE SET severity = EXCLUDED.severity, reason = EXCLUDED.reason, details_json = EXCLUDED.details_json";
            initializeJdbc();
        }

         private void initializeJdbc() throws IOException { /* ... keep existing logic ... */
             try {
                this.connection = DriverManager.getConnection(jdbcUrl, username, password);
                this.statement = connection.prepareStatement(this.insertSql);
                logger.info("EyeGazeAlert Sink: Successfully connected/reconnected.");
            } catch (SQLException e) {
                logger.error("EyeGazeAlert Sink: Failed JDBC connection", e);
                throw new IOException("Failed JDBC connection", e);
            }
         }

        @Override
        public void write(String jsonRecord, Context context) throws IOException {
            if (jsonRecord == null || jsonRecord.isEmpty()) { return; }
             checkConnection();
            String thingId = "unknown";
            Timestamp alertTs = null;
            String feedbackType = "unknown_gaze_alert"; // Default

            try {
                JsonObject alertJson = JsonParser.parseString(jsonRecord).getAsJsonObject();
                thingId = alertJson.has("thingId") ? alertJson.get("thingId").getAsString() : "unknown";
                feedbackType = alertJson.has("feedbackType") ? alertJson.get("feedbackType").getAsString() : "unknown_gaze_alert";

                // Use alertTimestamp or windowEndTimestamp from JSON to create the SQL Timestamp
                Long alertTimestampMillis = alertJson.has("alertTimestamp") ? alertJson.get("alertTimestamp").getAsLong() :
                                            (alertJson.has("windowEndTimestamp") ? alertJson.get("windowEndTimestamp").getAsLong() : null);

                if (alertTimestampMillis != null) {
                    alertTs = new Timestamp(alertTimestampMillis);
                } else {
                    logger.warn("EyeGazeAlert Sink: Missing alert/window timestamp in JSON for {}. Storing NULL.", thingId);
                    alertTs = null; // Ensure it's null
                }

                String severity = alertJson.has("severity") ? alertJson.get("severity").getAsString() : "UNKNOWN";
                String reason = alertJson.has("reason") ? alertJson.get("reason").getAsString() : "N/A";

                // Set Parameters
                if (alertTs != null) statement.setTimestamp(1, alertTs); else statement.setNull(1, Types.TIMESTAMP_WITH_TIMEZONE); // time
                statement.setString(2, thingId);
                if (alertTs != null) statement.setTimestamp(3, alertTs); else statement.setNull(3, Types.TIMESTAMP_WITH_TIMEZONE); // alert_timestamp
                statement.setString(4, feedbackType);
                statement.setString(5, severity);
                statement.setString(6, reason);
                statement.setString(7, jsonRecord); // Store full JSON

                statement.executeUpdate();

            } catch (JsonSyntaxException e) {
                 logger.error("EyeGazeAlert Sink: Error parsing JSON: {}", jsonRecord, e);
            } catch (SQLException e) {
                 if (!"23505".equals(e.getSQLState())) { // Don't log duplicate key warnings if using ON CONFLICT
                    logger.error("EyeGazeAlert Sink: Error inserting data: {}", jsonRecord, e);
                 }
            } catch (Exception e) {
                 logger.error("EyeGazeAlert Sink: Unexpected error writing: {}", jsonRecord, e);
            }
        }

        @Override
        public void flush(boolean endOfInput) throws IOException { /* No-op */ }

        @Override
        public void close() throws IOException { /* ... keep existing logic ... */
            closeSilently();
            logger.info("EyeGazeAlert Sink: Database connection closed.");
        }

        private void checkConnection() throws IOException { /* ... keep existing logic ... */
            if (connection == null) { initializeJdbc(); return; }
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
        private void closeSilently() { /* ... keep existing logic ... */
             try { if (statement != null) statement.close(); } catch (SQLException ignored) {}
             try { if (connection != null) connection.close(); } catch (SQLException ignored) {}
             statement = null;
             connection = null;
        }
    }
}
