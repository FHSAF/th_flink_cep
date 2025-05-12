// File: Flink-CEP/src/main/java/org/example/sinks/db/EMGFatigueAlertDbSink.java
package org.example.sinks.db;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
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
import java.time.Instant; // Use Instant for ISO parsing
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

public class EMGFatigueAlertDbSink implements Sink<String> {
    private static final Logger logger = LoggerFactory.getLogger(EMGFatigueAlertDbSink.class);
    // Standard ISO formatter
    private static final DateTimeFormatter ISO_FORMATTER = DateTimeFormatter.ISO_OFFSET_DATE_TIME;

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
        private static final long serialVersionUID = 505L;

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
             // Added ON CONFLICT DO NOTHING/UPDATE (choose one)
             this.insertSql = "INSERT INTO " + tableName + " (" +
                "time, thingid, alert_timestamp, muscle, severity, reason, " + // time is TIMESTAMPTZ
                "average_rms, check_window_seconds, rms_threshold, full_alert_json" +
                ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?::JSONB)";
             // Or: ON CONFLICT (time, thingid, muscle) DO UPDATE SET ... ";
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

        @Override
        public void write(String jsonRecord, Context context) throws IOException {
            if (jsonRecord == null || jsonRecord.isEmpty()) { return; }
             checkConnection();
            String thingId = "unknown"; // Initialize defaults
            Timestamp alertTs = null;
            String originalTimestampStr = null; // Store original if available in JSON
            String muscle = "unknown"; // Default for log message

            try {
                JsonObject alertJson = JsonParser.parseString(jsonRecord).getAsJsonObject();
                thingId = alertJson.has("thingId") ? alertJson.get("thingId").getAsString() : "unknown";
                muscle = alertJson.has("muscle") ? alertJson.get("muscle").getAsString() : "unknown";

                // Attempt to parse ISO timestamp from the alert payload if available
                originalTimestampStr = alertJson.has("originalTimestamp") ? alertJson.get("originalTimestamp").getAsString() : null; // Example field
                Long alertTimestampMillis = alertJson.has("timestamp") ? alertJson.get("timestamp").getAsLong() : null; // Flink processing time

                if (originalTimestampStr != null) {
                     try {
                         Instant instant = Instant.from(ISO_FORMATTER.parse(originalTimestampStr));
                         alertTs = Timestamp.from(instant);
                     } catch (DateTimeParseException e) {
                         logger.warn("EMGFatigueAlert Sink: Failed parse original ISO timestamp '{}'. Using Flink timestamp.", originalTimestampStr);
                         if (alertTimestampMillis != null) alertTs = new Timestamp(alertTimestampMillis);
                     }
                } else if (alertTimestampMillis != null) {
                    alertTs = new Timestamp(alertTimestampMillis);
                    logger.debug("EMGFatigueAlert Sink: Using Flink processing timestamp for alert.");
                } else {
                    logger.warn("EMGFatigueAlert Sink: Missing timestamp in JSON for {}. Storing NULL.", thingId);
                     alertTs = null;
                }

                String severity = alertJson.has("severity") ? alertJson.get("severity").getAsString() : "UNKNOWN";
                String reason = alertJson.has("reason") ? alertJson.get("reason").getAsString() : "N/A";
                Double averageRms = alertJson.has("averageRMS") && !alertJson.get("averageRMS").isJsonNull() ? alertJson.get("averageRMS").getAsDouble() : null;
                Integer checkWindow = alertJson.has("checkWindowSeconds") ? alertJson.get("checkWindowSeconds").getAsInt() : null;
                Double rmsThreshold = alertJson.has("rmsThreshold") && !alertJson.get("rmsThreshold").isJsonNull() ? alertJson.get("rmsThreshold").getAsDouble() : null;

                // Set Parameters
                if (alertTs != null) statement.setTimestamp(1, alertTs); else statement.setNull(1, Types.TIMESTAMP_WITH_TIMEZONE); // time
                statement.setString(2, thingId);
                if (alertTs != null) statement.setTimestamp(3, alertTs); else statement.setNull(3, Types.TIMESTAMP_WITH_TIMEZONE); // alert_timestamp
                statement.setString(4, muscle);
                statement.setString(5, severity);
                statement.setString(6, reason);
                if (averageRms != null) statement.setDouble(7, averageRms); else statement.setNull(7, Types.DOUBLE);
                if (checkWindow != null) statement.setInt(8, checkWindow); else statement.setNull(8, Types.INTEGER);
                if (rmsThreshold != null) statement.setDouble(9, rmsThreshold); else statement.setNull(9, Types.DOUBLE);
                statement.setString(10, jsonRecord); // full_alert_json

                statement.executeUpdate();
            } catch (JsonSyntaxException e) {
                 logger.error("EMGFatigueAlert Sink: Error parsing JSON: {}", jsonRecord, e);
            } catch (SQLException e) {
                 if (!"23505".equals(e.getSQLState())) { // Don't log duplicate key warnings if using ON CONFLICT
                    logger.error("EMGFatigueAlert Sink: Error inserting data: {}", jsonRecord, e);
                 }
            } catch (Exception e) {
                 logger.error("EMGFatigueAlert Sink: Unexpected error writing: {}", jsonRecord, e);
            }
        }

        @Override
        public void flush(boolean endOfInput) throws IOException { /* No-op */ }

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