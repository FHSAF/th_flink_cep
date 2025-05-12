
// File: Flink-CEP/src/main/java/org/example/sinks/db/AvgAngleAlertDbSink.java
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

public class AvgAngleAlertDbSink implements Sink<String> {
    private static final Logger logger = LoggerFactory.getLogger(AvgAngleAlertDbSink.class);

    private final String jdbcUrl;
    private final String username;
    private final String password;
    private final String tableName;

    public AvgAngleAlertDbSink(String jdbcUrl, String tableName, String username, String password) {
        this.jdbcUrl = jdbcUrl;
        this.tableName = tableName;
        this.username = username;
        this.password = password;
    }

    @Override
    public SinkWriter<String> createWriter(WriterInitContext context) throws IOException {
        return new AvgAngleAlertDbSinkWriter(jdbcUrl, tableName, username, password);
    }

    @Override
    @Deprecated
    public SinkWriter<String> createWriter(InitContext context) throws IOException {
        logger.warn("AvgAngleAlertDbSink: Using deprecated createWriter(Sink.InitContext).");
        return new AvgAngleAlertDbSinkWriter(jdbcUrl, tableName, username, password);
    }

    private static class AvgAngleAlertDbSinkWriter implements SinkWriter<String>, Serializable {
        private static final long serialVersionUID = 503L;

        private final String insertSql;
        private final String jdbcUrl;
        private final String username;
        private final String password;
        private transient Connection connection;
        private transient PreparedStatement statement;

        public AvgAngleAlertDbSinkWriter(String jdbcUrl, String tableName, String username, String password) throws IOException {
             this.jdbcUrl = jdbcUrl;
             this.username = username;
             this.password = password;
             this.insertSql = "INSERT INTO " + tableName + " (" +
                "time, thingid, window_end_ts, feedback_type, alert_details" + // time is TIMESTAMPTZ
                ") VALUES (?, ?, ?, ?, ?::JSONB)";
            initializeJdbc();
        }

         private void initializeJdbc() throws IOException { /* ... keep existing logic ... */
             try {
                this.connection = DriverManager.getConnection(jdbcUrl, username, password);
                this.statement = connection.prepareStatement(this.insertSql);
                logger.info("AvgAngleAlert Sink: Successfully connected/reconnected to the database.");
            } catch (SQLException e) {
                logger.error("AvgAngleAlert Sink: Failed to establish JDBC connection", e);
                throw new IOException("Failed to establish JDBC connection", e);
            }
         }

        @Override
        public void write(String jsonRecord, Context context) throws IOException {
             if (jsonRecord == null || jsonRecord.isEmpty()) { return; }
             checkConnection();
             String thingId = null; // Initialize to handle potential parse errors
             Timestamp hypertableTime = null; // Initialize to handle potential parse errors

             try {
                JsonObject jsonObject = JsonParser.parseString(jsonRecord).getAsJsonObject();
                thingId = jsonObject.has("thingId") ? jsonObject.get("thingId").getAsString() : null;
                Long windowEndMillis = jsonObject.has("windowEndTimestamp") ? jsonObject.get("windowEndTimestamp").getAsLong() : null;
                String feedbackType = jsonObject.has("feedbackType") ? jsonObject.get("feedbackType").getAsString() : "unknown";

                if (thingId == null || windowEndMillis == null) {
                    logger.warn("AvgAngleAlert Sink: Missing fields in JSON: {}", jsonRecord);
                    return;
                }

                Timestamp windowEndTs = new Timestamp(windowEndMillis);
                hypertableTime = windowEndTs; // Use window end as primary time

                // Set Parameters
                statement.setTimestamp(1, hypertableTime);
                statement.setString(2, thingId);
                statement.setTimestamp(3, windowEndTs);
                statement.setString(4, feedbackType);
                statement.setString(5, jsonRecord); // Store full JSON

                statement.executeUpdate();

            } catch (JsonSyntaxException e) {
                 logger.error("AvgAngleAlert Sink: Error parsing JSON: {}", jsonRecord, e);
            } catch (SQLException e) {
                 if ("23505".equals(e.getSQLState())) {
                     logger.warn("AvgAngleAlert Sink: Duplicate key ignored for time={}, thingid={}", hypertableTime, thingId);
                 } else {
                    logger.error("AvgAngleAlert Sink: Error inserting data: {}", jsonRecord, e);
                 }
            } catch (Exception e) {
                 logger.error("AvgAngleAlert Sink: Unexpected error writing: {}", jsonRecord, e);
            }
        }

        @Override
        public void flush(boolean endOfInput) throws IOException { /* No-op */ }

        @Override
        public void close() throws IOException { /* ... keep existing logic ... */
            closeSilently();
            logger.info("AvgAngleAlert Sink: Database connection closed.");
        }

        private void checkConnection() throws IOException { /* ... keep existing logic ... */
            if (connection == null) { initializeJdbc(); return; }
            try {
                if (!connection.isValid(1)) {
                    logger.warn("AvgAngleAlert Sink: JDBC connection is not valid. Reconnecting...");
                    closeSilently();
                    initializeJdbc();
                }
            } catch (SQLException e) {
                 logger.error("AvgAngleAlert Sink: Error checking/restoring connection.", e);
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
