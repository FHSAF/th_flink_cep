
// File: Flink-CEP/src/main/java/org/example/sinks/db/EyeGazeRawDbSink.java
package org.example.sinks.db;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.example.models.EyeGazeReading;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.sql.*;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

public class EyeGazeRawDbSink implements Sink<EyeGazeReading> {
    private static final Logger logger = LoggerFactory.getLogger(EyeGazeRawDbSink.class);
    // Standard ISO formatter
    private static final DateTimeFormatter ISO_FORMATTER = DateTimeFormatter.ISO_OFFSET_DATE_TIME;


    private final String jdbcUrl;
    private final String username;
    private final String password;
    private final String tableName;

    public EyeGazeRawDbSink(String jdbcUrl, String tableName, String username, String password) {
        this.jdbcUrl = jdbcUrl;
        this.tableName = tableName;
        this.username = username;
        this.password = password;
    }

    @Override
    public SinkWriter<EyeGazeReading> createWriter(WriterInitContext context) throws IOException {
        return new EyeGazeRawDbSinkWriter(jdbcUrl, tableName, username, password);
    }

    @Override
    @Deprecated
    public SinkWriter<EyeGazeReading> createWriter(InitContext context) throws IOException {
        logger.warn("EyeGazeRawDbSink: Using deprecated createWriter(Sink.InitContext).");
        return new EyeGazeRawDbSinkWriter(jdbcUrl, tableName, username, password);
    }

    private static class EyeGazeRawDbSinkWriter implements SinkWriter<EyeGazeReading>, Serializable {
        private static final long serialVersionUID = 508L;

        private final String insertSql;
        private final String jdbcUrl;
        private final String username;
        private final String password;
        private transient Connection connection;
        private transient PreparedStatement statement;

        public EyeGazeRawDbSinkWriter(String jdbcUrl, String tableName, String username, String password) throws IOException {
            this.jdbcUrl = jdbcUrl;
            this.username = username;
            this.password = password;
            // Added ON CONFLICT DO NOTHING
            this.insertSql = "INSERT INTO " + tableName + " (time, thingid, timestamp_str, attention_state) VALUES (?, ?, ?, ?) ON CONFLICT (time, thingid) DO NOTHING";
            initializeJdbc();
        }

         private void initializeJdbc() throws IOException { /* ... keep existing logic ... */
             try {
                this.connection = DriverManager.getConnection(jdbcUrl, username, password);
                this.statement = connection.prepareStatement(this.insertSql);
                logger.info("EyeGazeRaw Sink: Successfully connected/reconnected.");
            } catch (SQLException e) {
                logger.error("EyeGazeRaw Sink: Failed JDBC connection", e);
                throw new IOException("Failed JDBC connection", e);
            }
         }

        @Override
        public void write(EyeGazeReading record, Context context) throws IOException {
            if (record == null || record.getThingid() == null || record.getTimestamp() == null) { return; }
             checkConnection();

            Timestamp sqlTimestamp = null;
            String originalTimestampStr = record.getTimestamp();
            try {
                // *** Parse the ISO string to get the Instant for the 'time' column ***
                Instant instant = Instant.from(ISO_FORMATTER.parse(originalTimestampStr));
                sqlTimestamp = Timestamp.from(instant);
            } catch (DateTimeParseException | NullPointerException e) {
                logger.warn("EyeGazeRaw Sink: Failed to parse ISO timestamp '{}' for {}. Storing NULL for time.", originalTimestampStr, record.getThingid());
                sqlTimestamp = null;
            } catch (Exception e) {
                 logger.error("EyeGazeRaw Sink: Unexpected error parsing timestamp '{}' for {}.", originalTimestampStr, record.getThingid(), e);
                 sqlTimestamp = null;
            }

            try {
                // Set Parameters
                if (sqlTimestamp != null) statement.setTimestamp(1, sqlTimestamp); else statement.setNull(1, Types.TIMESTAMP_WITH_TIMEZONE);
                statement.setString(2, record.getThingid());
                statement.setString(3, originalTimestampStr); // Store original string
                statement.setBoolean(4, record.isAttention());

                statement.executeUpdate();

            } catch (SQLException e) {
                 if (!"23505".equals(e.getSQLState())) { // Don't log duplicate key warnings if using ON CONFLICT
                    logger.error("EyeGazeRaw Sink: Error inserting data: {}", record, e);
                 }
            } catch (Exception e) {
                 logger.error("EyeGazeRaw Sink: Unexpected error writing: {}", record, e);
            }
        }

        @Override
        public void flush(boolean endOfInput) throws IOException { /* No-op */ }

        @Override
        public void close() throws IOException { /* ... keep existing logic ... */
            closeSilently();
            logger.info("EyeGazeRaw Sink: Database connection closed.");
        }

        private void checkConnection() throws IOException { /* ... keep existing logic ... */
            if (connection == null) { initializeJdbc(); return; }
            try {
                if (!connection.isValid(1)) {
                    logger.warn("EyeGazeRaw Sink: JDBC connection is not valid. Reconnecting...");
                    closeSilently();
                    initializeJdbc();
                }
            } catch (SQLException e) {
                 logger.error("EyeGazeRaw Sink: Error checking/restoring connection.", e);
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
