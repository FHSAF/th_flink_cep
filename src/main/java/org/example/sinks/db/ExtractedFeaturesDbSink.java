// File: Flink-CEP/src/main/java/org/example/sinks/db/ExtractedFeaturesDbSink.java
package org.example.sinks.db;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.example.models.FeatureDbRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;

public class ExtractedFeaturesDbSink implements Sink<FeatureDbRow> {
    private static final Logger logger = LoggerFactory.getLogger(ExtractedFeaturesDbSink.class);

    private final String jdbcUrlBase;
    private final String dbName;
    private final String tableName;
    private final String username;
    private final String password;

    public ExtractedFeaturesDbSink(String jdbcUrlBase, String dbName, String tableName, String username, String password) {
        this.jdbcUrlBase = jdbcUrlBase;
        this.dbName = dbName;
        this.tableName = tableName;
        this.username = username;
        this.password = password;
    }

    @Override
    public SinkWriter<FeatureDbRow> createWriter(WriterInitContext context) throws IOException {
        String fullJdbcUrl = jdbcUrlBase + dbName;
        return new ExtractedFeaturesDbSinkWriter(fullJdbcUrl, tableName, username, password);
    }
    
    // Add deprecated createWriter if your Flink version requires it for compatibility
    @Override
    @Deprecated
    public SinkWriter<FeatureDbRow> createWriter(InitContext context) throws IOException {
        logger.warn("ExtractedFeaturesDbSink: Using deprecated createWriter(Sink.InitContext).");
        String fullJdbcUrl = jdbcUrlBase + dbName;
        return new ExtractedFeaturesDbSinkWriter(fullJdbcUrl, tableName, username, password);
    }


    private static class ExtractedFeaturesDbSinkWriter implements SinkWriter<FeatureDbRow>, Serializable {
        private static final long serialVersionUID = 507L; // Unique ID

        private final String jdbcUrl;
        private final String tableName;
        private final String username;
        private final String password;
        private transient Connection connection;
        private transient PreparedStatement statement;

        private static final String INSERT_SQL_TEMPLATE = "INSERT INTO %s " +
            "(time, thing_id, channel_name, rms, mdf, mnf, mnp, rms_percent_mvc, source_timestamp_ms) " +
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) " +
            "ON CONFLICT (time, thing_id, channel_name) DO UPDATE SET " +
            "rms = EXCLUDED.rms, mdf = EXCLUDED.mdf, mnf = EXCLUDED.mnf, mnp = EXCLUDED.mnp, " +
            "rms_percent_mvc = EXCLUDED.rms_percent_mvc, source_timestamp_ms = EXCLUDED.source_timestamp_ms";


        public ExtractedFeaturesDbSinkWriter(String jdbcUrl, String tableName, String username, String password) throws IOException {
            this.jdbcUrl = jdbcUrl;
            this.tableName = tableName;
            this.username = username;
            this.password = password;
            initializeJdbc();
        }

        private void initializeJdbc() throws IOException {
            try {
                this.connection = DriverManager.getConnection(jdbcUrl, username, password);
                String actualInsertSql = String.format(INSERT_SQL_TEMPLATE, this.tableName);
                this.statement = connection.prepareStatement(actualInsertSql);
                logger.info("ExtractedFeaturesDbSinkWriter: DB connected for table {}.", this.tableName);
            } catch (SQLException e) {
                logger.error("ExtractedFeaturesDbSinkWriter: Failed to establish JDBC for table {}", this.tableName, e);
                throw new IOException("Failed to establish JDBC connection", e);
            }
        }

        private void checkConnection() throws IOException {
            if (connection == null) { initializeJdbc(); return; }
            try {
                if (!connection.isValid(1)) {
                    logger.warn("ExtractedFeaturesDbSinkWriter: JDBC connection invalid. Reconnecting...");
                    closeSilently();
                    initializeJdbc();
                }
            } catch (SQLException e) {
                logger.error("ExtractedFeaturesDbSinkWriter: Error checking/restoring connection.", e);
                closeSilently();
                initializeJdbc();
            }
        }

        @Override
        public void write(FeatureDbRow row, Context context) throws IOException {
            if (row == null) { logger.warn("ExtractedFeaturesDbSinkWriter: Received null row. Skipping."); return; }
            checkConnection();

            try {
                statement.setTimestamp(1, row.eventTime);
                statement.setString(2, row.thingId);
                statement.setString(3, row.channelName);

                if (row.rms != null) statement.setDouble(4, row.rms); else statement.setNull(4, Types.DOUBLE);
                if (row.mdf != null) statement.setDouble(5, row.mdf); else statement.setNull(5, Types.DOUBLE);
                if (row.mnf != null) statement.setDouble(6, row.mnf); else statement.setNull(6, Types.DOUBLE);
                if (row.mnp != null) statement.setDouble(7, row.mnp); else statement.setNull(7, Types.DOUBLE);
                if (row.rmsPercentMvc != null) statement.setDouble(8, row.rmsPercentMvc); else statement.setNull(8, Types.DOUBLE);
                if (row.sourceTimestampMsOrigin != null) statement.setLong(9, row.sourceTimestampMsOrigin); else statement.setNull(9, Types.BIGINT);
                
                statement.executeUpdate();
            } catch (SQLException e) {
                // ON CONFLICT handles duplicates, so other SQL errors are more concerning
                logger.error("ExtractedFeaturesDbSinkWriter: Error inserting features for {}-{}: {}", row.thingId, row.channelName, e.getMessage(), e);
                if (e.getMessage() != null && (e.getMessage().toLowerCase().contains("connection") || e.getMessage().toLowerCase().contains("broken pipe"))) {
                    closeSilently(); initializeJdbc();
                }
            } catch (Exception e) {
                logger.error("ExtractedFeaturesDbSinkWriter: Unexpected error writing features for {}-{}: {}", row.thingId, row.channelName, e.getMessage(), e);
            }
        }
        
        @Override public void flush(boolean endOfInput) {}
        private void closeSilently() {
            try { if (statement != null) statement.close(); } catch (SQLException ignored) {}
            try { if (connection != null) connection.close(); } catch (SQLException ignored) {}
            statement = null; connection = null;
        }
        @Override public void close() throws Exception { closeSilently(); }
    }
}