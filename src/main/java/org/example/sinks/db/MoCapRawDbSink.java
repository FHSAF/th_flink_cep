package org.example.sinks.db;

import java.sql.Timestamp;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.example.models.MoCapReading;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Instant; // Use Instant for ISO parsing
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

public class MoCapRawDbSink implements Sink<MoCapReading> {
    private static final Logger logger = LoggerFactory.getLogger(MoCapRawDbSink.class);
    // *** CORRECTED FORMATTER to standard ISO ***
    private static final DateTimeFormatter ISO_FORMATTER = DateTimeFormatter.ISO_OFFSET_DATE_TIME;

    private final String jdbcUrl;
    private final String username;
    private final String password;
    private final String tableName;

    public MoCapRawDbSink(String jdbcUrl, String tableName, String username, String password) {
        this.jdbcUrl = jdbcUrl;
        this.tableName = tableName;
        this.username = username;
        this.password = password;
    }

    @Override
    public SinkWriter<MoCapReading> createWriter(WriterInitContext context) throws IOException {
        return new MoCapRawDbSinkWriter(jdbcUrl, tableName, username, password);
    }

    @Override
    @Deprecated
    public SinkWriter<MoCapReading> createWriter(InitContext context) throws IOException {
        logger.warn("MoCapRawDbSink: Using deprecated createWriter(Sink.InitContext).");
        return new MoCapRawDbSinkWriter(jdbcUrl, tableName, username, password);
    }

    private static class MoCapRawDbSinkWriter implements SinkWriter<MoCapReading>, Serializable {
        private static final long serialVersionUID = 501L;

        private final String insertSql;
        private final String jdbcUrl;
        private final String username;
        private final String password;
        private transient Connection connection;
        private transient PreparedStatement statement;

        public MoCapRawDbSinkWriter(String jdbcUrl, String tableName, String username, String password) throws IOException {
            this.jdbcUrl = jdbcUrl;
            this.username = username;
            this.password = password;
            // Added ON CONFLICT DO NOTHING
            this.insertSql = "INSERT INTO " + tableName + " (" +
                "time, thingid, timestamp_str, " +
                "elbow_flex_ext_left, elbow_flex_ext_right, shoulder_flex_ext_left, shoulder_flex_ext_right, " +
                "shoulder_abd_add_left, shoulder_abd_add_right, lowerarm_pron_sup_left, lowerarm_pron_sup_right, " +
                "upperarm_rotation_left, upperarm_rotation_right, hand_flex_ext_left, hand_flex_ext_right, " +
                "hand_radial_ulnar_left, hand_radial_ulnar_right, neck_flex_ext, neck_torsion, head_tilt, " +
                "torso_tilt, torso_side_tilt, back_curve, back_torsion, knee_flex_ext_left, knee_flex_ext_right " +
                ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ";
            initializeJdbc();
        }

        private void initializeJdbc() throws IOException {
             try {
                this.connection = DriverManager.getConnection(jdbcUrl, username, password);
                this.statement = connection.prepareStatement(this.insertSql);
                logger.info("MoCapRaw Sink: Successfully connected/reconnected to the database.");
            } catch (SQLException e) {
                logger.error("MoCapRaw Sink: Failed to establish JDBC connection", e);
                throw new IOException("Failed to establish JDBC connection", e);
            }
        }

        @Override
        public void write(MoCapReading record, Context context) throws IOException {
             if (record == null || record.getThingid() == null || record.getTimestamp() == null) {
                logger.warn("MoCapRaw Sink: Received null or incomplete record. Skipping.");
                return;
            }
             checkConnection();

            Timestamp sqlTimestamp = null;
            String originalTimestampStr = record.getTimestamp();
            try {
                // *** Parse the ISO string using the CORRECT formatter ***
                Instant instant = Instant.from(ISO_FORMATTER.parse(originalTimestampStr));
                sqlTimestamp = Timestamp.from(instant);
            } catch (DateTimeParseException | NullPointerException e) {
                logger.warn("MoCapRaw Sink: Failed to parse ISO timestamp '{}' for {}. Storing NULL for time.", originalTimestampStr, record.getThingid());
                sqlTimestamp = null;
            } catch (Exception e) {
                 logger.error("MoCapRaw Sink: Unexpected error parsing timestamp '{}' for {}.", originalTimestampStr, record.getThingid(), e);
                 sqlTimestamp = null;
            }

            // *** Skip insert if timestamp parsing failed ***
             if (sqlTimestamp == null) {
                 logger.error("MoCapRaw Sink: Skipping insert for {} due to failed timestamp parsing (time column cannot be NULL).", record.getThingid());
                 return;
             }

            try {
                // Set Parameters
                statement.setTimestamp(1, sqlTimestamp); // time (parsed)
                statement.setString(2, record.getThingid());
                statement.setString(3, originalTimestampStr); // timestamp_str (original)

                // Set remaining parameters (4 to 26)
                statement.setDouble(4, record.getElbowFlexExtLeft());
                statement.setDouble(5, record.getElbowFlexExtRight());
                statement.setDouble(6, record.getShoulderFlexExtLeft());
                statement.setDouble(7, record.getShoulderFlexExtRight());
                statement.setDouble(8, record.getShoulderAbdAddLeft());
                statement.setDouble(9, record.getShoulderAbdAddRight());
                statement.setDouble(10, record.getLowerarmPronSupLeft());
                statement.setDouble(11, record.getLowerarmPronSupRight());
                statement.setDouble(12, record.getUpperarmRotationLeft());
                statement.setDouble(13, record.getUpperarmRotationRight());
                statement.setDouble(14, record.getHandFlexExtLeft());
                statement.setDouble(15, record.getHandFlexExtRight());
                statement.setDouble(16, record.getHandRadialUlnarLeft());
                statement.setDouble(17, record.getHandRadialUlnarRight());
                statement.setDouble(18, record.getNeckFlexExt());
                statement.setDouble(19, record.getNeckTorsion());
                statement.setDouble(20, record.getHeadTilt());
                statement.setDouble(21, record.getTorsoTilt());
                statement.setDouble(22, record.getTorsoSideTilt());
                statement.setDouble(23, record.getBackCurve());
                statement.setDouble(24, record.getBackTorsion());
                statement.setDouble(25, record.getKneeFlexExtLeft());
                statement.setDouble(26, record.getKneeFlexExtRight());

                statement.executeUpdate();

            } catch (SQLException e) {
                 // ON CONFLICT handles duplicate keys silently
                 if (!"23505".equals(e.getSQLState())) {
                    logger.error("MoCapRaw Sink: Error inserting raw MoCap data for {}: {}", record.getThingid(), e.getMessage());
                 }
            } catch (Exception e) {
                logger.error("MoCapRaw Sink: Unexpected error writing raw MoCap record for {}: {}", record.getThingid(), e.getMessage());
            }
        }

        @Override
        public void flush(boolean endOfInput) throws IOException { /* No-op */ }

        @Override
        public void close() throws IOException {
            closeSilently();
            logger.info("MoCapRaw Sink: Database connection closed.");
        }

        private void checkConnection() throws IOException {
            if (connection == null) { initializeJdbc(); return; }
            try {
                if (!connection.isValid(1)) {
                    logger.warn("MoCapRaw Sink: JDBC connection is not valid. Reconnecting...");
                    closeSilently();
                    initializeJdbc();
                }
            } catch (SQLException e) {
                 logger.error("MoCapRaw Sink: Error checking/restoring connection.", e);
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