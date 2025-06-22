// File: Flink-CEP/src/main/java/org/example/models/FeatureDbRow.java
package org.example.models;

import java.sql.Timestamp;

public class FeatureDbRow {
    public Timestamp eventTime;
    public String thingId;
    public String channelName;
    public Double rms;
    public Double mdf;
    public Double mnf;
    public Double mnp;
    public Double rmsPercentMvc;
    public Long sourceTimestampMsOrigin; // To store original ms timestamp

    public FeatureDbRow() {}

    public FeatureDbRow(Timestamp eventTime, String thingId, String channelName, Double rms, Double mdf, Double mnf, Double mnp, Double rmsPercentMvc, Long sourceTimestampMsOrigin) {
        this.eventTime = eventTime;
        this.thingId = thingId;
        this.channelName = channelName;
        this.rms = rms;
        this.mdf = mdf;
        this.mnf = mnf;
        this.mnp = mnp;
        this.rmsPercentMvc = rmsPercentMvc;
        this.sourceTimestampMsOrigin = sourceTimestampMsOrigin;
    }
}