package org.example.models;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
public class EmgFeatureMessage {
    @JsonProperty("thingId")
    private String thingId;

    // This annotation MUST match the JSON field
    @JsonProperty("timestamp_utc")
    private String timestampUtc;

    @JsonProperty("sourceTimestampMs")
    private Long sourceTimestampMs;
    
    // This annotation MUST match the JSON field
    @JsonProperty("channels_features")
    private Map<String, ChannelSpecificFeatures> channelsFeatures;

    // Getters and setters...
    public String getThingId() { return thingId; }
    public void setThingId(String thingId) { this.thingId = thingId; }
    public String getTimestampUtc() { return timestampUtc; }
    public void setTimestampUtc(String timestampUtc) { this.timestampUtc = timestampUtc; }
    public Long getSourceTimestampMs() { return sourceTimestampMs; }
    public void setSourceTimestampMs(Long sourceTimestampMs) { this.sourceTimestampMs = sourceTimestampMs; }
    public Map<String, ChannelSpecificFeatures> getChannelsFeatures() { return channelsFeatures; }
    public void setChannelsFeatures(Map<String, ChannelSpecificFeatures> channelsFeatures) { this.channelsFeatures = channelsFeatures; }
}