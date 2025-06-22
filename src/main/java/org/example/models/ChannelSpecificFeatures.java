// File: Flink-CEP/src/main/java/org/example/models/ChannelSpecificFeatures.java
package org.example.models;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class ChannelSpecificFeatures {
    // FIX: Annotation must match the JSON key "original_ch_name"
    @JsonProperty("original_ch_name")
    private String originalChannelName;

    @JsonProperty("RMS")
    private Double rms;
    @JsonProperty("MDF")
    private Double mdf;
    @JsonProperty("MNF")
    private Double mnf;
    @JsonProperty("MNP")
    private Double mnp;
    @JsonProperty("RMS_percent_MVC")
    private Double rmsPercentMvc;

    // Getters and setters...
    public String getOriginalChannelName() { return originalChannelName; }
    public void setOriginalChannelName(String originalChannelName) { this.originalChannelName = originalChannelName; }
    public Double getRms() { return rms; }
    public void setRms(Double rms) { this.rms = rms; }
    public Double getMdf() { return mdf; }
    public void setMdf(Double mdf) { this.mdf = mdf; }
    public Double getMnf() { return mnf; }
    public void setMnf(Double mnf) { this.mnf = mnf; }
    public Double getMnp() { return mnp; }
    public void setMnp(Double mnp) { this.mnp = mnp; }
    public Double getRmsPercentMvc() { return rmsPercentMvc; }
    public void setRmsPercentMvc(Double rmsPercentMvc) { this.rmsPercentMvc = rmsPercentMvc; }
}