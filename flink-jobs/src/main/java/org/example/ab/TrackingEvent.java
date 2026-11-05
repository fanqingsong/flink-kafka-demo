package org.example.ab;

import java.io.Serializable;
import java.math.BigDecimal;

public class TrackingEvent implements Serializable {

    private String eventId;
    private String eventName;
    private String eventTime;
    private String receivedAt;
    private String userId;
    private String sessionId;
    private String experimentId;
    private String variantId;
    private String properties;
    private int attributed;
    private long version;
    private long eventTimestamp;
    private BigDecimal revenue = BigDecimal.ZERO;

    public TrackingEvent() {
    }

    public String getEventId() { return eventId; }
    public void setEventId(String eventId) { this.eventId = eventId; }
    public String getEventName() { return eventName; }
    public void setEventName(String eventName) { this.eventName = eventName; }
    public String getEventTime() { return eventTime; }
    public void setEventTime(String eventTime) { this.eventTime = eventTime; }
    public String getReceivedAt() { return receivedAt; }
    public void setReceivedAt(String receivedAt) { this.receivedAt = receivedAt; }
    public String getUserId() { return userId; }
    public void setUserId(String userId) { this.userId = userId; }
    public String getSessionId() { return sessionId; }
    public void setSessionId(String sessionId) { this.sessionId = sessionId; }
    public String getExperimentId() { return experimentId; }
    public void setExperimentId(String experimentId) { this.experimentId = experimentId; }
    public String getVariantId() { return variantId; }
    public void setVariantId(String variantId) { this.variantId = variantId; }
    public String getProperties() { return properties; }
    public void setProperties(String properties) { this.properties = properties; }
    public int getAttributed() { return attributed; }
    public void setAttributed(int attributed) { this.attributed = attributed; }
    public long getVersion() { return version; }
    public void setVersion(long version) { this.version = version; }
    public long getEventTimestamp() { return eventTimestamp; }
    public void setEventTimestamp(long eventTimestamp) { this.eventTimestamp = eventTimestamp; }
    public BigDecimal getRevenue() { return revenue; }
    public void setRevenue(BigDecimal revenue) { this.revenue = revenue; }
}
