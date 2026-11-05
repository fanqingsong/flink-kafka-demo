package org.example.ab;

import java.io.Serializable;
import java.math.BigDecimal;

public class MetricResult implements Serializable {

    private String windowStart;
    private String windowEnd;
    private String experimentId;
    private String variantId;
    private long exposures;
    private long clicks;
    private long purchases;
    private BigDecimal revenue = BigDecimal.ZERO;
    private long version;

    public MetricResult() {
    }

    public String getWindowStart() { return windowStart; }
    public void setWindowStart(String windowStart) { this.windowStart = windowStart; }
    public String getWindowEnd() { return windowEnd; }
    public void setWindowEnd(String windowEnd) { this.windowEnd = windowEnd; }
    public String getExperimentId() { return experimentId; }
    public void setExperimentId(String experimentId) { this.experimentId = experimentId; }
    public String getVariantId() { return variantId; }
    public void setVariantId(String variantId) { this.variantId = variantId; }
    public long getExposures() { return exposures; }
    public void setExposures(long exposures) { this.exposures = exposures; }
    public long getClicks() { return clicks; }
    public void setClicks(long clicks) { this.clicks = clicks; }
    public long getPurchases() { return purchases; }
    public void setPurchases(long purchases) { this.purchases = purchases; }
    public BigDecimal getRevenue() { return revenue; }
    public void setRevenue(BigDecimal revenue) { this.revenue = revenue; }
    public long getVersion() { return version; }
    public void setVersion(long version) { this.version = version; }
}
