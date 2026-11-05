package org.example.ab;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class EventAnalyticsJobTest {

    private static final long WINDOW = Duration.ofDays(7).toMillis();

    @Test
    void attributesOnlyMatchingVariantInsideWindow() {
        EventAnalyticsJob.ExposureState exposure =
                new EventAnalyticsJob.ExposureState(1_000L, "variant-a");

        assertTrue(EventAnalyticsJob.isWithinAttribution(exposure, "variant-a", 2_000L, WINDOW));
        assertFalse(EventAnalyticsJob.isWithinAttribution(exposure, "variant-b", 2_000L, WINDOW));
        assertFalse(EventAnalyticsJob.isWithinAttribution(exposure, "variant-a", 999L, WINDOW));
        assertFalse(EventAnalyticsJob.isWithinAttribution(
                exposure, "variant-a", 1_000L + WINDOW + 1, WINDOW));
    }

    @Test
    void marksOnlyFirstExposureWithinAttributionWindow() {
        assertTrue(EventAnalyticsJob.isFirstExposure(null, 1_000L, WINDOW));
        EventAnalyticsJob.ExposureState exposure =
                new EventAnalyticsJob.ExposureState(1_000L, "variant-a");
        assertFalse(EventAnalyticsJob.isFirstExposure(exposure, 2_000L, WINDOW));
        assertTrue(EventAnalyticsJob.isFirstExposure(
                exposure, 1_000L + WINDOW + 1, WINDOW));
    }
}
