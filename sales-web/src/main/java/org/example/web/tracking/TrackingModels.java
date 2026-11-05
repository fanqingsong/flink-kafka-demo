package org.example.web.tracking;

import com.fasterxml.jackson.databind.JsonNode;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

public final class TrackingModels {

    private TrackingModels() {
    }

    public record TrackingEventRequest(
            @NotNull UUID eventId,
            @NotBlank String eventName,
            @NotNull Instant eventTime,
            @NotBlank String userId,
            String sessionId,
            @NotNull UUID experimentId,
            @NotNull UUID variantId,
            JsonNode properties,
            String sdkVersion,
            String pageUrl
    ) {
    }

    public record RawTrackingEvent(
            UUID eventId,
            String eventName,
            Instant eventTime,
            Instant receivedAt,
            String userId,
            String sessionId,
            UUID experimentId,
            UUID variantId,
            JsonNode properties,
            String sdkVersion,
            String pageUrl
    ) {
    }

    public record IngestResponse(int accepted, List<UUID> eventIds) {
    }
}
