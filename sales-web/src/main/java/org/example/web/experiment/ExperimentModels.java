package org.example.web.experiment;

import com.fasterxml.jackson.databind.JsonNode;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

public final class ExperimentModels {

    private ExperimentModels() {
    }

    public enum Status {
        DRAFT, RUNNING, PAUSED
    }

    public record Variant(
            UUID id,
            String key,
            String name,
            int weight,
            JsonNode payload
    ) {
    }

    public record Experiment(
            UUID id,
            String key,
            String name,
            Status status,
            String salt,
            int trafficPercent,
            Instant createdAt,
            Instant updatedAt,
            List<Variant> variants
    ) {
    }

    public record VariantInput(
            @NotBlank String key,
            @NotBlank String name,
            @Min(1) @Max(100) int weight,
            JsonNode payload
    ) {
    }

    public record ExperimentRequest(
            @NotBlank String key,
            @NotBlank String name,
            String salt,
            @Min(0) @Max(100) int trafficPercent,
            @NotEmpty List<@Valid VariantInput> variants
    ) {
    }

    public record AssignmentRequest(
            @NotBlank String experimentKey,
            @NotBlank String userId
    ) {
    }

    public record AssignmentResponse(
            boolean inExperiment,
            UUID experimentId,
            UUID variantId,
            String variantKey,
            JsonNode payload
    ) {
        public static AssignmentResponse excluded(UUID experimentId) {
            return new AssignmentResponse(false, experimentId, null, null, null);
        }
    }
}
