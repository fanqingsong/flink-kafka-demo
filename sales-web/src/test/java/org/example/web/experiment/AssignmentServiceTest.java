package org.example.web.experiment;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class AssignmentServiceTest {

    private static final UUID EXPERIMENT_ID = UUID.fromString("11111111-1111-1111-1111-111111111111");
    private static final UUID CONTROL_ID = UUID.fromString("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa");
    private static final UUID TREATMENT_ID = UUID.fromString("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb");

    @Test
    void assignmentIsStableAndUsesBothVariants() {
        ExperimentService experiments = mock(ExperimentService.class);
        ObjectMapper mapper = new ObjectMapper();
        ExperimentModels.Experiment experiment = new ExperimentModels.Experiment(
                EXPERIMENT_ID,
                "checkout-button",
                "Checkout button",
                ExperimentModels.Status.RUNNING,
                "fixed-salt",
                100,
                Instant.EPOCH,
                Instant.EPOCH,
                List.of(
                        new ExperimentModels.Variant(
                                CONTROL_ID, "control", "Control", 50, mapper.createObjectNode()),
                        new ExperimentModels.Variant(
                                TREATMENT_ID, "treatment", "Treatment", 50, mapper.createObjectNode())
                )
        );
        when(experiments.getRunning("checkout-button")).thenReturn(experiment);
        AssignmentService service = new AssignmentService(experiments);

        ExperimentModels.AssignmentResponse first = service.assign("checkout-button", "user-42");
        assertEquals(first, service.assign("checkout-button", "user-42"));

        Set<String> variants = new HashSet<>();
        for (int i = 0; i < 200; i++) {
            variants.add(service.assign("checkout-button", "user-" + i).variantKey());
        }
        assertEquals(Set.of("control", "treatment"), variants);
    }

    @Test
    void bucketIsDeterministicAndWithinBounds() {
        int first = AssignmentService.bucket("experiment", "user", "salt", 10_000);
        assertEquals(first, AssignmentService.bucket("experiment", "user", "salt", 10_000));
        assertTrue(first >= 0 && first < 10_000);
    }
}
