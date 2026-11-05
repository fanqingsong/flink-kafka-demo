package org.example.web.experiment;

import org.example.web.ApiException;
import org.springframework.stereotype.Service;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import static org.example.web.experiment.ExperimentModels.AssignmentResponse;
import static org.example.web.experiment.ExperimentModels.Experiment;

@Service
public class AssignmentService {

    private final ExperimentService experiments;

    public AssignmentService(ExperimentService experiments) {
        this.experiments = experiments;
    }

    public AssignmentResponse assign(String experimentKey, String userId) {
        Experiment experiment = experiments.getRunning(experimentKey);
        int bucket = bucket(experiment.key(), userId, experiment.salt(), 10_000);
        if (bucket >= experiment.trafficPercent() * 100) {
            return AssignmentResponse.excluded(experiment.id());
        }

        int variantBucket = bucket % 100;
        int upperBound = 0;
        for (ExperimentModels.Variant variant : experiment.variants()) {
            upperBound += variant.weight();
            if (variantBucket < upperBound) {
                return new AssignmentResponse(
                        true,
                        experiment.id(),
                        variant.id(),
                        variant.key(),
                        variant.payload()
                );
            }
        }
        throw new ApiException(500, "实验版本权重配置无效");
    }

    static int bucket(String experimentKey, String userId, String salt, int bound) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(
                    (experimentKey + ":" + userId + ":" + salt).getBytes(StandardCharsets.UTF_8)
            );
            long firstEightBytes = ByteBuffer.wrap(hash).getLong();
            return (int) Long.remainderUnsigned(firstEightBytes, bound);
        } catch (NoSuchAlgorithmException ex) {
            throw new IllegalStateException("SHA-256 is unavailable", ex);
        }
    }
}
