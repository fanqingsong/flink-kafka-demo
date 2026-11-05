package org.example.web.experiment;

import org.example.web.ApiException;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.HashSet;
import java.util.List;
import java.util.UUID;

import static org.example.web.experiment.ExperimentModels.Experiment;
import static org.example.web.experiment.ExperimentModels.ExperimentRequest;
import static org.example.web.experiment.ExperimentModels.Status;

@Service
public class ExperimentService {

    private final ExperimentRepository repository;

    public ExperimentService(ExperimentRepository repository) {
        this.repository = repository;
    }

    public List<Experiment> list() {
        return repository.findAll();
    }

    public Experiment get(UUID id) {
        return repository.findById(id)
                .orElseThrow(() -> new ApiException(404, "实验不存在"));
    }

    public Experiment getRunning(String key) {
        return repository.findRunningByKey(key)
                .orElseThrow(() -> new ApiException(404, "运行中的实验不存在"));
    }

    @Transactional
    public Experiment create(ExperimentRequest request) {
        validate(request);
        String salt = hasText(request.salt()) ? request.salt() : UUID.randomUUID().toString();
        try {
            return repository.insert(UUID.randomUUID(), request, salt);
        } catch (DataIntegrityViolationException ex) {
            throw new ApiException(409, "实验 key 或版本 key 已存在");
        }
    }

    @Transactional
    public Experiment update(UUID id, ExperimentRequest request) {
        validate(request);
        Experiment current = get(id);
        String salt = hasText(request.salt()) ? request.salt() : current.salt();
        try {
            return repository.update(id, request, salt);
        } catch (DataIntegrityViolationException ex) {
            throw new ApiException(409, "实验 key 或版本 key 已存在");
        }
    }

    @Transactional
    public Experiment start(UUID id) {
        Experiment experiment = get(id);
        validateWeights(experiment.variants().stream()
                .map(variant -> new ExperimentModels.VariantInput(
                        variant.key(), variant.name(), variant.weight(), variant.payload()))
                .toList());
        return repository.setStatus(id, Status.RUNNING).orElseThrow();
    }

    @Transactional
    public Experiment pause(UUID id) {
        get(id);
        return repository.setStatus(id, Status.PAUSED).orElseThrow();
    }

    private static void validate(ExperimentRequest request) {
        validateWeights(request.variants());
    }

    private static void validateWeights(List<ExperimentModels.VariantInput> variants) {
        int total = variants.stream().mapToInt(ExperimentModels.VariantInput::weight).sum();
        if (total != 100) {
            throw new ApiException(400, "版本权重之和必须为 100");
        }
        HashSet<String> keys = new HashSet<>();
        if (variants.stream().anyMatch(variant -> !keys.add(variant.key()))) {
            throw new ApiException(400, "版本 key 不能重复");
        }
    }

    private static boolean hasText(String value) {
        return value != null && !value.isBlank();
    }
}
