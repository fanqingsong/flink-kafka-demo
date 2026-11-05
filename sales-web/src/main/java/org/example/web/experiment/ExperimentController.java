package org.example.web.experiment;

import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.UUID;

import static org.example.web.experiment.ExperimentModels.AssignmentRequest;
import static org.example.web.experiment.ExperimentModels.AssignmentResponse;
import static org.example.web.experiment.ExperimentModels.Experiment;
import static org.example.web.experiment.ExperimentModels.ExperimentRequest;

@RestController
@RequestMapping("/api")
public class ExperimentController {

    private final ExperimentService experiments;
    private final AssignmentService assignments;

    public ExperimentController(ExperimentService experiments, AssignmentService assignments) {
        this.experiments = experiments;
        this.assignments = assignments;
    }

    @GetMapping("/experiments")
    public List<Experiment> list() {
        return experiments.list();
    }

    @PostMapping("/experiments")
    @ResponseStatus(HttpStatus.CREATED)
    public Experiment create(@Valid @RequestBody ExperimentRequest request) {
        return experiments.create(request);
    }

    @PutMapping("/experiments/{id}")
    public Experiment update(@PathVariable("id") UUID id, @Valid @RequestBody ExperimentRequest request) {
        return experiments.update(id, request);
    }

    @PostMapping("/experiments/{id}/start")
    public Experiment start(@PathVariable("id") UUID id) {
        return experiments.start(id);
    }

    @PostMapping("/experiments/{id}/pause")
    public Experiment pause(@PathVariable("id") UUID id) {
        return experiments.pause(id);
    }

    @PostMapping("/assignments")
    public AssignmentResponse assign(@Valid @RequestBody AssignmentRequest request) {
        return assignments.assign(request.experimentKey(), request.userId());
    }
}
