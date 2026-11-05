package org.example.web.tracking;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validator;
import org.example.web.ApiException;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.example.web.tracking.TrackingModels.IngestResponse;
import static org.example.web.tracking.TrackingModels.TrackingEventRequest;

@RestController
public class TrackingController {

    private final TrackingService tracking;
    private final ObjectMapper objectMapper;
    private final Validator validator;

    public TrackingController(TrackingService tracking, ObjectMapper objectMapper, Validator validator) {
        this.tracking = tracking;
        this.objectMapper = objectMapper;
        this.validator = validator;
    }

    @PostMapping("/api/events")
    @ResponseStatus(HttpStatus.ACCEPTED)
    public IngestResponse ingest(@RequestBody JsonNode body) {
        List<TrackingEventRequest> requests = new ArrayList<>();
        if (body.isArray()) {
            body.forEach(node -> requests.add(parse(node)));
        } else if (body.isObject()) {
            requests.add(parse(body));
        } else {
            throw new ApiException(400, "请求必须是事件对象或事件数组");
        }
        return tracking.ingest(requests);
    }

    private TrackingEventRequest parse(JsonNode node) {
        try {
            TrackingEventRequest request = objectMapper.treeToValue(node, TrackingEventRequest.class);
            Set<ConstraintViolation<TrackingEventRequest>> violations = validator.validate(request);
            if (!violations.isEmpty()) {
                String detail = violations.iterator().next().getPropertyPath() + " "
                        + violations.iterator().next().getMessage();
                throw new ApiException(400, detail);
            }
            return request;
        } catch (ApiException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new ApiException(400, "事件 JSON 格式无效");
        }
    }
}
