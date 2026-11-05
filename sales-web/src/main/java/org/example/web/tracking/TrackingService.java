package org.example.web.tracking;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.example.web.ApiException;
import org.example.web.TopicNames;
import org.example.web.experiment.ExperimentModels;
import org.example.web.experiment.ExperimentService;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.example.web.tracking.TrackingModels.IngestResponse;
import static org.example.web.tracking.TrackingModels.RawTrackingEvent;
import static org.example.web.tracking.TrackingModels.TrackingEventRequest;

@Service
public class TrackingService {

    private static final Set<String> STANDARD_EVENTS = Set.of("exposure", "click", "purchase");

    private final ExperimentService experiments;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final TopicNames topics;
    private final ObjectMapper objectMapper;

    public TrackingService(ExperimentService experiments,
                           KafkaTemplate<String, String> kafkaTemplate,
                           TopicNames topics,
                           ObjectMapper objectMapper) {
        this.experiments = experiments;
        this.kafkaTemplate = kafkaTemplate;
        this.topics = topics;
        this.objectMapper = objectMapper;
    }

    public IngestResponse ingest(List<TrackingEventRequest> requests) {
        if (requests.isEmpty()) {
            throw new ApiException(400, "至少需要一条事件");
        }
        if (requests.size() > 100) {
            throw new ApiException(400, "单次最多上报 100 条事件");
        }
        for (TrackingEventRequest request : requests) {
            ingestOne(request);
        }
        return new IngestResponse(
                requests.size(),
                requests.stream().map(TrackingEventRequest::eventId).toList()
        );
    }

    private void ingestOne(TrackingEventRequest request) {
        if (!STANDARD_EVENTS.contains(request.eventName())) {
            throw new ApiException(400, "不支持的标准事件: " + request.eventName());
        }
        ExperimentModels.Experiment experiment = experiments.get(request.experimentId());
        if (experiment.status() != ExperimentModels.Status.RUNNING) {
            throw new ApiException(409, "实验当前未运行");
        }
        boolean validVariant = experiment.variants().stream()
                .anyMatch(variant -> variant.id().equals(request.variantId()));
        if (!validVariant) {
            throw new ApiException(400, "版本不属于指定实验");
        }

        RawTrackingEvent event = new RawTrackingEvent(
                request.eventId(),
                request.eventName(),
                request.eventTime(),
                Instant.now(),
                request.userId(),
                blankToEmpty(request.sessionId()),
                request.experimentId(),
                request.variantId(),
                request.properties() == null ? objectMapper.createObjectNode() : request.properties(),
                blankToEmpty(request.sdkVersion()),
                blankToEmpty(request.pageUrl())
        );
        try {
            String json = objectMapper.writeValueAsString(event);
            kafkaTemplate.send(topics.eventsRaw(), request.userId(), json).get(10, TimeUnit.SECONDS);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new ApiException(503, "埋点写入被中断");
        } catch (Exception ex) {
            throw new ApiException(503, "埋点暂时不可用");
        }
    }

    private static String blankToEmpty(String value) {
        return value == null ? "" : value;
    }
}
