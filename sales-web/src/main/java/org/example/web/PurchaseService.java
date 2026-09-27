package org.example.web;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

@Service
public class PurchaseService {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final Catalog catalog;
    private final TopicNames topics;
    private final PipelineState state;

    public PurchaseService(KafkaTemplate<String, String> kafkaTemplate,
                           ObjectMapper objectMapper,
                           Catalog catalog,
                           TopicNames topics,
                           PipelineState state) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
        this.catalog = catalog;
        this.topics = topics;
        this.state = state;
    }

    public Map<String, Object> send(PurchaseRequest request) {
        PurchaseMessages.checkQuantity(request.getQuantity());
        JsonNode product = catalog.find(request.getProductId());
        if (product == null) {
            throw new ApiException(400, "unknown product_id");
        }
        Instant now = Instant.now();
        Map<String, Object> record = PurchaseMessages.build(
                product.path("product_id").asText(),
                product.path("item").asText(),
                product.path("price").decimalValue(),
                request.getQuantity(),
                request.isMember(),
                now,
                PurchaseMessages.transactionId(now, UUID.randomUUID()));
        Map<String, Object> payload = new LinkedHashMap<>(record);
        payload.remove("product_name");
        try {
            kafkaTemplate.send(topics.purchases(), objectMapper.writeValueAsString(payload))
                    .get(10, TimeUnit.SECONDS);
        } catch (Exception ex) {
            if (ex instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            Throwable shown = ex.getCause() == null ? ex : ex.getCause();
            throw new ApiException(503, "kafka produce failed: " + shown);
        }
        state.incrementSent();
        return record;
    }
}
