package org.example.web;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class PipelineListener {

    private final PipelineState state;
    private final TopicNames topics;
    private final ObjectMapper objectMapper;

    public PipelineListener(PipelineState state, TopicNames topics, ObjectMapper objectMapper) {
        this.state = state;
        this.topics = topics;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(
            id = "pipeline",
            autoStartup = "false",
            topics = {
                    "${app.topics.purchases}",
                    "${app.topics.totals}",
                    "${app.topics.enriched}"
            }
    )
    public void onRecord(ConsumerRecord<String, String> record) {
        state.remember(record.topic(), record.value(), topics, objectMapper);
    }
}
