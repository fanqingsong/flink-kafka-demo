package org.example.web;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.stereotype.Component;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Component
public class PipelineBootstrap implements org.springframework.context.SmartLifecycle {

    static final int PHASE = Integer.MAX_VALUE - 4096;

    private static final Logger log = LoggerFactory.getLogger(PipelineBootstrap.class);

    private final KafkaAdmin kafkaAdmin;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final Catalog catalog;
    private final TopicNames topics;
    private final KafkaListenerEndpointRegistry registry;
    private final PipelineState state;
    private boolean running;

    public PipelineBootstrap(KafkaAdmin kafkaAdmin,
                             KafkaTemplate<String, String> kafkaTemplate,
                             ObjectMapper objectMapper,
                             Catalog catalog,
                             TopicNames topics,
                             KafkaListenerEndpointRegistry registry,
                             PipelineState state) {
        this.kafkaAdmin = kafkaAdmin;
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
        this.catalog = catalog;
        this.topics = topics;
        this.registry = registry;
        this.state = state;
    }

    @Override
    public void start() {
        prepare();
        MessageListenerContainer container = registry.getListenerContainer("pipeline");
        if (container == null) {
            throw new IllegalStateException("pipeline listener is not registered");
        }
        container.start();
        state.markReady();
        running = true;
    }

    @Override
    public void stop() {
        running = false;
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    @Override
    public int getPhase() {
        return PHASE;
    }

    private void prepare() {
        RuntimeException last = null;
        for (int attempt = 0; attempt < 30; attempt++) {
            try {
                seedIfEmpty();
                return;
            } catch (RuntimeException ex) {
                last = ex;
                log.warn("kafka setup attempt {} failed: {}", attempt + 1, ex.toString());
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException("kafka setup interrupted", interrupted);
                }
            }
        }
        throw new IllegalStateException("kafka setup failed: " + last, last);
    }

    private void seedIfEmpty() {
        long endOffset = endOffset(topics.products());
        if (endOffset > 0) {
            log.info("catalog already present on {}", topics.products());
            return;
        }
        for (JsonNode product : catalog.items()) {
            try {
                kafkaTemplate.send(topics.products(), objectMapper.writeValueAsString(product))
                        .get(10, TimeUnit.SECONDS);
            } catch (Exception ex) {
                if (ex instanceof InterruptedException) {
                    Thread.currentThread().interrupt();
                }
                throw new IllegalStateException(ex);
            }
        }
        log.info("seeded {} products into {}", catalog.items().size(), topics.products());
    }

    private long endOffset(String topic) {
        try (AdminClient admin = AdminClient.create(kafkaAdmin.getConfigurationProperties())) {
            TopicDescription description = admin.describeTopics(List.of(topic))
                    .topicNameValues()
                    .get(topic)
                    .get(10, TimeUnit.SECONDS);
            Map<TopicPartition, OffsetSpec> request = new LinkedHashMap<>();
            for (TopicPartitionInfo partition : description.partitions()) {
                request.put(new TopicPartition(topic, partition.partition()), OffsetSpec.latest());
            }
            if (request.isEmpty()) {
                throw new IllegalStateException(topic + " has no partitions yet");
            }
            ListOffsetsResult offsets = admin.listOffsets(request);
            long end = 0;
            for (TopicPartition partition : request.keySet()) {
                end += offsets.partitionResult(partition).get(10, TimeUnit.SECONDS).offset();
            }
            return end;
        } catch (RuntimeException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new IllegalStateException(ex);
        }
    }
}
