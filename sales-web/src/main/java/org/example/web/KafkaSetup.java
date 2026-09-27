package org.example.web;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaSetup {

    @Bean
    public NewTopic productsTopic(TopicNames topics) {
        return topic(topics.products());
    }

    @Bean
    public NewTopic purchasesTopic(TopicNames topics) {
        return topic(topics.purchases());
    }

    @Bean
    public NewTopic totalsTopic(TopicNames topics) {
        return topic(topics.totals());
    }

    @Bean
    public NewTopic enrichedTopic(TopicNames topics) {
        return topic(topics.enriched());
    }

    private static NewTopic topic(String name) {
        return TopicBuilder.name(name).partitions(1).replicas(1).build();
    }
}
