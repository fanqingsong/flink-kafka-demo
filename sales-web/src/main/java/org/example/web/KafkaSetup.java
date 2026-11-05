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

    @Bean
    public NewTopic eventsRawTopic(TopicNames topics) {
        return eventTopic(topics.eventsRaw());
    }

    @Bean
    public NewTopic eventsValidTopic(TopicNames topics) {
        return eventTopic(topics.eventsValid());
    }

    @Bean
    public NewTopic eventsDlqTopic(TopicNames topics) {
        return eventTopic(topics.eventsDlq());
    }

    @Bean
    public NewTopic metricsTopic(TopicNames topics) {
        return eventTopic(topics.metrics());
    }

    private static NewTopic topic(String name) {
        return TopicBuilder.name(name).partitions(1).replicas(1).build();
    }

    private static NewTopic eventTopic(String name) {
        return TopicBuilder.name(name).partitions(4).replicas(1).build();
    }
}
