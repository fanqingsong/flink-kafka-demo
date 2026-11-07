package org.example.web;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

@SpringBootApplication
@EnableConfigurationProperties(TopicNames.class)
public class SalesWebApplication {

    public static void main(String[] args) {
        SpringApplication.run(SalesWebApplication.class, args);
    }
}
