package org.example.web;

import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Testcontainers
class PurchasePipelineTest {

    private static final int KAFKA_PORT = 19092;

    @Container
    static final GenericContainer<?> KAFKA = new GenericContainer<>(DockerImageName.parse("apache/kafka:3.8.0"))
            .withExposedPorts(9092)
            .withEnv("KAFKA_NODE_ID", "1")
            .withEnv("KAFKA_PROCESS_ROLES", "broker,controller")
            .withEnv("KAFKA_LISTENERS", "PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093")
            .withEnv("KAFKA_ADVERTISED_LISTENERS", "PLAINTEXT://127.0.0.1:" + KAFKA_PORT)
            .withEnv("KAFKA_CONTROLLER_LISTENER_NAMES", "CONTROLLER")
            .withEnv("KAFKA_LISTENER_SECURITY_PROTOCOL_MAP", "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT")
            .withEnv("KAFKA_CONTROLLER_QUORUM_VOTERS", "1@127.0.0.1:9093")
            .withEnv("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1")
            .withEnv("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1")
            .withEnv("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1")
            .withEnv("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "0")
            .withEnv("CLUSTER_ID", "MkU3OEVBNTcwNTJENDM2Qk")
            .waitingFor(Wait.forLogMessage(".*Kafka Server started.*", 1)
                    .withStartupTimeout(Duration.ofMinutes(2)));

    static {
        KAFKA.setPortBindings(List.of(KAFKA_PORT + ":9092"));
        KAFKA.start();
    }

    @Autowired
    private TestRestTemplate rest;

    @DynamicPropertySource
    static void kafka(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", () -> "127.0.0.1:" + KAFKA_PORT);
    }

    @Test
    void sentPurchaseShowsUpInState() throws Exception {
        ResponseEntity<JsonNode> sent = rest.postForEntity(
                "/api/purchases",
                Map.of("product_id", "SC04", "quantity", 1, "is_member", false),
                JsonNode.class);

        assertEquals(HttpStatus.OK, sent.getStatusCode());
        JsonNode body = sent.getBody();
        assertEquals("Health Nut", body.get("product_name").asText());
        String transactionId = body.get("transaction_id").asText();

        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
        while (System.nanoTime() < deadline) {
            JsonNode state = rest.getForObject("/api/state", JsonNode.class);
            for (JsonNode purchase : state.get("purchases")) {
                if (transactionId.equals(purchase.path("transaction_id").asText())) {
                    assertEquals("SC04", purchase.path("product_id").asText());
                    assertTrue(state.path("session_sent").asInt() >= 1);
                    assertTrue(state.get("products").size() >= 4);
                    return;
                }
            }
            Thread.sleep(200);
        }
        fail("purchase " + transactionId + " did not appear in /api/state");
    }
}
