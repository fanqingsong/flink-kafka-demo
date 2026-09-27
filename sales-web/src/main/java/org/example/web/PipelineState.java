package org.example.web;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Component;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Component
public class PipelineState {

    private static final int RECENT_LIMIT = 40;

    private final Object lock = new Object();
    private final Map<String, JsonNode> totals = new HashMap<>();
    private final Deque<JsonNode> purchases = new ArrayDeque<>();
    private final Deque<JsonNode> enriched = new ArrayDeque<>();
    private boolean ready;
    private String error;
    private int sessionSent;

    public void remember(String topic, String json, TopicNames topics, ObjectMapper objectMapper) {
        JsonNode node;
        try {
            node = objectMapper.readTree(json);
        } catch (Exception ex) {
            return;
        }
        if (node == null || !node.isObject()) {
            return;
        }
        synchronized (lock) {
            if (topics.totals().equals(topic)) {
                JsonNode productId = node.get("product_id");
                if (productId != null && !productId.asText().isBlank()) {
                    totals.put(productId.asText(), node);
                }
            } else if (topics.enriched().equals(topic)) {
                push(enriched, node);
            } else if (topics.purchases().equals(topic)) {
                push(purchases, node);
            }
        }
    }

    public void markReady() {
        synchronized (lock) {
            ready = true;
            error = null;
        }
    }

    public void incrementSent() {
        synchronized (lock) {
            sessionSent++;
        }
    }

    public Map<String, Object> snapshot(Catalog catalog) {
        synchronized (lock) {
            List<Map<String, Object>> totalRows = new ArrayList<>();
            for (Map.Entry<String, JsonNode> entry : totals.entrySet()) {
                JsonNode row = entry.getValue();
                JsonNode product = catalog.find(entry.getKey());
                Map<String, Object> view = new LinkedHashMap<>();
                view.put("product_id", entry.getKey());
                view.put("product_name", product == null ? "" : product.path("item").asText(""));
                view.put("category", product == null ? "" : product.path("category").asText(""));
                view.put("transactions", row.get("transactions"));
                view.put("quantities", row.get("quantities"));
                view.put("sales", row.get("sales"));
                view.put("event_time", row.get("event_time"));
                totalRows.add(view);
            }
            totalRows.sort(Comparator.comparing(row -> (String) row.get("product_id")));

            Map<String, Object> body = new LinkedHashMap<>();
            body.put("ready", ready);
            body.put("error", error);
            body.put("session_sent", sessionSent);
            body.put("products", catalog.products());
            body.put("totals", totalRows);
            body.put("purchases", new ArrayList<>(purchases));
            body.put("enriched", new ArrayList<>(enriched));
            return body;
        }
    }

    private static void push(Deque<JsonNode> deque, JsonNode node) {
        deque.addFirst(node);
        while (deque.size() > RECENT_LIMIT) {
            deque.removeLast();
        }
    }
}
