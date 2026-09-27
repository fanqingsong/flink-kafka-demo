package org.example.web;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.InputStream;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Component
public class Catalog {

    private final List<JsonNode> items;
    private final Map<String, JsonNode> byId;
    private final List<Map<String, Object>> products;

    public Catalog(ObjectMapper objectMapper) throws IOException {
        try (InputStream in = Catalog.class.getResourceAsStream("/catalog.json")) {
            if (in == null) {
                throw new IllegalStateException("catalog.json is missing");
            }
            items = objectMapper.readerForListOf(JsonNode.class).readValue(in);
        }
        byId = new LinkedHashMap<>();
        products = new ArrayList<>();
        for (JsonNode item : items) {
            String productId = item.path("product_id").asText();
            byId.put(productId, item);
            Map<String, Object> view = new LinkedHashMap<>();
            view.put("product_id", productId);
            view.put("item", item.path("item").asText());
            view.put("category", item.path("category").asText());
            view.put("price", item.path("price").decimalValue().setScale(2, RoundingMode.HALF_UP).toPlainString());
            products.add(view);
        }
    }

    public List<JsonNode> items() {
        return items;
    }

    public JsonNode find(String productId) {
        return byId.get(productId);
    }

    public List<Map<String, Object>> products() {
        return products;
    }
}
