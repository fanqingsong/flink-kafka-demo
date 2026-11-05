package org.example.web.analytics;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "app.clickhouse")
public record ClickHouseProperties(String url, String username, String password) {
}
