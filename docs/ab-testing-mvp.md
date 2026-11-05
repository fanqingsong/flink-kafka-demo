# 埋点与 A/B 测试 MVP

这个 MVP 在原销售 Demo 旁边增加了一条独立链路：

```text
浏览器 SDK -> Spring Boot -> Kafka raw -> Flink -> Kafka valid/metrics -> ClickHouse
                           PostgreSQL 保存实验配置
```

## 启动

```shell
docker compose up --build -d
docker compose ps
```

入口：

- A/B 演示：<http://localhost:8088/ab-demo.html>
- 实验 Dashboard：<http://localhost:8088/ab-dashboard.html>
- 原销售 Demo：<http://localhost:8088/>
- Flink UI：<http://localhost:8081/>

数据库：

- PostgreSQL：`localhost:5432/ab_testing`
- ClickHouse HTTP：`localhost:8123`
- ClickHouse Native：`localhost:9000`

首次启动时 Flyway 会创建实验表并写入 `checkout-button` 50/50 演示实验。

## JS SDK

```html
<script src="http://localhost:8088/sdk/ab-sdk.js"></script>
<script>
  AbSdk.init({ apiBase: "http://localhost:8088" });
  const assignment = await AbSdk.getVariant("checkout-button");

  if (assignment.in_experiment) {
    button.textContent = assignment.payload.button_text;
    await AbSdk.track("click", { target: "buy_button" }, "checkout-button");
    await AbSdk.track("purchase", { amount: 99 }, "checkout-button");
  }
</script>
```

`getVariant` 会为当前会话自动发送一次 `exposure`。匿名用户 ID 保存在
`localStorage`，会话 ID 保存在 `sessionStorage`。

## HTTP API

稳定分流：

```shell
curl -X POST http://localhost:8088/api/assignments \
  -H 'Content-Type: application/json' \
  -d '{"experiment_key":"checkout-button","user_id":"user-001"}'
```

单条埋点：

```shell
curl -X POST http://localhost:8088/api/events \
  -H 'Content-Type: application/json' \
  -d '{
    "event_id":"12345678-1234-4234-8234-123456789012",
    "event_name":"exposure",
    "event_time":"2026-11-05T04:30:00Z",
    "user_id":"user-001",
    "session_id":"session-001",
    "experiment_id":"11111111-1111-1111-1111-111111111111",
    "variant_id":"aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
    "properties":{}
  }'
```

`POST /api/events` 也接受最多 100 条事件组成的 JSON 数组。

## 数据口径

- 分流：`SHA-256(experimentKey:userId:salt)`，相同配置下同一用户版本稳定。
- 曝光：同一用户和实验在 7 天归因窗口内只计第一次曝光。
- 点击/购买：必须在最近曝光后 7 天内，且版本相同，才标记为已归因。
- 去重：Flink 按 `event_id` 保留 24 小时状态。
- 事件时间：允许 30 秒乱序；分钟指标使用事件时间滚动窗口。
- CTR：已归因点击 UV / 曝光 UV。
- 转化率：已归因购买 UV / 曝光 UV。
- GMV：已归因购买事件 `properties.amount` 之和。

## Topic

```text
ab.events.raw
ab.events.valid
ab.events.dlq
ab.metrics.1m
```

查看消息：

```shell
docker compose exec kafka /opt/bitnami/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic ab.events.valid --from-beginning
```

查询 ClickHouse：

```shell
docker compose exec clickhouse clickhouse-client --query \
  "SELECT event_name, variant_id, count() FROM analytics.ab_events FINAL GROUP BY event_name, variant_id"
```

## 常见排查

1. Dashboard 没有数据：先确认 Flink UI 中 `Flink AB Event Analytics` 为 RUNNING。
2. 事件进入 DLQ：消费 `ab.events.dlq` 查看 `error` 字段。
3. ClickHouse 暂时无数据：检查 `system.kafka_consumers` 和 Kafka Engine 的消费异常。
4. 修改 Flink 代码后：先在 Flink UI 取消旧作业，再执行
   `docker compose up --build -d event-analytics`。
5. 完全清空演示数据：执行 `docker compose down -v` 后重新启动。
