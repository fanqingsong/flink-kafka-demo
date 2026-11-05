package org.example.ab;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

public class EventAnalyticsJob {

    private static final Set<String> EVENT_NAMES = Set.of("exposure", "click", "purchase");
    private static final DateTimeFormatter CLICKHOUSE_TIME =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS").withZone(ZoneOffset.UTC);
    private static final OutputTag<String> DLQ = new OutputTag<String>("ab-events-dlq") { };

    public static void main(String[] args) throws Exception {
        Properties properties = loadProperties();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(30_000);
        env.getCheckpointConfig().setCheckpointingMode(
                org.apache.flink.streaming.api.CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(10_000);
        env.getCheckpointConfig().setCheckpointTimeout(60_000);
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(properties.getProperty("BOOTSTRAP_SERVERS"))
                .setTopics(properties.getProperty("AB_EVENTS_RAW_TOPIC"))
                .setGroupId("flink-ab-event-analytics-v1")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        SingleOutputStreamOperator<TrackingEvent> parsed = env
                .fromSource(source, WatermarkStrategy.noWatermarks(), "AB Raw Events")
                .process(new ParseEventFunction());

        SingleOutputStreamOperator<TrackingEvent> timed = parsed
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<TrackingEvent>forBoundedOutOfOrderness(Duration.ofSeconds(30))
                                .withTimestampAssigner((event, previous) -> event.getEventTimestamp())
                                .withIdleness(Duration.ofMinutes(1))
                );

        SingleOutputStreamOperator<TrackingEvent> deduplicated = timed
                .keyBy(TrackingEvent::getEventId)
                .process(new DeduplicateFunction());

        SingleOutputStreamOperator<TrackingEvent> attributed = deduplicated
                .keyBy(event -> event.getUserId() + "|" + event.getExperimentId())
                .process(new AttributionFunction(Duration.ofDays(7).toMillis()));

        attributed.sinkTo(eventSink(
                properties,
                properties.getProperty("AB_EVENTS_VALID_TOPIC"),
                "ab-valid-",
                new TrackingEventSerializationSchema()
        )).name("Valid Events");

        parsed.getSideOutput(DLQ).sinkTo(eventSink(
                properties,
                properties.getProperty("AB_EVENTS_DLQ_TOPIC"),
                "ab-dlq-",
                new SimpleStringSchema()
        )).name("Invalid Events");

        DataStream<MetricResult> metrics = attributed
                .filter((FilterFunction<TrackingEvent>) event -> event.getAttributed() == 1)
                .keyBy(event -> event.getExperimentId() + "|" + event.getVariantId())
                .window(TumblingEventTimeWindows.of(
                        org.apache.flink.streaming.api.windowing.time.Time.minutes(1)))
                .process(new MetricWindowFunction());

        metrics.sinkTo(eventSink(
                properties,
                properties.getProperty("AB_METRICS_TOPIC"),
                "ab-metrics-",
                new MetricSerializationSchema()
        )).name("Minute Metrics");

        env.execute("Flink AB Event Analytics");
    }

    private static Properties loadProperties() {
        Properties properties = new Properties();
        try (InputStream input = EventAnalyticsJob.class.getClassLoader()
                .getResourceAsStream("config.properties")) {
            if (input == null) {
                throw new IllegalStateException("config.properties not found");
            }
            properties.load(input);
        } catch (IOException ex) {
            throw new IllegalStateException("cannot load config.properties", ex);
        }
        String bootstrap = System.getenv("BOOTSTRAP_SERVERS");
        if (bootstrap != null && !bootstrap.isBlank()) {
            properties.setProperty("BOOTSTRAP_SERVERS", bootstrap);
        }
        return properties;
    }

    private static <T> KafkaSink<T> eventSink(
            Properties properties,
            String topic,
            String transactionPrefix,
            SerializationSchema<T> serializer) {
        return KafkaSink.<T>builder()
                .setBootstrapServers(properties.getProperty("BOOTSTRAP_SERVERS"))
                .setRecordSerializer(KafkaRecordSerializationSchema.<T>builder()
                        .setTopic(topic)
                        .setValueSerializationSchema(serializer)
                        .build())
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .setTransactionalIdPrefix(transactionPrefix)
                .build();
    }

    static class ParseEventFunction extends ProcessFunction<String, TrackingEvent> {
        private transient ObjectMapper mapper;

        @Override
        public void open(Configuration parameters) {
            mapper = new ObjectMapper();
        }

        @Override
        public void processElement(String raw, Context context, Collector<TrackingEvent> out) {
            try {
                JsonNode node = mapper.readTree(raw);
                String eventId = required(node, "event_id");
                String eventName = required(node, "event_name");
                String eventTime = required(node, "event_time");
                String receivedAt = required(node, "received_at");
                String userId = required(node, "user_id");
                String experimentId = required(node, "experiment_id");
                String variantId = required(node, "variant_id");
                if (!EVENT_NAMES.contains(eventName)) {
                    throw new IllegalArgumentException("unsupported event_name");
                }
                UUID.fromString(eventId);
                UUID.fromString(experimentId);
                UUID.fromString(variantId);
                Instant eventInstant = Instant.parse(eventTime);
                Instant receivedInstant = Instant.parse(receivedAt);

                TrackingEvent event = new TrackingEvent();
                event.setEventId(eventId);
                event.setEventName(eventName);
                event.setEventTimestamp(eventInstant.toEpochMilli());
                event.setEventTime(CLICKHOUSE_TIME.format(eventInstant));
                event.setReceivedAt(CLICKHOUSE_TIME.format(receivedInstant));
                event.setUserId(userId);
                event.setSessionId(node.path("session_id").asText(""));
                event.setExperimentId(experimentId);
                event.setVariantId(variantId);
                event.setProperties(node.path("properties").toString());
                event.setVersion(receivedInstant.toEpochMilli());
                JsonNode amount = node.path("properties").path("amount");
                if (amount.isNumber() || amount.isTextual()) {
                    event.setRevenue(new BigDecimal(amount.asText()));
                }
                out.collect(event);
            } catch (Exception ex) {
                ObjectNode invalid = mapper.createObjectNode();
                invalid.put("raw", raw);
                invalid.put("error", ex.getMessage() == null ? ex.getClass().getSimpleName() : ex.getMessage());
                invalid.put("failed_at", Instant.now().toString());
                context.output(DLQ, invalid.toString());
            }
        }

        private static String required(JsonNode node, String field) {
            String value = node.path(field).asText("");
            if (value.isBlank()) {
                throw new IllegalArgumentException(field + " is required");
            }
            return value;
        }
    }

    static class DeduplicateFunction extends KeyedProcessFunction<String, TrackingEvent, TrackingEvent> {
        private transient ValueState<Boolean> seen;

        @Override
        public void open(Configuration parameters) {
            ValueStateDescriptor<Boolean> descriptor =
                    new ValueStateDescriptor<>("seen-event-id", Boolean.class);
            descriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.hours(24)).build());
            seen = getRuntimeContext().getState(descriptor);
        }

        @Override
        public void processElement(TrackingEvent event, Context context, Collector<TrackingEvent> out)
                throws Exception {
            if (!Boolean.TRUE.equals(seen.value())) {
                seen.update(Boolean.TRUE);
                out.collect(event);
            }
        }
    }

    static class AttributionFunction
            extends KeyedProcessFunction<String, TrackingEvent, TrackingEvent> {
        private final long attributionWindowMillis;
        private transient ValueState<ExposureState> exposure;

        AttributionFunction(long attributionWindowMillis) {
            this.attributionWindowMillis = attributionWindowMillis;
        }

        @Override
        public void open(Configuration parameters) {
            ValueStateDescriptor<ExposureState> descriptor =
                    new ValueStateDescriptor<>("latest-exposure", ExposureState.class);
            descriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.days(8)).build());
            exposure = getRuntimeContext().getState(descriptor);
        }

        @Override
        public void processElement(TrackingEvent event, Context context, Collector<TrackingEvent> out)
                throws Exception {
            ExposureState current = exposure.value();
            if ("exposure".equals(event.getEventName())) {
                boolean firstInWindow = isFirstExposure(
                        current, event.getEventTimestamp(), attributionWindowMillis);
                event.setAttributed(firstInWindow ? 1 : 0);
                exposure.update(new ExposureState(event.getEventTimestamp(), event.getVariantId()));
            } else {
                boolean inWindow = isWithinAttribution(
                        current,
                        event.getVariantId(),
                        event.getEventTimestamp(),
                        attributionWindowMillis
                );
                event.setAttributed(inWindow ? 1 : 0);
            }
            out.collect(event);
        }
    }

    static boolean isFirstExposure(ExposureState current, long eventTimestamp, long windowMillis) {
        return current == null || eventTimestamp - current.timestamp > windowMillis;
    }

    static boolean isWithinAttribution(
            ExposureState current,
            String variantId,
            long eventTimestamp,
            long windowMillis) {
        return current != null
                && current.variantId.equals(variantId)
                && eventTimestamp >= current.timestamp
                && eventTimestamp - current.timestamp <= windowMillis;
    }

    public static class ExposureState implements Serializable {
        public long timestamp;
        public String variantId;

        public ExposureState() {
        }

        ExposureState(long timestamp, String variantId) {
            this.timestamp = timestamp;
            this.variantId = variantId;
        }
    }

    static class MetricWindowFunction
            extends ProcessWindowFunction<TrackingEvent, MetricResult, String, TimeWindow> {
        @Override
        public void process(
                String key,
                Context context,
                Iterable<TrackingEvent> events,
                Collector<MetricResult> out) {
            MetricResult result = new MetricResult();
            result.setWindowStart(CLICKHOUSE_TIME.format(Instant.ofEpochMilli(context.window().getStart())));
            result.setWindowEnd(CLICKHOUSE_TIME.format(Instant.ofEpochMilli(context.window().getEnd())));
            result.setVersion(context.window().getEnd());
            BigDecimal revenue = BigDecimal.ZERO;
            for (TrackingEvent event : events) {
                result.setExperimentId(event.getExperimentId());
                result.setVariantId(event.getVariantId());
                if ("exposure".equals(event.getEventName())) {
                    result.setExposures(result.getExposures() + 1);
                } else if ("click".equals(event.getEventName())) {
                    result.setClicks(result.getClicks() + 1);
                } else if ("purchase".equals(event.getEventName())) {
                    result.setPurchases(result.getPurchases() + 1);
                    revenue = revenue.add(event.getRevenue());
                }
            }
            result.setRevenue(revenue);
            out.collect(result);
        }
    }

    static class TrackingEventSerializationSchema implements SerializationSchema<TrackingEvent> {
        private transient ObjectMapper mapper;

        @Override
        public byte[] serialize(TrackingEvent event) {
            if (mapper == null) {
                mapper = new ObjectMapper();
            }
            ObjectNode node = mapper.createObjectNode();
            node.put("event_id", event.getEventId());
            node.put("event_name", event.getEventName());
            node.put("event_time", event.getEventTime());
            node.put("received_at", event.getReceivedAt());
            node.put("user_id", event.getUserId());
            node.put("session_id", event.getSessionId());
            node.put("experiment_id", event.getExperimentId());
            node.put("variant_id", event.getVariantId());
            node.put("properties", event.getProperties());
            node.put("attributed", event.getAttributed());
            node.put("version", event.getVersion());
            return node.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8);
        }
    }

    static class MetricSerializationSchema implements SerializationSchema<MetricResult> {
        private transient ObjectMapper mapper;

        @Override
        public byte[] serialize(MetricResult metric) {
            if (mapper == null) {
                mapper = new ObjectMapper();
            }
            ObjectNode node = mapper.createObjectNode();
            node.put("window_start", metric.getWindowStart());
            node.put("window_end", metric.getWindowEnd());
            node.put("experiment_id", metric.getExperimentId());
            node.put("variant_id", metric.getVariantId());
            node.put("exposures", metric.getExposures());
            node.put("clicks", metric.getClicks());
            node.put("purchases", metric.getPurchases());
            node.put("revenue", metric.getRevenue());
            node.put("version", metric.getVersion());
            return node.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8);
        }
    }
}
