package br.edu.bigdata;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.time.Instant;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class TrendTopicsJob {
    private static final ObjectMapper JSON = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("kafka:9092")
                .setTopics("events")
                .setGroupId("flink-trend-topics-java")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        WatermarkStrategy<Event> watermarks = WatermarkStrategy
                .<Event>forBoundedOutOfOrderness(Duration.ofSeconds(30))
                .withTimestampAssigner((event, ignored) -> Instant.parse(event.timestamp).toEpochMilli());

        DataStream<Event> events = environment
                .fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka events")
                .map(TrendTopicsJob::parse)
                .filter(event -> event != null && event.endpoint != null)
                .assignTimestampsAndWatermarks(watermarks);

        events.keyBy(event -> event.endpoint)
                .window(SlidingEventTimeWindows.of(Time.minutes(10), Time.minutes(1)))
                .aggregate(new CountEvents(), new FormatTrend())
                .name("trend-topics-by-endpoint")
                .print();

        environment.execute("trend-topics-watermarks");
    }

    private static Event parse(String value) {
        try {
            return JSON.readValue(value, Event.class);
        } catch (Exception ignored) {
            return null;
        }
    }

    public static class Event {
        public String timestamp;
        public String endpoint;
        public String metodo;
        public int status;
        public int tempo_resposta_ms;
        public String ip_origem;

        public Event() {
        }
    }

    private static class CountEvents implements AggregateFunction<Event, Long, Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(Event event, Long count) {
            return count + 1;
        }

        @Override
        public Long getResult(Long count) {
            return count;
        }

        @Override
        public Long merge(Long left, Long right) {
            return left + right;
        }
    }

    private static class FormatTrend extends ProcessWindowFunction<Long, String, String, TimeWindow> {
        @Override
        public void process(String endpoint, Context context, Iterable<Long> counts, Collector<String> output) {
            output.collect(String.format(
                    "{\"window_start\":\"%s\",\"window_end\":\"%s\",\"endpoint\":\"%s\",\"count\":%d}",
                    Instant.ofEpochMilli(context.window().getStart()),
                    Instant.ofEpochMilli(context.window().getEnd()),
                    endpoint,
                    counts.iterator().next()));
        }
    }
}
