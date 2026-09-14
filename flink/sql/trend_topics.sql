CREATE TABLE events (
  timestamp STRING,
  metodo STRING,
  endpoint STRING,
  status INT,
  tempo_resposta_ms INT,
  ip_origem STRING,
  WATERMARK FOR timestamp AS CAST(timestamp AS TIMESTAMP(3)) - INTERVAL '30' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'events',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id' = 'flink-trend-topics',
  'scan.startup.mode' = 'latest-offset',
  'format' = 'json',
  'json.timestamp-format.standard' = 'ISO-8601'
);

CREATE TABLE trend_topics_result (
  window_start TIMESTAMP(3),
  window_end TIMESTAMP(3),
  endpoint STRING,
  cnt BIGINT
) WITH (
  'connector' = 'print'
);

INSERT INTO trend_topics_result
SELECT
  TUMBLE_START(ts, INTERVAL '10' MINUTES) AS window_start,
  TUMBLE_END(ts, INTERVAL '10' MINUTES) AS window_end,
  endpoint,
  COUNT(*) AS cnt
FROM (
  SELECT
    CAST(timestamp AS TIMESTAMP(3)) AS ts,
    endpoint
  FROM events
)
GROUP BY TUMBLE(ts, INTERVAL '10' MINUTES), endpoint;
