SET 'classloader.resolve-order' = 'parent-first';

CREATE TABLE events (
  `timestamp` TIMESTAMP(3),
  metodo STRING,
  endpoint STRING,
  status INT,
  tempo_resposta_ms INT,
  ip_origem STRING,
  WATERMARK FOR `timestamp` AS `timestamp` - INTERVAL '30' SECOND
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
  window_start,
  window_end,
  endpoint,
  COUNT(*) AS cnt
FROM TABLE(
  HOP(
    TABLE events,
    DESCRIPTOR(`timestamp`),
    INTERVAL '1' MINUTE,
    INTERVAL '10' MINUTES
  )
)
GROUP BY window_start, window_end, endpoint;
