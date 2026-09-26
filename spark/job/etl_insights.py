"""
Job Spark - ETL de eventos de acesso + geracao de insights de negocio.

Le os eventos brutos do topico Kafka `events` (mesmo topico alimentado
pelo Flume), aplica um ETL com transformacoes de "wide dependency"
(join + groupBy, que exigem shuffle), calcula metricas de negocio e
grava o resultado em tres destinos, conforme a arquitetura do projeto:

  1) HDFS   -> parquet, para consumo posterior / dados frios
  2) Hive   -> tabela gerenciada, para consultas analiticas via SQL
  3) HBase  -> tabela "insights_endpoint", para consulta em tempo real
               (ex.: dashboard) por chave (endpoint)

Execucao (batch, mas pode ser reagendado via cron/Airflow):

    spark-submit \
        --master spark://spark:7077 \
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
        --conf spark.hadoop.fs.defaultFS=hdfs://namenode:9000 \
        /opt/spark-app/etl_insights.py
"""

from pyspark.sql import SparkSession, functions as F

KAFKA_BOOTSTRAP = "kafka:9092"
KAFKA_TOPIC = "events"

HDFS_OUTPUT_PATH = "hdfs://namenode:9000/data/processed/insights_endpoint"
HIVE_DATABASE = "bigdata"
HIVE_TABLE = "insights_endpoint"

HBASE_HOST = "hbase"
HBASE_THRIFT_PORT = 9090
HBASE_TABLE = "insights_endpoint"
HBASE_COLUMN_FAMILY = "cf"


def build_spark_session() -> SparkSession:
    return (
        SparkSession.builder.appName("EtlInsightsEndpoint")
        .enableHiveSupport()
        .getOrCreate()
    )


def ler_eventos_kafka(spark: SparkSession):
    """Le, em modo batch, todo o intervalo disponivel do topico `events`."""
    raw = (
        spark.read.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .option("endingOffsets", "latest")
        .load()
    )

    schema = "timestamp STRING, metodo STRING, endpoint STRING, status INT, tempo_resposta_ms INT, ip_origem STRING"

    eventos = (
        raw.selectExpr("CAST(value AS STRING) AS json_str")
        .select(F.from_json("json_str", schema).alias("evento"))
        .select("evento.*")
        .withColumn("timestamp", F.to_timestamp("timestamp"))
        .dropna(subset=["endpoint", "status"])
    )
    return eventos


def dimensao_endpoints(spark: SparkSession):
    """Pequena dimensao estatica: endpoint -> categoria de negocio.

    O join dessa dimensao com os eventos e proposital: e uma
    transformacao de wide dependency (shuffle), exigida pelo checklist.
    """
    dados = [
        ("/login", "autenticacao"),
        ("/produtos", "catalogo"),
        ("/carrinho", "carrinho"),
        ("/checkout", "vendas"),
        ("/usuarios", "conta"),
    ]
    return spark.createDataFrame(dados, ["endpoint", "categoria"])


def transformar(eventos, dim_endpoints):
    """ETL com wide dependencies: join (shuffle) + groupBy (shuffle)."""

    # 1) JOIN (wide dependency): enriquece cada evento com a categoria de negocio
    enriquecido = eventos.join(dim_endpoints, on="endpoint", how="left")

    # 2) GROUP BY (wide dependency): agrega metricas de negocio por endpoint
    insights = enriquecido.groupBy("endpoint", "categoria").agg(
        F.count("*").alias("total_requisicoes"),
        F.round(F.avg("tempo_resposta_ms"), 2).alias("tempo_resposta_medio_ms"),
        F.sum(F.when(F.col("status") >= 500, 1).otherwise(0)).alias("erros_5xx"),
        F.sum(F.when(F.col("status") >= 400, F.lit(1)).otherwise(F.lit(0))).alias("erros_4xx_5xx"),
        F.max("timestamp").alias("ultimo_evento"),
    )

    insights = insights.withColumn(
        "taxa_erro_pct",
        F.round(F.col("erros_4xx_5xx") / F.col("total_requisicoes") * 100, 2),
    )

    return insights


def gravar_hdfs(insights):
    (
        insights.write.mode("overwrite")
        .parquet(HDFS_OUTPUT_PATH)
    )
    print(f"[HDFS] Gravado em {HDFS_OUTPUT_PATH}")


def gravar_hive(spark: SparkSession, insights):
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {HIVE_DATABASE}")
    (
        insights.write.mode("overwrite")
        .format("parquet")
        .saveAsTable(f"{HIVE_DATABASE}.{HIVE_TABLE}")
    )
    print(f"[Hive] Gravado em {HIVE_DATABASE}.{HIVE_TABLE}")


def gravar_hbase(insights):
    """Grava o resumo de negocio no HBase via Thrift (happybase).

    O resultado ja esta agregado por endpoint (poucas linhas), por isso
    e seguro trazer para o driver com collect() antes de escrever.
    Alternativa mais "big data": usar o conector Spark-HBase (SHC) com
    saveAsNewAPIHadoopDataset, mas exige jars extras no classpath do
    Spark; o caminho via Thrift/happybase e o mais simples de reproduzir
    no ambiente docker-compose do trabalho.
    """
    import happybase

    conn = happybase.Connection(host=HBASE_HOST, port=HBASE_THRIFT_PORT)
    try:
        tabelas = {t.decode() for t in conn.tables()}
        if HBASE_TABLE not in tabelas:
            conn.create_table(HBASE_TABLE, {HBASE_COLUMN_FAMILY: dict()})

        tabela = conn.table(HBASE_TABLE)
        linhas = insights.collect()
        with tabela.batch(batch_size=200) as batch:
            for linha in linhas:
                row_key = linha["endpoint"].encode("utf-8")
                batch.put(row_key, {
                    f"{HBASE_COLUMN_FAMILY}:categoria".encode(): str(linha["categoria"]).encode(),
                    f"{HBASE_COLUMN_FAMILY}:total_requisicoes".encode(): str(linha["total_requisicoes"]).encode(),
                    f"{HBASE_COLUMN_FAMILY}:tempo_resposta_medio_ms".encode(): str(linha["tempo_resposta_medio_ms"]).encode(),
                    f"{HBASE_COLUMN_FAMILY}:erros_5xx".encode(): str(linha["erros_5xx"]).encode(),
                    f"{HBASE_COLUMN_FAMILY}:taxa_erro_pct".encode(): str(linha["taxa_erro_pct"]).encode(),
                    f"{HBASE_COLUMN_FAMILY}:ultimo_evento".encode(): str(linha["ultimo_evento"]).encode(),
                })
        print(f"[HBase] {len(linhas)} linha(s) gravada(s) em {HBASE_TABLE}")
    finally:
        conn.close()


def main():
    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    eventos = ler_eventos_kafka(spark)
    dim_endpoints = dimensao_endpoints(spark)
    insights = transformar(eventos, dim_endpoints).cache()

    total = insights.count()
    print(f"Insights calculados para {total} endpoint(s).")
    insights.show(truncate=False)

    gravar_hdfs(insights)
    gravar_hive(spark, insights)
    gravar_hbase(insights)

    spark.stop()


if __name__ == "__main__":
    main()
