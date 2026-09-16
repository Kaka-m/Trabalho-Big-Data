import json
import os

import pandas as pd
import streamlit as st
from kafka import KafkaConsumer
from kafka.serializer import Deserializer

TOPIC = os.getenv("KAFKA_TOPIC", "trend-topics-result")
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
PAGE_TITLE = "Trend Topics Dashboard"


class JsonDeserializer(Deserializer):
    def deserialize(self, value):
        if value is None:
            return None
        return json.loads(value.decode("utf-8"))


def fetch_latest_results(limit=20):
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=[BOOTSTRAP_SERVERS],
        auto_offset_reset="latest",
        enable_auto_commit=True,
        group_id="streamlit-dashboard",
        value_deserializer=JsonDeserializer(),
        consumer_timeout_ms=2000,
    )

    rows = []
    try:
        for message in consumer:
            rows.append(message.value)
            if len(rows) >= limit:
                break
    except Exception:
        rows = []
    finally:
        consumer.close()

    return rows


st.set_page_config(page_title=PAGE_TITLE, layout="wide")
st.title(PAGE_TITLE)

with st.container():
    st.caption("Consumo em tempo real do tópico Kafka trend-topics-result")

    refresh = st.button("Atualizar")
    if refresh:
        st.rerun()

    rows = fetch_latest_results(limit=20)

    if not rows:
        st.info("Ainda não há registros no tópico Kafka de resultados. Aguarde alguns segundos e tente novamente.")
        st.stop()

    df = pd.DataFrame(rows)
    df["window_start"] = pd.to_datetime(df["window_start"], utc=True)
    df["window_end"] = pd.to_datetime(df["window_end"], utc=True)

    endpoint_summary = df.groupby("endpoint", as_index=False)["count"].sum().sort_values("count", ascending=False)

    col1, col2, col3 = st.columns(3)
    col1.metric("Endpoints", str(len(endpoint_summary)))
    col2.metric("Total de eventos", str(int(endpoint_summary["count"].sum())))
    col3.metric("Última janela", df["window_end"].max().strftime("%Y-%m-%d %H:%M UTC"))

    st.subheader("Contagem por endpoint")
    st.bar_chart(endpoint_summary.set_index("endpoint")["count"])

    st.subheader("Resultados recentes")
    st.dataframe(df.sort_values("window_end", ascending=False), use_container_width=True)
