env.set_stream_time_characteristic(TimeCharacteristic.EventTime)
env.get_config().set_auto_watermark_interval(1000)
stream.assign_timestamps_and_watermarks(
    WatermarkStrategy
        .for_bounded_out_of_orderness(Duration.ofSeconds(30))
        .withTimestampAssigner(lambda event, timestamp: event['timestamp'])
)
stream.key_by(lambda e: e['endpoint']) \
      .window(SlidingEventTimeWindows.of(Time.minutes(10), Time.minutes(1))) \
      .reduce(lambda a, b: {'endpoint': a['endpoint'], 'count': a['count'] + b['count']})
late_tag = OutputTag('late-events', Types.PICKLED_BYTE_ARRAY())
main_stream = stream.process(MyProcessFunction(late_tag))
late_stream = main_stream.get_side_output(late_tag)
producer = FlinkKafkaProducer(
    topic='trend-topics-result',
    serialization_schema=SimpleStringSchema(),
    producer_config={'bootstrap.servers': 'localhost:9092'}
)
main_stream.add_sink(producer)
import streamlit as st
import pandas as pd

data = pd.read_csv('resultados.csv')
st.bar_chart(data.groupby('endpoint')['count'].sum())
from pyflink.datastream import ProcessFunction

class MyProcessFunction(ProcessFunction):
    def __init__(self, late_tag):
        self.late_tag = late_tag

    def process_element(self, value, ctx):
        if ctx.timestamp() < ctx.timer_service().current_watermark():
            ctx.output(self.late_tag, value)
        else:
            yield value
