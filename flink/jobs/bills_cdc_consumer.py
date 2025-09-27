from pyflink.common import WatermarkStrategy, Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.datastream.formats.json import JsonRowDeserializationSchema
from pyflink.datastream import ProcessFunction
import json
import requests
import os

class ClickHouseBillSink(ProcessFunction):
    """Custom ClickHouse sink for bills using HTTP interface - Individual record processing"""

    def __init__(self, clickhouse_url, database, table, username, password):
        self.clickhouse_url = clickhouse_url
        self.database = database
        self.table = table
        self.username = username
        self.password = password

    def open(self, runtime_context):
        pass

    def process_element(self, value, ctx):
        after = value[0]
        op = value[1]
        ts_ms = value[2]
        if after is not None:
            self.send_record(after, op, ts_ms)

    def close(self):
        pass

    def send_record(self, after, op, ts_ms):
        try:
            bill_id = int(after.get("bill_id", 0))
            subscriber_id = int(after.get("subscriber_id", 0))
            amount = after.get("amount", "0.00")
            bill_date = after.get("bill_date", None)
            status = after.get("status", "unpaid").replace("'", "''") if after.get("status") else 'unpaid'

            query = f"""INSERT INTO {self.database}.{self.table}
                        (bill_id, subscriber_id, amount, bill_date, status, op, ts_ms)
                        VALUES ({bill_id}, {subscriber_id}, {amount}, toDateTime64({bill_date} / 1000000, 3), '{status}', '{op}', toDateTime64({ts_ms} / 1000, 3))"""

            response = requests.post(
                f"{self.clickhouse_url}",
                data=query,
                auth=(self.username, self.password) if self.username and self.password else None,
                headers={'Content-Type': 'text/plain'},
                timeout=30
            )

            if response.status_code == 200:
                print(f"Inserted bill_id: {bill_id}")
            else:
                print(f"Error inserting bill {bill_id}: {response.status_code} - {response.text}")

        except Exception as e:
            print(f"Error sending bill record to ClickHouse: {str(e)}")

def read_from_kafka(env):
    with open("/opt/flink/conf/clickhouse_config.json") as f:
        conf = json.load(f)

    clickhouse_username = os.getenv('CLICKHOUSE_USERNAME', conf["CLICKHOUSE_USERNAME"])
    clickhouse_password = os.getenv('CLICKHOUSE_PASSWORD', conf["CLICKHOUSE_PASSWORD"])
    clickhouse_host = os.getenv('CLICKHOUSE_HOST', conf["CLICKHOUSE_HOST"])
    clickhouse_port = os.getenv('CLICKHOUSE_PORT', '8443')
    clickhouse_database = os.getenv('CLICKHOUSE_DATABASE', 'CDR')
    clickhouse_table = os.getenv('CLICKHOUSE_BILLS_TABLE', 'billing_events')

    deserialization_schema = (
        JsonRowDeserializationSchema.builder()
        .type_info(Types.ROW_NAMED(
            ["after", "op", "ts_ms"],
            [
                Types.MAP(Types.STRING(), Types.STRING()),  # after: dict of bill fields (all as string for simplicity)
                Types.STRING(),  # op
                Types.LONG()     # ts_ms
            ]
        ))
        .build()
    )

    kafka_source = (
        KafkaSource.builder()
        .set_bootstrap_servers("redpanda:9092")
        .set_topics("telecom.public.billing_events")
        .set_value_only_deserializer(deserialization_schema)
        .set_starting_offsets(KafkaOffsetsInitializer.latest())
        .build()
    )

    stream = env.from_source(
        source=kafka_source,
        watermark_strategy=WatermarkStrategy.no_watermarks(),
        source_name="Kafka Source",
    )

    clickhouse_sink = ClickHouseBillSink(
        clickhouse_url=f"https://{clickhouse_host}:{clickhouse_port}",
        database=clickhouse_database,
        table=clickhouse_table,
        username=clickhouse_username,
        password=clickhouse_password
    )

    stream.process(clickhouse_sink).name("ClickHouse Bill Sink")

    env.execute("Bills CDC")

if __name__ == "__main__":
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    print("Starting Kafka bills consumer and ClickHouse producer...")
    read_from_kafka(env)
