from pyflink.common import WatermarkStrategy, Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.datastream.formats.json import JsonRowDeserializationSchema
from pyflink.datastream import ProcessFunction
import json
import requests
import os

class ClickHouseTowerSink(ProcessFunction):
    """Custom ClickHouse sink for towers using HTTP interface - Individual record processing"""

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
            tower_id = int(after.get("tower_id", 0))
            location = after.get("location", '').replace("'", "''")
            capacity = int(after.get("capacity", 0))
            region = after.get("region", '').replace("'", "''")

            query = f"""INSERT INTO {self.database}.{self.table}
                        (tower_id, location, capacity, region, op, ts_ms)
                        VALUES ({tower_id}, '{location}', {capacity}, '{region}', '{op}', toDateTime64({ts_ms} / 1000, 3))"""

            response = requests.post(
                f"{self.clickhouse_url}",
                data=query,
                auth=(self.username, self.password) if self.username and self.password else None,
                headers={'Content-Type': 'text/plain'},
                timeout=30
            )

            if response.status_code == 200:
                print(f"Inserted tower_id: {tower_id}")
            else:
                print(f"Error inserting tower {tower_id}: {response.status_code} - {response.text}")

        except Exception as e:
            print(f"Error sending tower record to ClickHouse: {str(e)}")

def read_from_kafka(env):
    with open("/opt/flink/conf/clickhouse_config.json") as f:
        conf = json.load(f)

    clickhouse_username = os.getenv('CLICKHOUSE_USERNAME', conf["CLICKHOUSE_USERNAME"])
    clickhouse_password = os.getenv('CLICKHOUSE_PASSWORD', conf["CLICKHOUSE_PASSWORD"])
    clickhouse_host = os.getenv('CLICKHOUSE_HOST', conf["CLICKHOUSE_HOST"])
    clickhouse_port = os.getenv('CLICKHOUSE_PORT', '8443')
    clickhouse_database = os.getenv('CLICKHOUSE_DATABASE', 'CDR')
    clickhouse_table = os.getenv('CLICKHOUSE_TOWERS_TABLE', 'cell_towers')

    deserialization_schema = (
        JsonRowDeserializationSchema.builder()
        .type_info(Types.ROW_NAMED(
            ["after", "op", "ts_ms"],
            [
                Types.MAP(Types.STRING(), Types.STRING()),  # after: dict of tower fields (all as string for simplicity)
                Types.STRING(),  # op
                Types.LONG()     # ts_ms
            ]
        ))
        .build()
    )

    kafka_source = (
        KafkaSource.builder()
        .set_bootstrap_servers("redpanda:9092")
        .set_topics("telecom.public.cell_towers")
        .set_value_only_deserializer(deserialization_schema)
        .set_starting_offsets(KafkaOffsetsInitializer.latest())
        .build()
    )

    stream = env.from_source(
        source=kafka_source,
        watermark_strategy=WatermarkStrategy.no_watermarks(),
        source_name="Kafka Source",
    )

    clickhouse_sink = ClickHouseTowerSink(
        clickhouse_url=f"https://{clickhouse_host}:{clickhouse_port}",
        database=clickhouse_database,
        table=clickhouse_table,
        username=clickhouse_username,
        password=clickhouse_password
    )

    stream.process(clickhouse_sink).name("ClickHouse Tower Sink")

    env.execute("Towers CDC")

if __name__ == "__main__":
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    print("Starting Kafka towers consumer and ClickHouse producer...")
    read_from_kafka(env)
