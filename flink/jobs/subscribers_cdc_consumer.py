from pyflink.common import WatermarkStrategy, Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.datastream.formats.json import JsonRowDeserializationSchema
from pyflink.table import StreamTableEnvironment
from pyflink.datastream import ProcessFunction
import json
import requests
import logging
import os

class ClickHouseSink(ProcessFunction):
    """Custom ClickHouse sink using HTTP interface - Individual record processing"""
    
    def __init__(self, clickhouse_url, database, table, username, password):
        self.clickhouse_url = clickhouse_url
        self.database = database
        self.table = table
        self.username = username
        self.password = password
        
    def open(self, runtime_context):
        # Initialize any resources here if needed
        pass
        
    def process_element(self, value, ctx):
        # value: Row(after, op, ts_ms)
        after = value[0]
        op = value[1]
        ts_ms = value[2]
        if after is not None:
            self.send_record(after, op, ts_ms)
    
    def close(self):
        # Nothing to flush since we're processing individually
        pass
    
    def send_record(self, after, op, ts_ms):
        try:
            # after: dict with subscriber fields
            subscriber_id = after.get("subscriber_id", 0)
            name = after.get("name", 'NULL').replace("'", "''") if after.get("name") else 'NULL'
            phone_number = after.get("phone_number", 'NULL').replace("'", "''") if after.get("phone_number") else 'NULL'
            plan_type = after.get("plan_type", 'NULL').replace("'", "''") if after.get("plan_type") else 'NULL'
            region = after.get("region", 'NULL').replace("'", "''") if after.get("region") else 'NULL'
            join_date = after.get("join_date", 0)
            status = after.get("status", 'NULL').replace("'", "''") if after.get("status") else 'NULL'
            
            # Prepare INSERT query for single record
            query = f"""INSERT INTO {self.database}.{self.table} 
                       (subscriber_id, name, phone_number, plan_type, region, join_date, status, op, ts_ms) 
                       VALUES ({subscriber_id}, '{name}', '{phone_number}', '{plan_type}', '{region}', {join_date}, '{status}', '{op}', toDateTime64({ts_ms} / 1000, 3))"""
            
            # Send to ClickHouse via HTTP
            response = requests.post(
                f"{self.clickhouse_url}",
                data=query,
                auth=(self.username, self.password) if self.username and self.password else None,
                headers={'Content-Type': 'text/plain'},
                timeout=30
            )
            
            if response.status_code == 200:
                print(f"Successfully inserted subscriber_id: {subscriber_id}")
            else:
                print(f"Error inserting subscriber {subscriber_id}: {response.status_code} - {response.text}")
                
        except Exception as e:
            print(f"Error sending record to ClickHouse: {str(e)}")

def read_from_kafka(env):
    # Get ClickHouse credentials from environment variables
    with open("clickhouse_config.json") as f:
        conf = json.load(f)

    clickhouse_username = os.getenv('CLICKHOUSE_USERNAME', conf["CLICKHOUSE_USERNAME"])
    clickhouse_password = os.getenv('CLICKHOUSE_PASSWORD', conf["CLICKHOUSE_PASSWORD"])
    clickhouse_host = os.getenv('CLICKHOUSE_HOST', conf["CLICKHOUSE_HOST"])
    clickhouse_port = os.getenv('CLICKHOUSE_PORT', '8443')
    clickhouse_database = os.getenv('CLICKHOUSE_DATABASE', 'CDR')
    clickhouse_table = os.getenv('CLICKHOUSE_TABLE', 'subscribers')
    
    # Deserialization schema for reading from Kafka
    deserialization_schema = (
        JsonRowDeserializationSchema.builder()
        .type_info(Types.ROW_NAMED(
            ["after", "op", "ts_ms"],
            [
                Types.MAP(Types.STRING(), Types.STRING()),  # after: dict of subscriber fields (all as string for simplicity)
                Types.STRING(),  # op
                Types.LONG()     # ts_ms
            ]
        ))
        .build()
    )

    # Kafka source configuration
    kafka_source = (
        KafkaSource.builder()
        .set_bootstrap_servers("redpanda:9092")
        .set_topics("telecom.public.subscribers")
        .set_value_only_deserializer(deserialization_schema)
        .set_starting_offsets(KafkaOffsetsInitializer.latest())
        .build()
    )

    # Create stream from Kafka source
    stream = env.from_source(
        source=kafka_source,
        watermark_strategy=WatermarkStrategy.no_watermarks(),
        source_name="Kafka Source",
    )

    # Create ClickHouse sink
    clickhouse_sink = ClickHouseSink(
        clickhouse_url=f"https://{clickhouse_host}:{clickhouse_port}",
        database=clickhouse_database,
        table=clickhouse_table,
        username=clickhouse_username,
        password=clickhouse_password
    )
    
    # Process and sink data
    stream.process(clickhouse_sink).name("ClickHouse Sink")

    env.execute("Kafka Consumer and ClickHouse Producer Job")

if __name__ == "__main__":
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    
    print("Starting Kafka consumer and ClickHouse producer...")
    read_from_kafka(env)