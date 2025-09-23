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
        # Process each record immediately
        self.send_record(value)
    
    def close(self):
        # Nothing to flush since we're processing individually
        pass
    
    def send_record(self, record):
        try:
            # Extract values from the record
            call_id = record[0] if record[0] else 'NULL'
            subscribers_id = record[1] if record[1] else 0
            call_start = f"'{record[2]}'" if record[2] else 'NULL'
            call_end = f"'{record[3]}'" if record[3] else 'NULL'
            duration_sec = record[4] if record[4] else 0
            call_type = f"'{record[5]}'" if record[5] else 'NULL'
            tower_id = record[6] if record[6] else 0
            
            # Prepare INSERT query for single record
            query = f"""INSERT INTO {self.database}.{self.table} 
                       (call_id, subscribers_id, call_start, call_end, duration_sec, call_type, tower_id) 
                       VALUES ('{call_id}', {subscribers_id}, {call_start}, {call_end}, {duration_sec}, {call_type}, {tower_id})"""
            
            # Send to ClickHouse via HTTP
            response = requests.post(
                f"{self.clickhouse_url}",
                data=query,
                auth=(self.username, self.password) if self.username and self.password else None,
                headers={'Content-Type': 'text/plain'},
                timeout=30
            )
            
            if response.status_code == 200:
                print(f"Successfully inserted record with call_id: {call_id}")
            else:
                print(f"Error inserting record {call_id}: {response.status_code} - {response.text}")
                
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
    clickhouse_database = os.getenv('CLICKHOUSE_DATABASE', 'Telecom_Hot')
    clickhouse_table = os.getenv('CLICKHOUSE_TABLE', 'call_events_bronze')
    
    # Deserialization schema for reading from Kafka
    deserialization_schema = (
        JsonRowDeserializationSchema.builder()
        .type_info(Types.ROW_NAMED(
            ["call_id", "subscribers_id", "call_start", "call_end", "duration_sec", "call_type", "tower_id"],
            [Types.STRING(), Types.INT(), Types.STRING(), Types.STRING(), Types.INT(), Types.STRING(), Types.INT()]
        ))
        .build()
    )

    # Kafka source configuration
    kafka_source = (
        KafkaSource.builder()
        .set_bootstrap_servers("redpanda:9092")
        .set_topics("telecom.cdr")
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