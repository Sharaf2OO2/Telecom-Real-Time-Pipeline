from pyflink.common import WatermarkStrategy, Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer, KafkaSink, KafkaRecordSerializationSchema
from pyflink.datastream.formats.json import JsonRowDeserializationSchema
from pyflink.common.serialization import SimpleStringSchema
from datetime import datetime
import json

def process_cdr_record(row):
    """Process CDR record and add enrichment data"""
    processed_data = {
        "call_id": row[0],
        "subscribers_id": row[1], 
        "call_start": row[2],
        "call_end": row[3],
        "duration_sec": row[4],
        "call_type": row[5],
        "tower_id": row[6]
    }
    return json.dumps(processed_data)

def read_from_kafka(env):
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

    # Process the data (transform the consumed message)
    processed_stream = stream.map(
        process_cdr_record,
        output_type=Types.STRING()
    )

    # Serialization schema for writing to Kafka
    serialization_schema = KafkaRecordSerializationSchema.builder() \
        .set_topic("telecom.processed") \
        .set_value_serialization_schema(SimpleStringSchema()) \
        .build()

    # Kafka sink configuration
    kafka_sink = (
        KafkaSink.builder()
        .set_bootstrap_servers("redpanda:9092")
        .set_record_serializer(serialization_schema)
        .build()
    )

    # Send processed data to output topic
    processed_stream.sink_to(kafka_sink)

    env.execute("Kafka Consumer and Producer Job")

if __name__ == "__main__":
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    print("Starting Kafka consumer and producer...")
    read_from_kafka(env)