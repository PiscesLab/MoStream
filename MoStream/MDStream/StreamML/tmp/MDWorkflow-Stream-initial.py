import argparse, logging, sys, json

from pyflink.common import WatermarkStrategy, Encoder, Types
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.datastream.connectors import FlinkKafkaProducer, FlinkKafkaConsumer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.formats.json import JsonRowSerializationSchema, JsonRowDeserializationSchema

def train():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.add_jars("file:///mnt/media/MDStream/StreamML/flink-sql-connector-kafka-1.17.1.jar")
    env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
    # write all the data to one file

    # Configure the Kafka consumer properties
    kafka_consumer_props = {
      'bootstrap.servers': 'KAFKA_HOST:9092',
      'group.id': 'my-consumer-group'
    }

    # Create a Flink Kafka consumer
    kafka_source = FlinkKafkaConsumer(
      'Simulation',  # Kafka topic
      SimpleStringSchema(),  # Message deserialization schema
      properties=kafka_consumer_props
    )

    # Add the Kafka source as a data source to the Flink pipeline
    stream = env.add_source(kafka_source)

    # Parse the JSON stream and extract desired fields
    extracted_stream = stream.map(lambda value: json.loads(value)) \
        .map(lambda obj: "smiles: " + str(obj['smiles']) + "ip: " + str(obj['IP_simulate']) + "memo: " + "hahha", output_type=Types.STRING()) 

    # Configure the Kafka producer properties
    kafka_producer_props = {
      'bootstrap.servers': 'KAFKA_HOST:9092',
      'acks': 'all'
    }

    #type_info = [Types.STRING(), Types.DOUBLE()]
    #serialization_schema = JsonRowSerializationSchema.Builder() \
    #    .with_type_info(type_info) \
    #    .build()
    # Create a Flink Kafka producer
    kafka_producer = FlinkKafkaProducer(
      'Result',  # Kafka topic to produce to
      SimpleStringSchema(),  # Message serialization schema
      producer_config=kafka_producer_props
    )

    # Print the received messages to the TaskManager's standard output
    #stream.print()
    #extracted_stream.print()
    extracted_stream.add_sink(kafka_producer)

    #Execute the Flink job
    env.execute()

if __name__ == '__main__':
    train()
