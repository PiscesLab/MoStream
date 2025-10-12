import argparse, logging, sys, json

from pyflink.common import WatermarkStrategy, Encoder, Types
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaSink, KafkaOffsetsInitializer, KafkaTopicPartition, KafkaRecordSerializationSchema
from pyflink.datastream.connectors.base import DeliveryGuarantee, SupportsPreprocessing, StreamTransformer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.formats.json import JsonRowSerializationSchema, JsonRowDeserializationSchema

def workflow():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.add_jars("file:///mnt/media/MDStream/StreamML/flink-sql-connector-kafka-1.17.1.jar")
    env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
    # write all the data to one file

    # Configure the Kafka consumer properties
    #kafka_consumer_props = {
    #  'bootstrap.servers': '128.110.96.15:9092',
    #  'group.id': 'my-consumer-group'
    #}

    # Create a Flink Kafka consumer
    #kafka_source = FlinkKafkaConsumer(
    #  'Simulation',  # Kafka topic
    #  SimpleStringSchema(),  # Message deserialization schema
    #  properties=kafka_consumer_props
    #)
    #source_partition_set = {
    #   KafkaTopicPartition("Simulation", 16)
    #}
    kafka_source = KafkaSource.builder() \
      .set_bootstrap_servers('128.110.96.15:9092') \
      .set_group_id('my-group') \
      .set_topics("Simulation") \
      .set_starting_offsets(KafkaOffsetsInitializer.latest()) \
      .set_value_only_deserializer(SimpleStringSchema()) \
      .build() 

    # Add the Kafka source as a data source to the Flink pipeline
    #stream = env.add_source(kafka_source)
    stream = env.from_source(kafka_source, WatermarkStrategy.no_watermarks(), "Source-data")

    # Parse the JSON stream and extract desired fields
    extracted_stream = stream.map(lambda value: json.loads(value)) \
        .map(lambda obj: ((obj['smiles'], float(obj['IP_simulate']))), output_type=Types.TUPLE([Types.STRING(), Types.DOUBLE()]))
 
    output_stream = extracted_stream.key_by(lambda x: x[0]).reduce(lambda a, b: (a[1] + b[1], b[0]))
    output_stream = output_stream.map(lambda x: str(x), output_type=Types.STRING())
    #extracted_stream.map(lambda obj: "smiles: " + str(obj['smiles']) + "ip: " + str(obj['IP_simulate']) + "memo: " + "hahha", output_type=Types.STRING()) 

    # Configure the Kafka producer properties
    #kafka_producer_props = {
    #  'bootstrap.servers': '128.110.96.15:9092',
    #  'acks': 'all'
    #}

    #type_info = [Types.STRING(), Types.DOUBLE()]
    #serialization_schema = JsonRowSerializationSchema.Builder() \
    #    .with_type_info(type_info) \
    #    .build()
    # Create a Flink Kafka producer
    #kafka_sink = FlinkKafkaProducer(
    #  'Result',  # Kafka topic to produce to
    #  SimpleStringSchema(),  # Message serialization schema
    #  producer_config=kafka_producer_props
    #)
    #sink_partition_set = {
    #   KafkaTopicPartition("Result", 16)
    #}
    sink = KafkaSink.builder() \
      .set_bootstrap_servers('128.110.96.15:9092') \
      .set_record_serializer(
        KafkaRecordSerializationSchema.builder()
            .set_topic("Result")
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        ) \
      .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE) \
      .build()

    # Print the received messages to the TaskManager's standard output
    #stream.print()
    #extracted_stream.print()
    #extracted_stream.add_sink(kafka_sink)
    output_stream.sink_to(sink)
    #Execute the Flink job
    env.execute("MDStream")

if __name__ == '__main__':
    workflow()
