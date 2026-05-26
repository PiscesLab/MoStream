import argparse, logging, sys, json, time, os
from pathlib import Path
from pyflink.common import WatermarkStrategy, Encoder, Types, Time, Configuration
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.datastream.window import CountWindow, CountTumblingWindowAssigner
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaSink, KafkaOffsetsInitializer, KafkaTopicPartition, KafkaRecordSerializationSchema
from pyflink.datastream.connectors.base import DeliveryGuarantee, SupportsPreprocessing, StreamTransformer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.formats.json import JsonRowSerializationSchema, JsonRowDeserializationSchema
from NPMMModel import TrainFunction 
from Inference import InferFunction
from Ranking import RankFunction
#from .gitinfo import get_git_info            # relative import
# or
#from .moldesign.utils.gitinfo import get_git_info
import statistics
import argparse

def average(weight_str1, weight_str2):
    list1 = [i for i in weight_str1.split(";")]
    list2 = [i for i in weight_str2.split(";")]
    # Check if the lists have the same length
    if len(list1) != len(list2):
        raise ValueError("Lists must have the same length.")

    # Calculate the element-wise average
    average_list = [(x + y) / 2 for x, y in zip(list1, list2)]
    return average_list

def _fix_add_jars():
    import ast as _ast
    from pyflink.datastream.stream_execution_environment import StreamExecutionEnvironment as _E
    _orig = _E.add_jars

    def _patched(self, *jars_path):
        from pyflink.java_gateway import get_gateway as _gw
        jvm = _gw().jvm
        key = jvm.org.apache.flink.configuration.PipelineOptions.JARS.key()
        cfg = jvm.org.apache.flink.python.util.PythonConfigUtil \
            .getEnvironmentConfig(self._j_stream_execution_environment)
        old = cfg.getString(key, None)
        if old and old.strip().startswith('['):
            # Java stores pipeline.jars as YAML list: ['file:/a.jar', 'file:/b.jar']
            # add_jars() prepends it verbatim → broken URL ['file:/...'];file:/...
            # Parse and rewrite as semicolon-separated before appending new jars.
            try:
                old = ';'.join(_ast.literal_eval(old.strip()))
            except Exception:
                old = old.strip()[1:-1].replace("', '", ';') \
                          .replace("'", '').replace('"', '').strip()
            cfg.setString(key, old)
        _orig(self, *jars_path)

    _E.add_jars = _patched

_fix_add_jars()


def workflow(kafka_bootstrap='localhost:9092', local_mode=False):
    config = Configuration()
    config.set_string("taskmanager.memory.network.min", "512m")
    config.set_string("taskmanager.memory.network.max", "2g")
    config.set_string("taskmanager.memory.network.fraction", "0.2")
    env = StreamExecutionEnvironment.get_execution_environment(config)

    # --- JARs: use URIs (handles spaces automatically)
    jars_dir = Path(__file__).resolve().parents[3] / "jars"
    kafka_connector = (jars_dir / "flink-connector-kafka-4.0.1-2.0.jar").as_uri()
    kafka_clients   = (jars_dir / "kafka-clients-3.6.1.jar").as_uri()
    env.add_jars(kafka_connector, kafka_clients)

    # --- Python files: use a NORMAL PATH (no file://, no %20)
    # add the current StreamML package directory (where this file lives)
    streamml_dir = Path(__file__).resolve().parent
    env.add_python_file(str(streamml_dir))  # <-- plain path

    env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
    
    # Configure the KafkaSource (consumer)
    if local_mode:
        # use an in-memory collection for local testing
        sample_messages = [json.dumps({"smiles": "CCO", "IP_simulate": 12.34, "model_id": 0}),
                           json.dumps({"smiles": "CCC", "IP_simulate": 11.11, "model_id": 0})]
        source_stream = env.from_collection(sample_messages, type_info=Types.STRING())
    else:
        # allow the outer scope to provide desired starting offsets and group id via env vars
        starting = os.environ.get('KAFKA_STARTING_OFFSETS', 'latest')
        group_id = os.environ.get('KAFKA_GROUP_ID', 'my-group')
        if starting == 'earliest':
            offsets = KafkaOffsetsInitializer.earliest()
        else:
            offsets = KafkaOffsetsInitializer.latest()

        kafka_source = KafkaSource.builder() \
          .set_bootstrap_servers(kafka_bootstrap) \
      .set_group_id(group_id) \
      .set_topics("Simulation") \
      .set_starting_offsets(offsets) \
      .set_value_only_deserializer(SimpleStringSchema()) \
      .build() 
    # --- Sink (producer)
    sink = KafkaSink.builder() \
    .set_bootstrap_servers(kafka_bootstrap) \
    .set_record_serializer(
        KafkaRecordSerializationSchema.builder()
            .set_topic("Result")
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
    ) \
    .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE) \
    .build()

    # Add the source to the Flink pipeline
    if local_mode:
        stream = source_stream
    else:
        stream = env.from_source(kafka_source, WatermarkStrategy.no_watermarks(), "Source-data")

    # Parse the JSON stream and extract desired fields
    extracted_stream = stream.map(lambda value: json.loads(value)).name("LoadJSON") \
        .map(lambda obj: ((obj['smiles'], float(obj['IP_simulate']), int(obj['model_id']))), output_type=Types.TUPLE([Types.STRING(), Types.DOUBLE(), Types.INT()])).name("Parse")
 
    train_stream = extracted_stream.key_by(lambda x: x[2]) \
        .process(TrainFunction(), output_type=Types.STRING()) \
        .map(lambda x: ((int(x.split("$")[0]), x.split("$")[1], int(x.split("$")[2]), x.split("$")[3])), output_type=Types.TUPLE([Types.INT(), Types.STRING(), Types.INT(), Types.STRING()])).name("Parse->Train") 
    # input string model_id,string
    # output tuple (chunk_id, model-weigths, model_id, model-infra)

    #train_stream.print()
    #infer_stream = train_stream.reduce(lambda a, b: (average(a[1], b[1]), a[0]))
    infer_stream = train_stream.key_by(lambda x: x[0]) \
        .process(InferFunction(), output_type=Types.STRING()) \
        .map(lambda x: ((int(x.split("$")[0]), x.split("$")[1], float(x.split("$")[2]))), output_type=Types.TUPLE([Types.INT(), Types.STRING(), Types.DOUBLE()])).name("Train->Infer")
    # input string chunk_id, string (json) model-weights, string, model_id, string (json) model-infra
    # output tuple (int chunk_id, string smiles, float estimated_IP)
 
    #infer2_stream = extracted_stream.key_by(lambda x: x[2]) \
    #    .process(InferFunction(), output_type=Types.STRING()) \
    #    .map(lambda x: ((int(x.split("$")[0]), x.split("$")[1], float(x.split("$")[2]))), output_type=Types.TUPLE([Types.INT(), Types.STRING(), Types.DOUBLE()])).name("Parse->Infer")

    #infer_stream = infer1_stream.union(infer2_stream)

    rank_stream = infer_stream.key_by(lambda x: x[0]).window_all(CountTumblingWindowAssigner(1000)) \
        .apply(RankFunction(), output_type=Types.STRING()).name("Infer->Rank")
        #.map(lambda x: ((int(x.split("$")[0]), x.split("$")[1], float(x.split("$")[2]))), output_type=Types.TUPLE([Types.INT(), Types.STRING(), Types.DOUBLE()]))
    #rank_stream = infer_stream.window_all(TumblingEventTimeWindows.of(Time.seconds(5))) \
    #                          .apply(RankFunction())    
   
    #output_stream = rank_stream.filter(lambda x:x!='').name("Rank-Filter")
    output_stream = rank_stream.flat_map(lambda x: x.split("$"), output_type=Types.STRING()).name("Rank->Filter") \
        .filter(lambda x: x!='').name("Filter")
    
    #output_stream = extracted_stream.key_by(lambda x: x[0]).reduce(lambda a, b: (a[1] + b[1], b[0]))
    #output_stream = output_stream.map(lambda x: str(x), output_type=Types.STRING())
    #output_stream = rank_stream.map(lambda x: (x[0], x[1], x[2]), output_type=Types.LIST(Types.STRING()))
       #.map(lambda x:  "model_id: " + str(x[0]) + " smiles_search: " + str(x[1]) + " pred_IP: " + str(x[2]), output_type=Types.STRING())   

    # Configure the KafkaSink (producer)
    sink = KafkaSink.builder() \
      .set_bootstrap_servers("localhost:9092") \
      .set_record_serializer(
        KafkaRecordSerializationSchema.builder()
            .set_topic("Result")
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        ) \
      .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE) \
      .build()

    # Print the received messages to the TaskManager's standard output
    #output_stream.print() 
    output_stream.sink_to(sink)
    
    #Execute the Flink job
    env.execute("MDStream")

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--kafka-bootstrap', default='localhost:9092', help='Kafka bootstrap servers')
    parser.add_argument('--local', action='store_true', help='Run pipeline with an in-memory local source (no Kafka)')
    parser.add_argument('--starting-offset', choices=['earliest','latest'], default='latest', help='Kafka starting offset')
    parser.add_argument('--group-id', default='my-group', help='Kafka consumer group id')
    args = parser.parse_args()
    # expose starting offset and group id via env vars used by the KafkaSource builder
    os.environ['KAFKA_STARTING_OFFSETS'] = args.starting_offset
    os.environ['KAFKA_GROUP_ID'] = args.group_id
    workflow(kafka_bootstrap=args.kafka_bootstrap, local_mode=args.local)
