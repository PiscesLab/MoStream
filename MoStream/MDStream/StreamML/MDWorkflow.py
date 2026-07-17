import argparse, logging, sys, json, time, os
from pathlib import Path
from pyflink.common import WatermarkStrategy, Encoder, Types, Time, Configuration
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.datastream.window import (CountWindow, CountTumblingWindowAssigner,
                                       CountTrigger, PurgingTrigger)
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
    config.set_string("taskmanager.memory.network.max", "512m")

    # Bundle settings govern how many records the Python operator buffers before flushing a
    # bundle to the Beam worker, and how long it will wait to do so. A bundle completes when
    # EITHER limit is hit, so with size=1 the time limit never fires: every record is its own
    # bundle, flushed immediately. That is the lowest-latency configuration and is why it is
    # the default here.
    #
    # These are the knobs experiment E4 sweeps. Overridable so the sweep can be scripted
    # without editing this file (which would change the job graph between arms).
    bundle_size = os.environ.get("MOSTREAM_BUNDLE_SIZE", "1")
    bundle_time = os.environ.get("MOSTREAM_BUNDLE_TIME", "60000")
    config.set_string("python.fn-execution.bundle.size", bundle_size)
    config.set_string("python.fn-execution.bundle.time", bundle_time)
    print(f"[MDWorkflow] bundle.size={bundle_size} bundle.time={bundle_time}")

    config.set_string("python.executable",
                      "/users/NamSDSU/miniconda3/envs/mostream/bin/python3.9")

    # Checkpoint storage. The engine's DEFAULT is JobManagerCheckpointStorage, which keeps
    # checkpoint state in the JobManager's heap and CAPS each subtask's state at 5 MiB. That is
    # fine for lightweight analytics state and fatal for ours: once a subtask's managed state
    # crossed 5 MiB, every async snapshot failed with
    #   "Size of the state is larger than the maximum permitted memory-backed state
    #    (Size=6306404, maxSize=5242880). Consider using FileSystemCheckpointStorage."
    # and, because the counter only resets on a SUCCESSFUL checkpoint, the failures accumulated
    # toward the tolerable-failure threshold with no way back. This is the same class of defect
    # as the barrier finding in Section 5.4: a fault-tolerance default sized for cheap operators.
    # We select FileSystemCheckpointStorage, which writes checkpoints to a filesystem instead of
    # the JM heap and imposes no such cap. These are the canonical Flink 2.0 keys
    # (execution.checkpointing.*); the storage type is set EXPLICITLY rather than inferred from
    # the dir, and both are verified present in flink-dist-2.0.0.jar. /proj is the CloudLab
    # project NFS, mounted and writable on BOTH the JobManager and the TaskManager, which
    # FileSystemCheckpointStorage requires (the TM writes state, the JM writes the metadata).
    config.set_string("execution.checkpointing.storage", "filesystem")
    config.set_string("execution.checkpointing.dir",
                      "file:///proj/pisceslabsd-PG0/NamSDSU/flink-checkpoints")
    env = StreamExecutionEnvironment.get_execution_environment(config)

    # --- JARs: use URIs (handles spaces automatically)
    jars_dir = Path(__file__).resolve().parents[3] / "jars"
    kafka_connector = (jars_dir / "flink-connector-kafka-4.0.1-2.0.jar").as_uri()
    kafka_clients   = (jars_dir / "kafka-clients-3.6.1.jar").as_uri()
    env.add_jars(kafka_connector, kafka_clients)

    # Distribute StreamML directory to TaskManagers (moldesign package + UDF modules)
    streamml_dir = Path(__file__).resolve().parent
    env.add_python_file(str(streamml_dir))

    env.set_runtime_mode(RuntimeExecutionMode.STREAMING)

    # Checkpointing.
    #
    # An ALIGNED checkpoint barrier cannot overtake buffered records: it must wait for every
    # record queued ahead of it to be processed. Its traversal time is therefore a function of
    # the QUEUE, not of the per-record cost -- and that distinction is the whole story here.
    #
    # An earlier version of this comment blamed the per-record cost ("our operators take
    # SECONDS per record") and widened the timeout to compensate. That reasoning is wrong, and
    # the fix it motivated does not work. With a SHALLOW queue these same 1.5s/8s operators
    # checkpoint in 180 MILLISECONDS. It is only once a queue builds that traversal time
    # explodes -- and under sustained backpressure the queue is unbounded, so NO timeout is
    # large enough. Widening it only changes how long you wait before failing.
    #
    # What actually built the queue was Rank pinned to parallelism 1 by `window_all` (see the
    # Rank operator below). Every checkpoint after that queue formed timed out; ten consecutive
    # failures tripped the threshold; Flink killed the job; the restart restored from the last
    # good checkpoint and REPLAYED the backlog, which rebuilt the queue. The job could never
    # escape. Observed: 59 consecutive failed checkpoints and 5 forced restarts, with the last
    # successful checkpoint six hours in the past.
    #
    # So the real remedy is upstream (do not let a parallelism-1 operator sit behind a 500x
    # amplification), not here. What remains here is insurance: keep the interval and timeout
    # sized to the real cost, and do not let a TRANSIENT backpressure spike terminate a
    # multi-day endurance run. A missed checkpoint is affordable in this workflow -- the
    # surrogate is persisted outside the checkpoint (see TrainFunction) and the Kafka sink is
    # AT_LEAST_ONCE -- so it costs reprocessing, not correctness. Checkpoint health is measured
    # directly from the REST API rather than inferred from the job staying alive.
    env.enable_checkpointing(60000)  # flush KafkaSink AT_LEAST_ONCE every 60s
    _ckpt = env.get_checkpoint_config()
    _ckpt.set_checkpoint_timeout(600000)            # 10 min for a barrier to traverse
    # With the Rank chokepoint removed and FileSystemCheckpointStorage in place, checkpoints
    # succeed in tens of ms, so a large tolerance is no longer masking a spiral. Keep a modest
    # one as genuine insurance for a multi-day run (a transient backpressure spike should cost a
    # missed checkpoint, not the job) but small enough that a REAL regression surfaces quickly
    # rather than after 1000 silent failures, which is how the 5 MiB cap hid for 15 hours.
    _ckpt.set_tolerable_checkpoint_failure_number(20)
    _ckpt.set_min_pause_between_checkpoints(30000)  # don't stack barriers under backpressure
    _ckpt.set_max_concurrent_checkpoints(1)

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
    # NOTE: the KafkaSink is built once, further below, immediately before sink_to().
    # A second, earlier KafkaSink.builder() used to sit here; it was dead code -- the
    # variable was reassigned before use, so the object was constructed and discarded.

    # Add the source to the Flink pipeline
    if local_mode:
        stream = source_stream
    else:
        stream = env.from_source(kafka_source, WatermarkStrategy.no_watermarks(), "Source-data")

    # Parse the JSON stream and extract desired fields.
    # src_ts is the producer-side wall-clock (ms) stamped by the simulator. It is carried
    # unchanged through Train -> Infer -> Rank so that each emitted recommendation can be
    # attributed to the simulation result that caused it. This is the E0 (steering latency)
    # measurement: latency = emit_ts - src_ts.
    extracted_stream = stream.map(lambda value: json.loads(value)).name("LoadJSON") \
        .map(lambda obj: ((obj['smiles'], float(obj['IP_simulate']), int(obj['model_id']), int(obj.get('timestamp', 0)))),
             output_type=Types.TUPLE([Types.STRING(), Types.DOUBLE(), Types.INT(), Types.LONG()])).name("Parse")

    # E6 flag, resolved HERE on the client (where the env var exists) and passed into the operator
    # constructor so it is serialized with the operator and reaches the TaskManager's Python
    # worker. Setting it via the submission environment does NOT reach the worker.
    _persist_weights = os.environ.get("MOSTREAM_PERSIST_WEIGHTS", "1") == "1"
    # E5 train-once arm: freeze the surrogate after its first gradient step. Resolved HERE for
    # the same reason as persist_weights -- the env var exists on the client but not in the
    # TaskManager's Beam worker, so it must travel as a constructor argument.
    _train_once = os.environ.get("MOSTREAM_TRAIN_ONCE", "0") == "1"
    print(f"[MDWorkflow] persist_weights={_persist_weights} train_once={_train_once}")
    train_stream = extracted_stream.key_by(lambda x: x[2]) \
        .process(TrainFunction(persist_weights=_persist_weights, train_once=_train_once),
                 output_type=Types.STRING()) \
        .map(lambda x: ((int(x.split("$")[0]), x.split("$")[1], int(x.split("$")[2]), x.split("$")[3], int(x.split("$")[4]))),
             output_type=Types.TUPLE([Types.INT(), Types.STRING(), Types.INT(), Types.STRING(), Types.LONG()])).name("Parse->Train")
    # input  tuple (smiles, IP, model_id, src_ts)
    # output tuple (chunk_id, model-weights, model_id, model-infra, src_ts)
    # TrainFunction fits ONE record then emits, so the weights it emits provably incorporate
    # the record carrying src_ts. That is what makes the latency attribution causal.

    #train_stream.print()
    #infer_stream = train_stream.reduce(lambda a, b: (average(a[1], b[1]), a[0]))
    infer_stream = train_stream.key_by(lambda x: x[0]) \
        .process(InferFunction(), output_type=Types.STRING()) \
        .map(lambda x: ((int(x.split("$")[0]), x.split("$")[1], float(x.split("$")[2]), int(x.split("$")[3]))),
             output_type=Types.TUPLE([Types.INT(), Types.STRING(), Types.DOUBLE(), Types.LONG()])).name("Train->Infer")
    # input  tuple (chunk_id, model-weights, model_id, model-infra, src_ts)
    # output tuple (chunk_id, smiles, estimated_IP, src_ts)
 
    #infer2_stream = extracted_stream.key_by(lambda x: x[2]) \
    #    .process(InferFunction(), output_type=Types.STRING()) \
    #    .map(lambda x: ((int(x.split("$")[0]), x.split("$")[1], float(x.split("$")[2]))), output_type=Types.TUPLE([Types.INT(), Types.STRING(), Types.DOUBLE()])).name("Parse->Infer")

    #infer_stream = infer1_stream.union(infer2_stream)

    # Rank: a KEYED count window, not a global one.
    #
    # This was `.key_by(chunk_id).window_all(...)`. `window_all` DISCARDS the preceding key_by
    # and pins the operator to parallelism 1 -- silently, with no warning, in a line that reads
    # as if it were data-parallel. Because Rank sits downstream of Infer's 500x amplification,
    # that single sub-task had to absorb ~500x the record rate of the whole rest of the
    # pipeline while the other seven slots idled. It became the bottleneck, the Infer->Rank
    # queue grew without bound, and the sustained backpressure prevented checkpoint barriers
    # from ever traversing the dataflow -- which is what drove the checkpoint-timeout restart
    # loop. One word.
    #
    # chunk_id is a safe key: a SMILES lives in exactly one chunk (Infer slices the search
    # space by chunk_id), so it always routes to the same sub-task and RankFunction's `searched`
    # de-duplication set stays correct per sub-task. See Ranking.RankFunction's docstring.
    # The trigger is EXPLICIT because the default retains every record forever.
    #
    # CountTumblingWindowAssigner's default trigger is a bare CountTrigger, whose on_element
    # returns TriggerResult.FIRE -- not FIRE_AND_PURGE. pyflink's own source says so at
    # datastream/window.py: "the window is not purged though, all elements are retained."
    # Normally a window's contents are reclaimed by a cleanup timer registered at
    # window.max_timestamp() + allowed_lateness; but CountWindow.max_timestamp() is
    # MAX_LONG_VALUE, so that timer never fires. The two together mean the element ListState of
    # a count window is NEVER cleared: every record that has ever entered Rank stays in keyed
    # state for the life of the job.
    #
    # Measured on the 24.2 h E1 run, which carried this defect: Rank held 346.1 MB of the job's
    # 392 MB of checkpointed state (Train held 0.0, Infer 45.9) and it was still climbing with no
    # plateau. Both axes match the mechanism to within measurement noise -- 5,093,880 records
    # into Rank x ~68 B/record = 346 MB total, and 210,665 rec/h x 68 B = 14.3 MB/h against ~15
    # MB/h observed -- which is what distinguishes this from a guess. It also inflated every
    # snapshot: checkpoint duration drifted to p99 14.7 s / max 32.6 s carrying the garbage.
    #
    # PurgingTrigger wraps the nested trigger and converts its FIRE into FIRE_AND_PURGE, so the
    # window's contents are dropped once RankFunction has read them. This is safe here: the
    # window is TUMBLING, so panes are disjoint and nothing downstream re-reads a fired window.
    # Rank's cross-window state (the `searched` de-duplication set) is an instance variable, not
    # window state, and is untouched by purging.
    #
    # NOTE: `count_window(10)` is NOT an escape hatch -- it returns
    # WindowedStream(self, CountTumblingWindowAssigner(size)), i.e. this same non-purging default.
    rank_stream = infer_stream.key_by(lambda x: x[0]).window(CountTumblingWindowAssigner(10)) \
        .trigger(PurgingTrigger.of(CountTrigger.of(10))) \
        .apply(RankFunction(), output_type=Types.STRING()).name("Infer->Rank")
        #.map(lambda x: ((int(x.split("$")[0]), x.split("$")[1], float(x.split("$")[2]))), output_type=Types.TUPLE([Types.INT(), Types.STRING(), Types.DOUBLE()]))
    #rank_stream = infer_stream.window_all(TumblingEventTimeWindows.of(Time.seconds(5))) \
    #                          .apply(RankFunction())    
   
    #output_stream = rank_stream.filter(lambda x:x!='').name("Rank-Filter")
    _PLACEHOLDERS = {'search_space_empty', 'model_not_ready', 'mol_dicts_empty', 'inference_error'}
    output_stream = rank_stream.flat_map(lambda x: x.split("$"), output_type=Types.STRING()).name("Rank->Filter") \
        .filter(lambda x: x != '' and not any(p in x for p in ('search_space_empty', 'model_not_ready', 'mol_dicts_empty', 'inference_error'))).name("Filter")
    
    #output_stream = extracted_stream.key_by(lambda x: x[0]).reduce(lambda a, b: (a[1] + b[1], b[0]))
    #output_stream = output_stream.map(lambda x: str(x), output_type=Types.STRING())
    #output_stream = rank_stream.map(lambda x: (x[0], x[1], x[2]), output_type=Types.LIST(Types.STRING()))
       #.map(lambda x:  "model_id: " + str(x[0]) + " smiles_search: " + str(x[1]) + " pred_IP: " + str(x[2]), output_type=Types.STRING())   

    # Configure the KafkaSink (producer)
    sink = KafkaSink.builder() \
      .set_bootstrap_servers(kafka_bootstrap) \
      .set_record_serializer(
        KafkaRecordSerializationSchema.builder()
            .set_topic("Recommend")
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
