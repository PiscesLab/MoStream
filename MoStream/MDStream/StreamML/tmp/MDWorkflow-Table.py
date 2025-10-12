import argparse, logging, sys, json

from pyflink.common import WatermarkStrategy, Encoder, Types
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.datastream.connectors import FlinkKafkaProducer, FlinkKafkaConsumer
from pyflink.table import StreamTableEnvironment, EnvironmentSettings, DataTypes
from pyflink.table.udf import ScalarFunction, udf

def workflow():
    env = StreamExecutionEnvironment.get_execution_environment()
    #env.add_jars("file:///mnt/media/MDStream/StreamML/flink-sql-connector-kafka-1.17.1.jar")
    env.add_jars("file:///mnt/media/software/flink-1.17.1/opt/flink-sql-connector-kafka-1.17.1.jar", "file:///mnt/media/software/flink-1.17.1/opt/flink-connector-kafka-1.17.1.jar")
    #env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
    env_settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    # Configure to use UDF
    t_env = StreamTableEnvironment.create(env, environment_settings=env_settings)
    t_env.get_config().get_configuration().set_boolean("python.fn-execution.memory.managed", True)

    class Model(ScalarFunction):
        def __init__(self):
            self.model_name = 'online_ml_model'
        def eval(self, x, y):
            return "ahaha" 
         

    model = udf(Model(), input_types=[DataTypes.STRING(), DataTypes.DOUBLE()], result_type=DataTypes.STRING())
    t_env.register_function('train', model)
    
    # Create kafkaconsumer
    t_env.execute_sql(f"""
    CREATE TABLE source (
        smiles VARCHAR,
        IP_simulate DOUBLE
    ) with (
        'connector' = 'kafka',
        'topic' = 'Simulation',
        'properties.bootstrap.servers' = '128.110.96.15:9092',
        'properties.group.id' = 'simulate',
        'scan.startup.mode' = 'latest-offset',
        'json.fail-on-missing-field' = 'false',
        'json.ignore-parse-errors' = 'true',
        'format' = 'json'
    )
    """)

    # Create kafkaproducer
    t_env.execute_sql(f"""
    CREATE TABLE sink (
        smiles VARCHAR,
        actual_ip DOUBLE,
        pred_y VARCHAR
    ) with (
        'connector' = 'kafka',
        'topic' = 'Result',
        'properties.bootstrap.servers' = '128.110.96.15:9092',
        'scan.startup.mode' = 'latest-offset',
        'json.fail-on-missing-field' = 'false',
        'json.ignore-parse-errors' = 'true',
        'format' = 'json'
    )
    """)
    source_table = t_env.from_path("source")
    
    sql = ("""
        SELECT
          smiles,
          IP_simulate,
          train(smiles, IP_simulate) AS pred_y
        FROM
          source
    """)
    t_env.execute_sql(sql)
    
    result_tbl = source_table.select("smiles, IP_simulate, pred_y")
    result_tbl.execute_insert('sink').wait()

    t_env.execute()

if __name__ == '__main__':
    workflow()
