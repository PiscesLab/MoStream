# MoStream — Quick start

This README explains how to get a local environment running to test the `WLGenerator` simulator and the Flink pipeline `MDWorkflow.py` with Kafka. It provides Docker commands to run Kafka, steps to create the required topic, how to run the simulator in both dry-run and Kafka modes, and how to run the Flink pipeline locally (or in local mode) for experimentation.

## Prerequisites
- Git
- Docker & Docker Compose
- Python 3.8+ and pip
- Java (required for Flink/PyFlink if running the full pipeline)

## Recommended workspace layout
Clone the repository then cd into it.

## Start Kafka with Docker Compose
Create a `docker-compose.yml` file (example below) or run a single-node Kafka via Docker.

Example `docker-compose.yml` (single-node Zookeeper + Kafka):

```yaml
version: '3.8'
services:
  zookeeper:
    image: confluentinc/cp-zookeeper:7.2.1
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000
    ports:
      - 2181:2181

  kafka:
    image: confluentinc/cp-kafka:7.2.1
    depends_on:
      - zookeeper
    ports:
      - 9092:9092
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: 'zookeeper:2181'
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
```

Start services:
```bash
docker compose up -d
```

Verify Kafka is up (you may wait a few seconds):
```bash
docker compose ps
```

## Create the `Simulation` topic
If Kafka allows auto-create, producing will create it automatically. To create explicitly (using kafka-topics inside the broker container):

```bash
# Run inside the kafka container (adjust container name)
docker compose exec kafka kafka-topics --create --topic Simulation --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1
```

To list topics:
```bash
docker compose exec kafka kafka-topics --list --bootstrap-server localhost:9092
```

## Python environment
Create a virtualenv and install the minimal dependency needed to run the simulator and tests:

```bash
python3 -m venv .venv
source .venv/bin/activate
python3 -m pip install --upgrade pip
python3 -m pip install kafka-python
```

Note: `MDWorkflow.py` depends on PyFlink and other libraries (rdkit, moldesign). If you want to run the full Flink job, you must install PyFlink and ensure Flink is available. For quick experimentation, `MDWorkflow.py --local` uses an in-memory source and doesn't require Kafka.

## Running the simulator
Dry-run (no Kafka): prints JSON messages to stdout — good for testing message format.

```bash
python3 MoStream/MDStream/WLGenerator/simulator.py --local --interval 0.5
```

Kafka mode (requires Kafka running):

```bash
python3 MoStream/MDStream/WLGenerator/simulator.py --kafka-bootstrap localhost:9092 --topic Simulation --interval 1.0 --model-id 1
```

You should see logs like:
```
[simulator] Creating KafkaProducer connecting to localhost:9092
[simulator] KafkaProducer created
[simulator] Message sent for CCO
```

## Running MDWorkflow
Local (no Kafka) — useful for testing the pipeline's logic:

```bash
python3 MoStream/MDStream/StreamML/MDWorkflow.py --local
```

Kafka mode (will consume from `Simulation` topic). Use `--starting-offset earliest` to read existing messages and `--group-id` to set a fresh consumer group:

```bash
python3 MoStream/MDStream/StreamML/MDWorkflow.py --kafka-bootstrap localhost:9092 --starting-offset earliest --group-id debug-group-1
```

Notes:
- Running `MDWorkflow.py` requires PyFlink and a proper Flink runtime; if PyFlink is not installed or Flink isn’t configured, the script will fail. Use `--local` to run pipeline logic without Kafka/PyFlink for simple testing.
- If `MDWorkflow.py` shows no output when using Kafka, check consumer starting offsets and group id — using `--starting-offset earliest` and a new `--group-id` usually makes it consume existing messages.

## Troubleshooting
- If `ModuleNotFoundError: No module named 'kafka'` appears, install kafka-python in your environment: `python3 -m pip install kafka-python`.
- If Flink job fails on import, install PyFlink or run in `--local` mode.
- Check Kafka logs: `docker compose logs kafka`.

## Local HTTP shim for quick testing (no Kafka needed)

If you want to test simulator <-> MDWorkflow message exchange without Kafka or a full Flink runtime, two helpers are provided:

- `MDWorkflow_local.py` — tiny server that accepts POST /simulate and returns a small recommendation POST back to the simulator.
- `simulator.py --local` — posts simulation payloads to the MDWorkflow local server and runs a small HTTP server to accept recommendations.

Quick commands:

Terminal A:
```bash
python3 MDWorkflow_local.py --host 0.0.0.0 --port 5001 --sim-host localhost --sim-port 5000
```

Terminal B:
```bash
python3 MoStream/MDStream/WLGenerator/simulator.py --local --md-host localhost --md-port 5001 --local-port 5000 --interval 1.0
```

You should see round-trip POST logs on both sides. This is recommended for quick developer testing.

## MDWorkflow in-pipeline posting (local Flink mode)

`MDWorkflow.py --local` has been extended to attach a map operator in local mode that will POST each recommendation string to the simulator HTTP endpoint (configured via `SIM_HOST`/`SIM_PORT` environment variables). This requires `pyflink` and a working Flink environment.

Example:

```bash
export SIM_HOST=localhost
export SIM_PORT=5000
python3 MoStream/MDStream/StreamML/MDWorkflow.py --local
```

Then run the simulator to receive recommendation posts:

```bash
python3 MoStream/MDStream/WLGenerator/simulator.py --local --local-port 5000
```

If you want I can add a small launcher script to start both services with one command.

## Optional: verify messages in topic
You can use kafka-console-consumer inside the container to peek messages:

```bash
docker compose exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic Simulation --from-beginning --max-messages 5
```

## Contribution
Add issues and PRs for better integration, tests, and CI. Consider adding a full `requirements.txt` listing `kafka-python`, `pyflink`, `rdkit`, `moldesign` if you intend to run the full pipeline.

---

