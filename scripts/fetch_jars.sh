#!/usr/bin/env bash
# Download the Flink Kafka connector JARs the pipeline loads at submission time.
#
# These are build dependencies, not source, so they are not tracked in git. Run this
# once after cloning, and on every node that submits the job.
#
#   ./scripts/fetch_jars.sh
#
# Override the destination with JARS_DIR if your checkout lives elsewhere.
set -euo pipefail

JARS_DIR="${JARS_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/jars}"
MAVEN="${MAVEN_BASE:-https://repo1.maven.org/maven2}"

mkdir -p "$JARS_DIR"

fetch() {
  local url="$1" name="$2"
  if [[ -f "$JARS_DIR/$name" ]]; then
    echo "  present  $name"
    return
  fi
  echo "  fetching $name"
  curl -fsSL "$url" -o "$JARS_DIR/$name.part"
  mv "$JARS_DIR/$name.part" "$JARS_DIR/$name"
}

echo "Fetching Flink Kafka connector JARs into $JARS_DIR"
fetch "$MAVEN/org/apache/flink/flink-connector-kafka/4.0.1-2.0/flink-connector-kafka-4.0.1-2.0.jar" \
      "flink-connector-kafka-4.0.1-2.0.jar"
fetch "$MAVEN/org/apache/kafka/kafka-clients/3.6.1/kafka-clients-3.6.1.jar" \
      "kafka-clients-3.6.1.jar"
echo "Done. MDWorkflow.py loads both from $JARS_DIR at submission."
