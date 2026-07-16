#!/usr/bin/env bash
# Quick demo launcher — start Kafka first, then run components in separate terminals.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

CMD="${1:-}"
case "$CMD" in
  kafka)
    docker compose up -d
    echo "Kafka starting on localhost:9092"
    ;;
  build)
    mvn -q clean package -DskipTests
    echo "Build complete."
    ;;
  traffic)
    java -jar traffic-simulator/target/traffic-simulator-1.0.0.jar
    ;;
  processor)
    # Higher delay → visible lag for Vigil demos
    PROCESSING_DELAY_MS="${PROCESSING_DELAY_MS:-250}" java -jar stream-processor/target/stream-processor-1.0.0.jar
    ;;
  agent)
    java -jar vigil-agent/target/vigil-agent-1.0.0.jar
    ;;
  console)
    java -jar console/target/console-1.0.0.jar
    ;;
  *)
    cat <<EOF
Kafka Vigil — demo scripts

Usage: ./scripts/run.sh <command>

  kafka      Start local Kafka (Docker)
  build      mvn package all modules
  traffic    Produce order events
  processor  Consume + validate (DLQ on poison)
  agent      Vigil observability agent
  console    Live health dashboard

Interview demo flow:
  1. ./scripts/run.sh kafka
  2. ./scripts/run.sh build
  3. Start processor, agent, console, then traffic (4 terminals)
  4. Raise lag: PROCESSING_DELAY_MS=800 ./scripts/run.sh processor
EOF
    ;;
esac
