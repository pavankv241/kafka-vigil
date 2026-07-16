# Kafka Vigil

**Mini hybrid observability agent for Apache Kafka** — built as a portfolio project aligned with IBM / Confluent Control Center (C3) and Unified Stream Manager (USM) themes: cluster health, topic visibility, consumer lag monitoring, and anomaly alerts.

## Why this project?

Most Kafka demos stop at “produce → consume.” Kafka Vigil goes one level up into **platform engineering / observability**:

| Capability | Maps to role |
|---|---|
| Vigil Agent (AdminClient lag + health snapshots) | USM Agent-style sidecar |
| Topic + consumer group visibility | C3 cluster / topic / consumer views |
| Anomaly alerts on lag thresholds | Observability & ops workflows |
| DLQ routing for poison messages | Reliable stream processing |
| Idempotent producer + keyed partitioning | Distributed systems fundamentals |

## Architecture

```
┌─────────────────┐     orders.events      ┌──────────────────┐
│ Traffic         │ ─────────────────────► │ Stream Processor │
│ Simulator       │                        │ (order-validators│
└─────────────────┘                        │  + DLQ)          │
                                           └────────┬─────────┘
                                                    │ orders.events.dlq
┌─────────────────┐     AdminClient API             ▼
│ Vigil Agent     │◄──── offsets / lag ──── Kafka Broker
│ (health +       │
│  anomalies)     │──── cluster.health ───► Health Console
└─────────────────┘──── cluster.anomalies ─► (alerts)
```

## Modules

| Module | Role |
|---|---|
| `common` | Events, health models, JSON serde, topic bootstrap |
| `traffic-simulator` | Produces orders (keyed by customer) + poison msgs |
| `stream-processor` | Validates orders; slow path creates lag; DLQ |
| `vigil-agent` | Collects lag/health, classifies status, emits alerts |
| `console` | Terminal “Control Center lite” dashboard |

## Prerequisites

- **Java 21+**
- **Maven 3.9+**
- **Docker** (for local Kafka)

## Quick start

```bash
# 1. Start Kafka (KRaft, no ZooKeeper)
docker compose up -d

# 2. Build
mvn clean package

# 3. Run each in its own terminal
./scripts/run.sh processor
./scripts/run.sh agent
./scripts/run.sh console
./scripts/run.sh traffic
```

### Force a lag spike (demo for interviews)

```bash
PROCESSING_DELAY_MS=800 TRAFFIC_RATE=20 ./scripts/run.sh processor
# in another terminal keep traffic high:
TRAFFIC_RATE=20 ./scripts/run.sh traffic
```

Watch the console flip `HEALTHY → DEGRADED → CRITICAL` and print anomaly alerts.

## Config (env vars)

| Variable | Default | Meaning |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Broker address |
| `TRAFFIC_RATE` | `5` | Messages / second |
| `POISON_EVERY` | `25` | Every Nth message is poison → DLQ |
| `PROCESSING_DELAY_MS` | `200` | Artificial work per message |
| `VIGIL_INTERVAL_MS` | `3000` | Health scrape interval |
| `LAG_WARN_THRESHOLD` | `50` | DEGRADED above this lag |
| `LAG_CRITICAL_THRESHOLD` | `200` | CRITICAL + anomaly alert |

## Tests

```bash
mvn test
```

Unit tests cover event factories and `HealthClassifier` (pure logic, no broker required).

## Design document

See [DESIGN.md](DESIGN.md) for the full design: problem, goals/non-goals, architecture, data models, component behavior, health algorithms, trade-offs, risks, role mapping, roadmap, and interview demo script.

## Talking points for interviews

1. **Consumer lag** = end offset − committed offset; Vigil computes this via `AdminClient` (same class of APIs used by control planes).
2. **Partition hotspots** — agent flags individual partitions with critical lag, not just group totals.
3. **Poison → DLQ** — invalid payloads are isolated so the main consumer group keeps making progress.
4. **Idempotent producer** (`enable.idempotence=true`, `acks=all`) — safe retries without duplicates.
5. **Keying by `customerId`** — sticky partitioning / ordering within a customer.

## License

MIT — personal learning / interview portfolio project.
