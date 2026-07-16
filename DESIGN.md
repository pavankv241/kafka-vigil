# Design Document: Kafka Vigil

**Title:** Mini Hybrid Observability Agent for Apache Kafka  
**Version:** 1.0  
**Status:** Implemented  
**Stack:** Java 21 · Apache Kafka 3.8 (clients) · Maven multi-module · Docker Compose (KRaft)

---

## 1. Document purpose

This design explains **why** Kafka Vigil exists, **what** it builds, **how** components interact, and **which trade-offs** were accepted. It is written in the style of an engineering design one-pager expanded into a full design note — the kind of artifact expected when owning a scoped sub-component on a platform team (USM Agent, C3 consumer monitoring, etc.).

---

## 2. Background & motivation

### 2.1 Context

Apache Kafka is the backbone of many event-driven and streaming platforms. Day-2 operations matter as much as producing and consuming messages:

- Are consumers keeping up with producers?
- Which topics and partitions are growing?
- When should an operator get paged?
- How do poison messages get isolated without stalling the pipeline?

Products such as **Confluent Control Center (C3)** and **Unified Stream Manager (USM)** turn those questions into product surfaces: cluster health, topic visibility, consumer monitoring, connector status, and hybrid management agents.

### 2.2 Gap in typical learning projects

Most “Kafka + Java” tutorials stop at:

```
Producer → Topic → Consumer → print(record)
```

That proves client APIs work, but it does **not** demonstrate:

| Platform skill | Missing in basic demos |
|---|---|
| Observability | Lag, health status, alerts |
| Reliability patterns | DLQ, idempotent produce, commits |
| Control-plane thinking | AdminClient, group offsets, thresholds |
| Ownership narrative | A named sub-component you can defend in review |

### 2.3 Design thesis

> **Kafka Vigil is a teaching-scale control plane:** a real domain pipeline plus a sidecar agent that scrapes broker metadata, classifies health, and publishes observability events that a console can render — analogous to a thin slice of USM Agent + C3 consumer/topic views.

---

## 3. Problem statement

**Operators (and interviewers) need a concrete answer to:**

> “Given a Kafka cluster running an order validation pipeline, how do we continuously know whether the system is healthy, and how do we surface actionable signals when it is not?”

Concrete failure modes we care about in v1:

1. **Consumer lag growth** — processor too slow or down while traffic continues.  
2. **Partition hotspots** — one partition lags while others are fine (skew / sticky key imbalance).  
3. **Poison payloads** — bad messages must not block the consumer group indefinitely.  
4. **Lack of a shared health contract** — metrics trapped in process logs instead of a durable topic other tools can consume.

---

## 4. Goals and non-goals

### 4.1 Goals (v1)

| ID | Goal | Measurable outcome |
|---|---|---|
| G1 | Run a multi-process Kafka pipeline locally | Docker Kafka + 4 Java processes |
| G2 | Compute accurate consumer lag | Per-partition + group totals via AdminClient |
| G3 | Classify health with clear thresholds | `HEALTHY` / `DEGRADED` / `CRITICAL` |
| G4 | Emit anomaly alerts | Messages on `cluster.anomalies` |
| G5 | Render a live operator view | Terminal console updates from `cluster.health` |
| G6 | Demonstrate reliability basics | Idempotent producer, manual commits, DLQ |
| G7 | Keep core logic testable | `HealthClassifier` unit tests without a broker |

### 4.2 Non-goals (v1)

| Explicitly out of scope | Why deferred |
|---|---|
| Multi-cluster / hybrid federation | Needs inventory + remote agents |
| AuthN/AuthZ, TLS, ACLs | Local demo; not security review focus |
| Web UI / React dashboard | Terminal is enough for narrative |
| Prometheus / OpenTelemetry export | Follow-up; Kafka topics act as the bus today |
| Connector / Connect monitoring | C3 has it; not modeled here |
| Exactly-once end-to-end transactions | Idempotent produce + at-least-once consume is enough for v1 |
| Schema Registry / Avro | JSON keeps the demo inspectable |
| Production HA (RF≥3, ISR monitoring) | Single-broker KRaft for local speed |

---

## 5. Users & use cases

### 5.1 Primary user personas

| Persona | Need |
|---|---|
| **Platform engineer (early career)** | Understand lag, groups, partitions, DLQ |
| **On-call mentee** | See health flip under load; practice reading signals |
| **Interviewer / reviewer** | Hear a coherent design: problem → model → trade-offs |

### 5.2 Use cases

| UC | Description | Happy path |
|---|---|---|
| UC1 | Steady-state monitoring | Traffic + processor balanced → console shows HEALTHY, lag near 0 |
| UC2 | Lag spike investigation | Slow processor → DEGRADED/CRITICAL + anomaly alerts |
| UC3 | Poison isolation | Every Nth poison order → DLQ; main group continues |
| UC4 | Topic visibility | Console shows partition counts + approximate message totals |
| UC5 | Hot partition detection | One partition lag ≥ CRITICAL → `PARTITION_LAG_HOTSPOT` |

---

## 6. High-level architecture

### 6.1 Logical view

```
┌──────────────────────┐         orders.events (key=customerId)
│  Traffic Simulator   │ ──────────────────────────────────────┐
│  - idempotent produce│                                       │
│  - poison injection  │                                       ▼
└──────────────────────┘                          ┌────────────────────────┐
                                                  │  Stream Processor      │
                                                  │  group: order-validators│
                                                  │  - validate            │
                                                  │  - delay (tunable)     │
                                                  │  - commitSync          │
                                                  │  - DLQ on failure      │
                                                  └───────────┬────────────┘
                                                              │ orders.events.dlq
┌──────────────────────┐     AdminClient                      ▼
│  Vigil Agent         │◄──── describeTopics                  Kafka (KRaft)
│  - scrape loop       │◄──── listOffsets (end)               Broker :9092
│  - HealthClassifier  │◄──── listConsumerGroupOffsets
│  - publish snapshots │
└──────────┬───────────┘
           │  cluster.health
           │  cluster.anomalies
           ▼
┌──────────────────────┐
│  Health Console      │  “Control Center lite”
│  - health dashboard  │
│  - alert stream      │
└──────────────────────┘
```

### 6.2 Module map (Maven)

| Module | Artifact | Main class | Role |
|---|---|---|---|
| `common` | library | — | Topics, records, JSON serde, bootstrap, config |
| `traffic-simulator` | fat JAR | `TrafficSimulator` | Load generator |
| `stream-processor` | fat JAR | `StreamProcessor` | Domain consumer + DLQ |
| `vigil-agent` | fat JAR | `VigilAgent` | Observability agent |
| `console` | fat JAR | `HealthConsole` | Operator UI (terminal) |

### 6.3 Process topology (runtime)

All processes are **independent JVMs**. They share only:

1. Kafka bootstrap address (`KAFKA_BOOTSTRAP_SERVERS`)  
2. Topic name contracts (`Topics` constants)  
3. JSON schemas implied by Java records  

This mirrors how agents and consoles talk through the platform rather than through in-process coupling.

---

## 7. Topic design

### 7.1 Topic inventory

| Topic | Partitions | RF (local) | Producers | Consumers | Purpose |
|---|---|---|---|---|---|
| `orders.events` | 3 | 1 | Traffic Simulator | Stream Processor (`order-validators`) | Domain stream |
| `orders.events.dlq` | 1 | 1 | Stream Processor | (ops / future reprocessor) | Dead letters |
| `cluster.health` | 1 | 1 | Vigil Agent | Health Console | Periodic snapshots |
| `cluster.anomalies` | 1 | 1 | Vigil Agent | Health Console | Discrete alerts |

Topics are created at process start by `TopicBootstrap.ensureTopics()` via `AdminClient.createTopics`. Existing topics are treated as success (`TopicExistsException` ignored).

### 7.2 Partitioning rationale

- **`orders.events` uses 3 partitions** so the demo can show per-partition lag and hotspots.  
- **Record key = `customerId`** → same customer maps to the same partition → **ordering per customer**, which is a classic interview talking point.  
- Side effect: popular customers can create **partition imbalance** (useful for explaining hotspots).

### 7.3 Why health/alerts are Kafka topics

Publishing observability onto Kafka (instead of only stdout) gives:

- Decoupling (console can restart independently)  
- A durable, inspectable event stream  
- A story closer to “platform bus” / control-plane events  

---

## 8. Data model

### 8.1 Domain: `OrderEvent`

| Field | Type | Notes |
|---|---|---|
| `orderId` | String | UUID; poison IDs prefixed with `POISON-` |
| `customerId` | String | Also used as Kafka key |
| `region` | String | e.g. `us-east`, `ap-south` |
| `amount` | double | Must be `> 0` to validate |
| `status` | enum | `CREATED` / `VALIDATED` / `FAILED` |
| `createdAt` | Instant | Event time |
| `poison` | boolean | Explicit bad-message flag for demos |

### 8.2 Observability: `HealthSnapshot`

| Field | Meaning |
|---|---|
| `collectedAt` | Scrape timestamp |
| `clusterId` | Logical cluster label (`vigil-local`) |
| `topics` | List of `TopicHealth` |
| `consumerLags` | List of `ConsumerGroupLag` |
| `overallStatus` | Aggregate classification |
| `summary` | Human-readable one-liner |

### 8.3 `TopicHealth`

| Field | Meaning |
|---|---|
| `topic` | Topic name |
| `partitionCount` | From `describeTopics` |
| `totalMessages` | Sum of end offsets (approx log end, not “unique events”) |
| `bytesEstimate` | Rough `totalMessages * 256` for dashboard demos |
| `underReplicated` | Reserved signal (always `false` in v1 single-broker) |

### 8.4 `ConsumerGroupLag` / `PartitionLag`

For each watched partition of `orders.events`:

```
lag(partition) = max(0, endOffset − committedOffset)
totalLag       = Σ lag(partition)
```

If no committed offset exists yet, committed is treated as `0` (conservative: may look “behind” until first commit).

### 8.5 `AnomalyAlert`

| Field | Example |
|---|---|
| `type` | `CONSUMER_LAG_WARN`, `CONSUMER_LAG_CRITICAL`, `PARTITION_LAG_HOTSPOT` |
| `severity` | `DEGRADED` or `CRITICAL` |
| `message` | Operator-facing text |
| `groupId` / `topic` | Entity identity |
| `metricValue` / `threshold` | Numbers for the alert |

### 8.6 Serialization

- Format: **JSON** via Jackson (`JavaTimeModule`, ISO-8601 Instant)  
- Kafka key: String  
- Kafka value: JSON bytes (`JsonSerde`)  

**Trade-off:** JSON is easy to debug with console tools; Schema Registry would be a natural v1.1 upgrade for contract evolution.

---

## 9. Component design (detailed)

### 9.1 Traffic Simulator

**Responsibility:** Generate realistic-enough load and controlled failures.

**Behavior:**

1. Ensure topics exist.  
2. Create idempotent producer (`acks=all`, `enable.idempotence=true`).  
3. Loop at `TRAFFIC_RATE` messages/second.  
4. Every `POISON_EVERY` messages, emit a poison order (`amount < 0`, `poison=true`).  
5. Key records by `customerId` for sticky partitioning.  
6. Shutdown hook stops the loop cleanly and flushes.

**Why idempotent producer?** Retries after network blips should not create duplicate logical sends with the same producer identity — a standard distributed-systems reliability control.

### 9.2 Stream Processor

**Responsibility:** Validate orders; demonstrate lag and DLQ.

**Consumer settings (important):**

| Setting | Value | Rationale |
|---|---|---|
| `group.id` | `order-validators` | Stable identity for lag tracking |
| `enable.auto.commit` | `false` | Commit only after successful handling / DLQ path |
| `auto.offset.reset` | `earliest` | Replay from start on fresh group |
| `max.poll.records` | `10` | Smaller batches → lag more visible under delay |

**Processing algorithm:**

```
poll batch
for each record:
  if null OR poison OR amount <= 0:
    write FAILED copy to orders.events.dlq
    log warn
  else:
    sleep(PROCESSING_DELAY_MS)   // simulated validation work
    log validated
commitSync()
```

**Lag demo knob:** Increase `PROCESSING_DELAY_MS` while traffic is high → end offsets race ahead of committed offsets → Vigil Agent reports rising lag.

**DLQ philosophy:** Prefer **progress + isolation** over infinite retry on known-bad payloads. Reprocessing from DLQ is intentionally left as a follow-up workflow.

### 9.3 Vigil Agent (core sub-component)

This is the **owned sub-component** narrative for the IBM role: an agent loop that turns broker facts into health products.

**Scrape loop (default every 3s):**

1. `describeTopics` for watched topics  
2. `listOffsets(..., OffsetSpec.latest())` for all partitions  
3. `listConsumerGroupOffsets("order-validators")`  
4. Build `TopicHealth` + `ConsumerGroupLag`  
5. `HealthClassifier.classify(totalLag)`  
6. Publish `HealthSnapshot` → `cluster.health`  
7. `detectAnomalies(snapshot)` → publish each alert → `cluster.anomalies`

**Why AdminClient (not consumer metrics alone)?**

Control planes typically use admin/metadata APIs so they are not tied to a single application’s JMX. This matches how external observers (C3-like tools, agents) reason about the cluster.

**Watched scope (v1):**

- Group: `order-validators` only  
- Topics: `orders.events`, `orders.events.dlq`  

Discovery of all groups/topics is a deliberate follow-up (closer to full C3 browser).

### 9.4 Health Classifier (pure domain logic)

Extracted as a separate class so it can be unit-tested without Kafka.

**Inputs:** thresholds `warn` and `critical` where `0 < warn < critical`.

**Classification:**

| Condition | Status |
|---|---|
| `totalLag < warn` | `HEALTHY` |
| `warn ≤ totalLag < critical` | `DEGRADED` |
| `totalLag ≥ critical` | `CRITICAL` |

**Anomaly rules:**

| Rule | Alert type |
|---|---|
| Group total lag ≥ critical | `CONSUMER_LAG_CRITICAL` |
| Group total lag ≥ warn (and below critical path) | `CONSUMER_LAG_WARN` |
| Any partition lag ≥ critical | `PARTITION_LAG_HOTSPOT` |

Defaults: `LAG_WARN_THRESHOLD=50`, `LAG_CRITICAL_THRESHOLD=200`.

### 9.5 Health Console

**Responsibility:** Human-facing “lite Control Center.”

**Threads:**

1. **health-reader** — consumer on `cluster.health` (`auto.offset.reset=latest`)  
2. **alert-reader** — consumer on `cluster.anomalies`  
3. **renderer** — reprints ASCII dashboard every 2s from latest snapshot  

Using `latest` avoids flooding the console with historical snapshots on restart; alerts still print as they arrive.

---

## 10. End-to-end sequences

### 10.1 Happy path

```
Traffic --produce--> orders.events
Processor --validate--> commit offset
Agent --scrape--> lag≈0 --> HEALTHY snapshot --> Console
```

### 10.2 Lag spike

```
Traffic rate high + PROCESSING_DELAY_MS large
  → end offsets grow faster than commits
  → Agent totalLag crosses WARN then CRITICAL
  → anomalies published
  → Console status + ⚠ ALERT lines
```

### 10.3 Poison message

```
Traffic emits poison order
  → Processor validation fails
  → record copied to orders.events.dlq with status=FAILED
  → main group still commits and continues
  → Agent topic health shows DLQ message count rising
```

---

## 11. Configuration contract

| Variable | Default | Component | Effect |
|---|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | all | Broker address |
| `TRAFFIC_RATE` | `5` | traffic | msgs/sec |
| `POISON_EVERY` | `25` | traffic | poison cadence |
| `PROCESSING_DELAY_MS` | `200` | processor | artificial work |
| `VIGIL_INTERVAL_MS` | `3000` | agent | scrape period |
| `LAG_WARN_THRESHOLD` | `50` | agent | DEGRADED boundary |
| `LAG_CRITICAL_THRESHOLD` | `200` | agent | CRITICAL boundary |

All knobs are environment variables so demos do not require rebuilds.

---

## 12. Operational design

### 12.1 Local infrastructure

- `docker-compose.yml` runs **Apache Kafka 3.8 in KRaft mode** (broker+controller, no ZooKeeper).  
- Advertised listener: `PLAINTEXT://localhost:9092`.  
- Replication factors set to `1` for single-node comfort.

### 12.2 Recommended start order

1. Kafka  
2. Stream Processor (so the group exists before heavy traffic)  
3. Vigil Agent  
4. Health Console  
5. Traffic Simulator  

### 12.3 Shutdown

Each long-running process registers a JVM shutdown hook that flips a `running` flag so loops exit and clients close via try-with-resources.

### 12.4 Logging

SLF4J Simple is used for zero-config demos. Production would swap to structured logging + correlation IDs.

---

## 13. Testing strategy

| Layer | What | Tooling |
|---|---|---|
| Unit | `OrderEvent` factories | JUnit 5 |
| Unit | `HealthClassifier` thresholds & alert types | JUnit 5 |
| Manual E2E | 4-process demo + lag spike | Docker + scripts |
| Future | Embedded Kafka / Testcontainers integration tests | deferred (needs Docker in CI) |

**Design choice:** Keep classification pure so correctness of the “product brain” does not depend on broker flakiness.

---

## 14. Alternatives considered

| Alternative | Decision | Reason |
|---|---|---|
| Kafka Streams app instead of consumer | Rejected for v1 | Harder to explain lag + commits; Consumer API is clearer for learning |
| JMX-only metrics | Rejected | Less portable; AdminClient matches external agent story |
| Prometheus push gateway | Deferred | Kafka topics already form an event bus for the console |
| Spring Boot microservices | Rejected | Extra magic; plain Java clients show fundamentals |
| Single monolith process | Rejected | Multi-process topology better mirrors distributed reality |
| ZooKeeper Kafka image | Rejected | KRaft is current Kafka operational model |

---

## 15. Trade-offs summary

| Decision | Benefit | Cost |
|---|---|---|
| Polling AdminClient every N seconds | Simple mental model; C3-like scrape | Detection latency ≈ interval |
| JSON payloads | Debuggable | Weaker schema evolution |
| Artificial processing delay | Deterministic lag demos | Not realistic CPU load |
| Single broker RF=1 | Fast setup | Cannot demo under-replication truthfully |
| Fat JARs | Easy `java -jar` story | Larger artifacts |
| Hard-coded watched group/topics | Clear scope for ownership | Not auto-discovery |

---

## 16. Risks, edge cases, mitigations

| Risk / edge case | Impact | Mitigation |
|---|---|---|
| Processor down while traffic runs | Lag CRITICAL (noisy) | Document start order; treat as real signal |
| No committed offsets yet | Inflated lag | Accept conservative estimate; settles after first commit |
| Thresholds too low/high for hardware | Flappy status | Env-tunable thresholds |
| Poison JSON malformed beyond domain flag | Consumer deserialize errors | Domain poison is structured; serde failures still possible → future DLQ deserializer |
| Console group offsets with `latest` | Miss alerts during downtime | Acceptable for dashboard UX; could switch to `earliest` for audit |
| Clock skew on `Instant.now()` | Cosmetic only | Single-machine demo |

---

## 17. Security & privacy (v1 note)

v1 is **local plaintext**. For a production-shaped design review, call out required follow-ups:

- TLS for brokers and clients  
- SASL/OAuth authentication  
- ACLs limiting agent to describe/list offsets + produce to health topics  
- No PII in health topics (orders currently include customer ids — sanitize for real deployments)

---

## 18. Mapping to IBM / Confluent role themes

| Job theme | Kafka Vigil counterpart |
|---|---|
| USM Agent / hybrid management | `vigil-agent` scrape + publish loop |
| C3 cluster health | `HealthSnapshot.overallStatus` + summary |
| C3 topic visibility | `TopicHealth` (partitions, end-offset totals) |
| C3 consumer monitoring | `ConsumerGroupLag` / `PartitionLag` |
| Mentored ownership of a sub-component | `HealthClassifier` + agent collect path |
| Design one-pager habit | This document |
| Tests + craftsmanship | Unit tests + DLQ + idempotent produce |
| On-call / troubleshoot with logs & metrics | Console + agent logs during lag spike drill |

---

## 19. Success criteria (acceptance)

| Criterion | Status |
|---|---|
| Multi-module Maven build on Java 21 | Done |
| Unit tests pass without broker | Done |
| Docker Compose Kafka boots | Requires Docker on host |
| E2E: HEALTHY under balanced load | Demo |
| E2E: DEGRADED/CRITICAL under delay spike | Demo |
| Anomalies printed in console | Demo |
| DLQ receives poison orders | Demo |
| README + run script | Done |
| Design doc for interview | Done (this file) |

---

## 20. Roadmap / follow-ups

Prioritized for interview storytelling (“what I’d build next”):

1. **OpenMetrics `/metrics` on Vigil Agent** — scrape lag gauges alongside Kafka health topics.  
2. **Dynamic discovery** — list all consumer groups/topics (C3 browser lite).  
3. **Schema Registry + Avro** — evolve `OrderEvent` safely.  
4. **Under-replication / ISR signals** — real RF>1 cluster or simulated broker faults.  
5. **Kubernetes Deployments + ConfigMaps** — hybrid agent packaging story.  
6. **DLQ reprocessor CLI** — close the reliability loop.  
7. **Integration tests with Testcontainers** — CI confidence.  
8. **Simple web console** — still optional; terminal remains the teaching UI.

---



---

## 21. Appendix — key formulas & invariants

**Lag (per partition):**

\[
\text{lag}_{p} = \max(0,\ \text{endOffset}_{p} - \text{committedOffset}_{p})
\]

**Group lag:**

\[
\text{totalLag} = \sum_{p \in P} \text{lag}_{p}
\]

**Invariants we aim to preserve:**

1. Poison/invalid domain events do not prevent commits for successfully handled peers in the same poll (per-record handling then batch commit).  
2. Health snapshots are always published on a fixed interval while the agent is up (even when HEALTHY).  
3. Alert topics carry **events** (state transitions / breaches), while health topic carries **periodic state**.  
4. Classifier thresholds satisfy `0 < warn < critical` or construction fails fast.

---

## 22. Appendix — glossary

| Term | Meaning |
|---|---|
| **Consumer group** | Coordinated set of consumers sharing work on a topic |
| **Committed offset** | Last offset the group marked as processed |
| **End offset** | Next offset that would be assigned to a new produce (log high-water for our purposes) |
| **Lag** | How far the group is behind the log end |
| **DLQ** | Dead-letter queue for failed records |
| **KRaft** | Kafka’s ZooKeeper-less metadata mode |
| **AdminClient** | Kafka API for cluster/topic/group metadata operations |
| **USM** | Unified Stream Manager (hybrid management/observability charter) |
| **C3** | Confluent Control Center |

---

*End of design document.*
