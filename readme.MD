# Distributed Message Queue

A Kafka-inspired distributed message queue built from scratch in Java 17 and Spring Boot 3. Implements core distributed systems concepts including Raft consensus, consistent hashing, leader-follower replication, ISR tracking, and consumer group coordination — all without using Kafka, ZooKeeper, or any message queue library.

> Built as a flagship backend engineering project to demonstrate deep understanding of distributed systems internals.

---

## Table of Contents

- [Architecture](#architecture)
- [Features](#features)
- [Tech Stack](#tech-stack)
- [Project Structure](#project-structure)
- [Getting Started](#getting-started)
- [Running a 3-Broker Cluster](#running-a-3-broker-cluster)
- [API Reference](#api-reference)
- [Design Decisions](#design-decisions)
- [Key Bugs Fixed](#key-bugs-fixed)

---

## Architecture

```
┌──────────────┐     ┌──────────────────────────────────────────┐     ┌──────────────┐
│  Producer A  │────▶│              3-Broker Cluster            │────▶│  Consumer 1  │
│  Producer B  │     │                                          │     │  Consumer 2  │
└──────────────┘     │  ┌──────────────────────────────────┐    │     │  Consumer 3  │
                     │  │   broker-1 (Raft Leader)         │    │     └──────────────┘
                     │  │   · Accepts writes               │    │
                     │  │   · ConsistentHashRing           │    │
                     │  │   · ConsumerGroupCoordinator     │    │
                     │  │   · ISRTracker                   │    │
                     │  └──────────┬──────────┬────────────┘    │
                     │    replicate│          │replicate        │
                     │  ┌──────────▼──┐  ┌────▼───────────┐     │
                     │  │  broker-2   │  │   broker-3     │     │
                     │  │  (follower) │  │   (follower)   │     │
                     │  │  307 redir  │  │   ISR member   │     │
                     │  └──────────┬──┘  └───┬────────────┘     │
                     │             │         │                  │
                     │  ┌──────────▼─────────▼─────────────┐    │
                     │  │   Append-only Log (FileChannel)  │    │
                     │  │   LogSegment · OffsetIndex       │    │
                     │  │   RetentionManager               │    │
                     │  └──────────────────────────────────┘    │
                     └──────────────────────────────────────────┘
                                        │
                               ┌────────▼────────┐
                               │   PostgreSQL    │
                               │  Topics·Offsets │
                               └─────────────────┘
```

---

## Features

### Core Storage Engine
- **Append-only log** using Java NIO `FileChannel` — O(1) writes, crash-safe by design
- **Sparse offset index** using `TreeMap` — maps offset → byte position, O(log n) lookup without storing every message
- **Log segment rolling** — partitions split into multiple sealed segments for efficient retention
- **Time-based and size-based retention** — segments deleted in O(1) without rewriting the log

### Distributed Cluster
- **Raft-lite leader election** — randomised 300–700ms timeouts with fixed deadline generation; 3-phase election (candidate → vote collection → leader check) prevents split-brain
- **Mesh-join registry** — each broker announces itself to all peers on startup; every broker maintains a complete cluster view
- **Consistent hashing** — FNV-1a, 150 virtual nodes per broker, `ConcurrentSkipListMap` ring; O(log n) partition lookup with minimal reshuffling on node join/leave
- **Leader-follower replication** — parallel `CompletableFuture` HTTP replication with configurable timeout; leader waits for ISR majority before confirming write
- **ISR tracking** — lag-based In-Sync Replica membership; followers added/removed based on offset lag and heartbeat health
- **HTTP 307 routing** — followers redirect producers to the current Raft leader automatically

### Consumer Group Coordination
- **Range partition assignment** — deterministic, alphabetically sorted; same input always produces identical assignment regardless of which broker computes it
- **Generation ID** — increments on every rebalance; prevents zombie consumers (paused then reawakened) from committing stale offsets
- **Background member eviction** — detects timed-out consumers (no heartbeat > `sessionTimeoutMs`) and triggers automatic rebalance
- **Graceful leave** — immediate rebalance on consumer shutdown instead of waiting for session timeout

### Operational
- **Consumer lag metrics** — per-group, per-partition lag with status (CAUGHT_UP / SLIGHTLY_BEHIND / BEHIND / CRITICALLY_BEHIND)
- **Storage metrics** — per-topic, per-partition disk usage and segment counts
- **Graceful shutdown** — Raft step-down + peer deregistration + FileChannel flush before process exit
- **Broker restart recovery** — partition logs restored from DB metadata + disk files on rejoin
- **Structured error responses** — precise HTTP status codes (403 ownership violation, 409 stale generation, 503 rebalancing) with actionable error codes

---

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Language | Java 17 |
| Framework | Spring Boot 3.5 |
| Storage (metadata) | PostgreSQL 18 + Spring Data JPA |
| Storage (messages) | Java NIO FileChannel (append-only log) |
| Build | Maven |
| Utilities | Lombok |

No Kafka. No ZooKeeper. No message queue library.

---

## Project Structure

```
src/main/java/com/mq/
│
├── cluster/
│   ├── BrokerRegistry.java         # tracks all known brokers
│   ├── ConsistentHashRing.java     # FNV-1a, 150 virtual nodes, ConcurrentSkipListMap
│   └── PartitionMetadata.java      # partition → broker assignment table
│
├── config/
│   ├── BrokerStartupConfig.java    # mesh-join + partition log restore on startup
│   └── RestTemplateConfig.java
│
├── consumergroup/
│   ├── ConsumerGroupCoordinator.java  # join/leave/heartbeat/rebalance
│   ├── ConsumerGroup.java             # group state, members, assignments
│   ├── ConsumerGroupMember.java       # per-member heartbeat + assignment
│   ├── GroupState.java                # EMPTY / PREPARING_REBALANCE / STABLE
│   └── dto/                           # JoinGroupRequest/Response, HeartbeatGroup*, etc.
│
├── controller/
│   ├── TopicController.java
│   ├── ProducerController.java        # 307 redirect to leader if follower
│   ├── ConsumerController.java        # /consume + /group-consume
│   ├── ConsumerGroupController.java   # /groups/** REST API
│   ├── RaftController.java            # /raft/vote + /raft/heartbeat + /raft/state
│   ├── ReplicationController.java     # /replication/replicate + /init-partition
│   ├── BrokerController.java          # /cluster/brokers/join + /cluster/assignments
│   ├── MetricsController.java         # /metrics/cluster + /metrics/lag/**
│   └── ShutdownController.java        # /admin/shutdown + /admin/health
│
├── raft/
│   ├── RaftNode.java                  # 3-phase election, heartbeat, step-down
│   └── RaftState.java                 # FOLLOWER / CANDIDATE / LEADER
│
├── replication/
│   ├── ReplicationService.java        # parallel CompletableFuture replication
│   └── ISRTracker.java                # lag-based ISR membership (threshold=1)
│
├── service/
│   ├── TopicService.java
│   ├── ProducerService.java
│   ├── ConsumerService.java           # consume() + consumeAsGroupMember()
│   ├── MetricsService.java            # lag + storage stats
│   └── PartitionSelector.java         # key hash + round-robin
│
└── storage/
    ├── LogManager.java                # partition lifecycle, read/write, retention
    ├── LogSegment.java                # single .log file + FileChannel management
    ├── OffsetIndex.java               # sparse TreeMap index
    ├── MessageRecord.java
    └── RetentionManager.java          # time-based + size-based segment deletion
```

---

## Getting Started

### Prerequisites

- Java 17+
- Maven 3.8+
- PostgreSQL (running on port 5432)

### Database Setup

```sql
CREATE DATABASE mqdb;
```

Spring Boot auto-creates tables via `spring.jpa.hibernate.ddl-auto=update`.

### Create Log Directories

```bash
# Windows
mkdir C:\tmp\mq-data-1
mkdir C:\tmp\mq-data-2
mkdir C:\tmp\mq-data-3

# Linux / Mac
mkdir -p /tmp/mq-data-{1,2,3}
```

### Build

```bash
mvn clean compile
```

---

## Running a 3-Broker Cluster

Each broker is the same JAR, launched with different properties. Open **3 terminals**.

**Terminal 1 — broker-1 (port 8082)**
```bash
mvn spring-boot:run \
  -Dspring-boot.run.jvmArguments="\
    -Dserver.port=8082 \
    -Dbroker.id=broker-1 \
    -Dmq.data.dir=C:/tmp/mq-data-1"
```

**Terminal 2 — broker-2 (port 8083)**
```bash
mvn spring-boot:run \
  -Dspring-boot.run.jvmArguments="\
    -Dserver.port=8083 \
    -Dbroker.id=broker-2 \
    -Dmq.data.dir=C:/tmp/mq-data-2"
```

**Terminal 3 — broker-3 (port 8084)**
```bash
mvn spring-boot:run \
  -Dspring-boot.run.jvmArguments="\
    -Dserver.port=8084 \
    -Dbroker.id=broker-3 \
    -Dmq.data.dir=C:/tmp/mq-data-3"
```

After ~3 seconds, exactly one broker will log `|| BECAME LEADER for term 1 ||`. The others will log `Stepping down to FOLLOWER`.

---

## API Reference

### Topics

```
POST   /api/v1/topics
       Body: { "name": "orders", "partitionCount": 6 }
       → 201 Created

GET    /api/v1/topics
       → list of all topics
```

### Producer

```
POST   /api/v1/producer/produce
       Body: { "topicName": "orders", "value": "order-001" }
              optionally: "key": "user-123"  ← enables key-based routing
       → 201 { "partition": 3, "offset": 0, "topicName": "orders" }
       
       If sent to a follower → 307 redirect to leader automatically
```

### Consumer

```
# Direct consume (specify partition explicitly)
GET    /api/v1/consumer/consume
       ?topic=orders&group=my-group&partition=3
       ?offset=-1   ← resume from committed offset (default)
       ?offset=0    ← replay from beginning
       → 200 { messages: [...] }  |  204 No Content (caught up)

# Group-coordinated consume (validates membership + ownership + generation)
GET    /api/v1/consumer/group-consume
       ?topic=orders&groupId=payment-svc&consumerId=consumer-1
       ?partition=0&generation=3
       → 200 messages  |  204 caught up  |  403 wrong partition
       → 409 stale generation (rejoin)   |  503 rebalancing (retry)

# Check committed offset + lag for a group/partition
GET    /api/v1/consumer/offset
       ?topic=orders&group=payment-svc&partition=0
       → { committedOffset, latestOffset, lag }
```

### Consumer Groups

```
POST   /api/v1/groups/{groupId}/join
       Body: { "groupId":"payment-svc","consumerId":"c-1","topicName":"orders","sessionTimeoutMs":30000 }
       → { generation, groupState, assignedPartitions }

POST   /api/v1/groups/{groupId}/heartbeat
       Body: { "groupId":"payment-svc","consumerId":"c-1","generation":3 }
       → { success, rebalanceRequired, generation }
       If rebalanceRequired=true → call /join again

POST   /api/v1/groups/{groupId}/leave
       Body: { "groupId":"payment-svc","consumerId":"c-1" }
       → triggers immediate rebalance for remaining members

GET    /api/v1/groups/{groupId}/assignment?consumerId=c-1
       → { assignedPartitions, generation, groupState }

GET    /api/v1/groups/{groupId}/status
       → { state, generation, memberCount, assignments, heartbeatAge }

GET    /api/v1/groups
       → list of all group IDs
```

### Cluster & Replication

```
GET    /api/v1/raft/state
       → { state, isLeader, term, currentLeader, brokerId }

GET    /api/v1/cluster/brokers
       → all registered brokers

GET    /api/v1/cluster/assignments
       → partition → broker routing table

GET    /api/v1/cluster/replication/{topic}/{partition}
       → { followers: { broker-2: { replicatedOffset, lag, inISR } }, ISR: [...] }
```

### Metrics

```
GET    /api/v1/metrics/cluster
       → topic count, total messages, total disk usage, retention policy

GET    /api/v1/metrics/topics/{topicName}
       → per-partition size, segment count, latest offset

GET    /api/v1/metrics/lag/{groupId}/{topicName}
       → { totalLag, lagStatus, lagByPartition }

GET    /api/v1/metrics/lag/{topicName}
       → lag for ALL groups on this topic side by side

GET    /api/v1/metrics/retention
       → current retention policy config

POST   /api/v1/metrics/retention/run
       → trigger retention check immediately (for testing)
```

### Admin

```
GET    /api/v1/admin/health
       → { status, brokerId, raftState, isLeader, term }

POST   /api/v1/admin/shutdown
       → graceful shutdown: Raft step-down → peer deregister → Spring context close
```

---

## Configuration Reference

`application.properties` (key settings — same file for all brokers, override via JVM args):

```properties
server.port=8082
broker.id=broker-1
broker.host=localhost
mq.data.dir=C:/tmp/mq-data-1

# Cluster peers (all 3 addresses on every broker — self excluded automatically)
cluster.peers=localhost:8082,localhost:8083,localhost:8084

# Replication
replication.factor=3
replication.timeout.ms=5000

# Retention
retention.hours=168
retention.max.bytes=1073741824
retention.check.interval.ms=300000
```

---
