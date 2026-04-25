# CommIPC Architectural Patterns

CommIPC is highly flexible. Because it natively supports RPC, Pub/Sub, load-balanced groups, and high-performance routing, it can be used to implement almost any modern distributed systems architecture.

This document serves as a guide for implementing common architectural patterns using `comm-ipc-rs`.

---

## 1. Scatter-Gather
A central client broadcasts a task to multiple workers. The workers process the task in parallel and return the results to the client.

**Implementation**:
The client publishes an event to a topic including an `aggregation_id`. Multiple workers subscribe to the topic, do the work, and send their results back via a designated RPC channel.

**Rust Example**:
```rust
// Client Side (Aggregator)
let (tx, mut rx) = mpsc::channel(10);
let agg_id = "job-123";

// 1. Subscribe to results
channel.subscribe("results", move |cd| {
    if cd.data["agg_id"] == agg_id {
        let _ = tx.blocking_send(cd.data["result"].clone());
    }
    Ok(json!({"status": "received"}))
}).await?;

// 2. Broadcast task
channel.publish("tasks", json!({"agg_id": agg_id, "payload": "..."})).await?;

// 3. Collect results with timeout
let mut results = Vec::new();
let timeout = sleep(Duration::from_secs(2));
tokio::pin!(timeout);

loop {
    tokio::select! {
        Some(res) = rx.recv() => results.push(res),
        _ = &mut timeout => break,
    }
}
```

---

## 2. Request-Response (RPC)
Standard remote procedure call.

**Implementation**:
Native RPC using `channel.add_event` and `channel.call`.

---

## 3. Publish-Subscribe (Pub/Sub)
One-to-many communication where publishers emit events without knowing who the subscribers are.

**Implementation**:
Native pub/sub using `channel.subscribe` and `channel.publish`.

---

## 4. Event Sourcing
State is determined by a sequence of events.

**Implementation**:
Services publish state-change events. A dedicated "EventStore" service (in Rust or Python) subscribes to these and appends them to a log.

---

## 5. Command Query Responsibility Segregation (CQRS)
Separating mutation (Commands) from retrieval (Queries).

**Implementation**:
Use distinct channels. `write_channel` handles mutations. `read_channel` handles queries and is load-balanced using `channel.group("readers")`.

---

## 6. Saga Pattern (Orchestration)
Managing distributed transactions without 2-phase commit.

**Implementation**:
A Rust coordinator makes sequential `channel.call()` calls. If one fails, it issues compensation calls to the previous services.

---

## 7. Leader-Follower (Worker Groups)
Work is delegated to a cluster of workers.

**Implementation**:
Native load-balancing via `channel.group("workers").provide("task", handler)`. The Hub automatically routes to the least busy follower.
