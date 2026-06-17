# Technical Design Document (TDD): Project Kervan-Proxy (Resilient Stream Pipeline SDK)

## 1. Executive Summary & Philosophical Dedication
**Kervan-Proxy** is a lightweight, zero-dependency, protocol-agnostic Layer 7 (L7) application-level proxy and circuit buffering engine implemented in Go. 

The project draws its inspiration from the ancient Silk Road **Caravanserais** (*Kervansaray*). In historical trade routes, a caravanserai acted as a secure sanctuary, shielding valuable commercial caravans and their cargo from unpredictable desert storms, bandits, and harsh environmental shifts. Once the path to the final destination became safe, the caravan resumed its journey safely.

Analogously, **Kervan-Proxy** treats network payloads as valuable cargo traversing unpredictable cloud infrastructures. When an upstream backend service undergoes a deployment rollout or suffers a sudden infrastructure outage, Kervan-Proxy acts as a digital caravanserai. Instead of severing the connection to the client (the source), Kervan-Proxy isolates the failure, halts upstream transmission, swallows incoming payloads into a highly optimized, stateful, in-memory buffer, and seamlessly flushes them in a strict FIFO order to the target once structural health is restored.

---

## 2. System Architecture & Core Primitives

Kervan-Proxy operates as an inline, reactive stream-processing pipeline. It abstracts network endpoints away from static configurations, giving developers programmatic control over the life-cycle of long-lived stream states (e.g., WebSockets, gRPC streams, or raw TCP sockets).

### Core Architectural Components
1. **Pipeline**: The logical streaming container encapsulating one distinct `Source` (Client) and one distinct `Target` (Upstream/Backend).
2. **Valve**: The internal execution gatekeeper controlling data routing states. The Valve implements a finite state machine with three behaviors: `OPEN`, `HELD`, and `DRAINING`.
3. **Sanctuary**: A thread-safe, memory-mapped, bounded in-memory FIFO ring-buffer attached specifically to each Pipeline instance.
4. **Interceptors (Rule Engine)**: Developer-defined predicate hooks that dynamically evaluate telemetry, connection health events, or message payload structures using an event-driven `WHEN-THEN` specification pattern.

### Operational State Machine Flowchart

```text
           +-------------------------------------------+
           |               STATE: OPEN                  |
           |  Source Connections stream directly to     |
           |  Target via low-latency zero-copy pipes.   |
           +---------------------++--------------------+
                                 ||
             Rule Triggered:     ||  Target Failure /
             WHEN Condition Met  ||  Network Interruption
                                 \/
           +-------------------------------------------+
           |               STATE: HELD                 |
           |  Upstream Pipe is frozen. Source Socket   |
           |  remains alive, payloads write into the   |
           |  Sanctuary In-Memory Bounded FIFO Buffer. |
           +---------------------++--------------------+
                                 ||
             Rule Triggered:     ||  Target Reconnected /
             WHEN Condition Met  ||  Handshake Validated
                                 \/
           +-------------------------------------------+
           |             STATE: DRAINING               |
           |  Sanctuary Buffer flushes to Target       |
           |  sequentially while Source continues      |
           |  appending to the tail of the buffer.     |
           +---------------------++--------------------+
                                 ||
                                 ||  Sanctuary Buffer
                                 ||  Hits Empty (0)
                                 \/
```

---

## 3. The Rule Engine Topology ("When-Then" Spec)

Kervan-Proxy implements a non-blocking Rule Engine executing user-defined predicates on the execution hot path. It shifts the definition of connection resilience out of infrastructure configuration and places it directly into application-level programming.

### Rule Evaluation Pipeline

```text
[Event / Network Exception / Payload Inspection] 
                       │
                       ▼
         ┌──────────────────────────┐
         │ Interceptor Evaluation   │ ──► Returns Boolean (True/False)
         └──────────────────────────┘
                       │
               ┌───────┴───────┐
         True  │               │  False
               ▼               ▼
   ┌───────────────────────┐   ┌───────────────────────┐
   │ State Mutation Event  │   │ No Action / Continuous│
   │ (e.g., OPEN -> HELD)  │   │ Streaming             │
   └───────────────────────┘   └───────────────────────┘

```

### Programmatic Spec Formats
Developers supply concrete predicates during Pipeline instantiation. The Rule Engine evaluates these conditions using structured state hooks.

Rule Spec 1: Handling Target Degradation

```go
// Declarative Pseudo-Configuration
Pipeline.RegisterRule(
    WHEN(Target.ReadErrors.Consecutive() > 3 || Target.Latency > 2500*time.Millisecond),
    THEN(Valve.TransitionTo(HELD)),
    ON_TRANSITION(Target.ExecuteAncillaryHealthChecks()),
)
```

Rule Spec 2: Restoring Normalcy

```go
Pipeline.RegisterRule(
    WHEN(Target.Status == NetworkConnected && Target.ProtocolHandshake == Success),
    THEN(Valve.TransitionTo(DRAINING)),
)
```

Rule Spec 3: Enforcing Boundary Backpressure

```go
Pipeline.RegisterRule(
    WHEN(Sanctuary.MemoryFootprint() >= 64*MB || Sanctuary.Length() >= 10000),
    THEN(Action.ApplyBackpressure(DropOldest / RejectNewSourceWrites)),
)
```

## 4. Deep-Dive Internal Mechanics & Memory Optimization

To process thousands of stateful streaming pipelines concurrently without generating massive Garbage Collection (GC) pauses or crashing due to Out-Of-Memory (OOM) triggers, Kervan-Proxy implements a highly defensive memory-management strategy.

       +-------------------------------------------------------+
       |                  PIPELINE STRUCTURE                   |
       |                                                       |
       |  +-----------------+           +-------------------+  |
       |  | INGESTION PATH  |           |  EXECUTION PATH   |  |
       |  | (Source Reader) |           |  (Target Writer)  |  |
       |  +--------+--------+           +---------+---------+  |
       |           |                              ^            |
       |           v                              |            |
       |    +──────┴──────+ Read/Write            |            |
       |    │ VALVE STATE │ Stream                |            |
       |    +──────┬──────+                       |            |
       |           │                              |            |
       |           ├────► [If OPEN] ──────────────┤            |
       |           │                              |            |
       |           └────► [If HELD] ──► +---------+---------+  |
       |                                | SANCTUARY BUFFER  |  |
       |                                | (In-Memory FIFO)  |  |
       |                                +-------------------+  |
       +-------------------------------------------------------+

### A. The Double-Buffered Conduit
Each Pipeline isolates I/O processing into two lightweight goroutines operating concurrently:

* The Ingestion Path: Bound to reading incoming data payloads from the Source connection. This loop checks the pointer atomic state of the Valve. If the Valve is OPEN, the byte payload is fed directly across an unbuffered internal sync channel to the execution channel. If the Valve is HELD, the ingestion path bypasses the execution channel entirely and serializes the byte payload directly into the Sanctuary.

* The Execution Path: Responsible for formatting and transmitting data to the upstream Target. When the Valve flips to HELD, the execution loop blocks gracefully on a notification channel lock. The source socket continues reading smoothly without noticing TCP window collapses or closed connection frame failures.

### B. High-Performance Zero-Allocation Buffer (sync.Pool)
To prevent runtime memory allocation thrashing on the heap during active buffering sequences, Kervan-Proxy enforces zero-copy payload recycling:

* Pre-allocated Node Segments: Message segments inside the Sanctuary are allocated from a unified sync.Pool hosting fixed size []byte blocks.

* When the ingestion loop captures data from the source, it grabs a recycling slice from the pool, reads the socket directly into it, and links it into the buffer queue.

* When the draining mechanism flushes a segment out to the newly stabilized target, it immediately zeros out the slice data and releases it back to the sync.Pool for subsequent pipeline consumption. This pins the heap allocation footprint to practically zero.

### C. The Bounded Drainage Engine (The FIFO Flush Cycle)
The shift from HELD to DRAINING executes via sequential atomic states:

1. The Pipeline locks the Target selection mutation pointer to guard against split-brain target selection.

2. The Ingestion Path remains active on the source socket, continuing to intercept new payloads and seamlessly pushing them onto the tail of the Sanctuary buffer.

3. A high-throughput optimization loop spins up on the Execution Path, pulling payloads sequentially from the head of the Sanctuary FIFO ring buffer and pumping them down the new Target stream.

4. Throughout the draining operation, the pipeline monitors the length metrics of the buffer.

5. The moment the buffer length successfully zeroes out, the Valve state undergoes an atomic CAS (Compare-And-Swap) operation back to OPEN, dropping the inline buffer out of the processing path and re-establishing microsecond-latency streaming directly between Source and Target.

## 5. Failure Mode Analysis & Edge Case Mitigations

| Failure Vector | Trigger Event | Internal Engine Mitigation |
| :--- | :--- | :--- |
| **Target Poisoning** | Target dies mid-packet transfer while Valve is `OPEN`. | The Execution interceptor catches the socket close event, instantly forces the Valve state to `HELD`, captures the un-acknowledged payload frame, and reroutes it as the first item in the `Sanctuary` FIFO queue. |
| **Buffer Saturation (OOM Risk)** | Target remains dead for prolonged intervals; Source continuously streams large payloads. | The Pipeline triggers the developer-defined Backpressure rule. The engine can execute a `Drop-Oldest` eviction loop, switch to a `Block-Source` pattern by halting reads on the source TCP socket (forcing a native TCP window shutdown), or issue a clean, graceful downstream termination code. |
| **Cascading Target Failures** | Target drops, Valve switches to `HELD`, a new Target is attached, but the new Target dies during the `DRAINING` cycle. | The engine aborts the drain loop immediately. The Valve reverts instantly from `DRAINING` back to `HELD`. The un-flushed payloads remain safe inside the `Sanctuary` without loss of ordering sequence, and the target-seeking loop resets. |
| **Zombie Source Connections** | The client disconnects completely while the proxy is holding its buffer due to an upstream outage. | A localized source keep-alive evaluator monitors downstream socket liveness. If a hard TCP disconnect occurs at the source, the pipeline is torn down, and the pre-allocated `Sanctuary` data segments are immediately flushed back to the global `sync.Pool`. |

