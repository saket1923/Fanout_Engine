## Fanout Engine (Java 21)

### 1. Setup & Running

- **Prerequisites**
  - **Java**: 21+
  - **Maven**: 3.9+

- **Build**

```bash
mvn clean package
```

- **Run**

From the project root:

```bash
java -cp target/fanout-engine-0.1.0-SNAPSHOT.jar com.fanout.engine.Orchestrator
```

By default the `Orchestrator` uses:
- A `BlockingQueue<Record>` with capacity `10_000`
- Mock sinks (`REST`, `GRPC`, `MQ`, `DB`)
- `FileIngestor` reading from an input file path resolved by `ConfigLoader`

- **Configuration**
  - Application settings are defined in `src/main/resources/application.yaml` (currently a placeholder; `ConfigLoader` does not yet fully parse this file).
  - System properties can be used as fallbacks for some values:
    - **Input file path**: `-Dfanout.inputFilePath=/path/to/input.csv`
    - **Global sink rate limit**: `-Dfanout.rateLimitRps=100`
    - **Per-sink rate limits**:
      - `-Dfanout.sinks.rest.rateLimitRps=50`
      - `-Dfanout.sinks.grpc.rateLimitRps=50`
      - `-Dfanout.sinks.mq.rateLimitRps=100`
      - `-Dfanout.sinks.db.rateLimitRps=1000`

> Note: Wiring `ConfigLoader` to actually read `application.yaml` can be added later; right now the YAML file documents intended settings and serves as a starting point.

---

### 2. Architecture Overview

- **High-level pipeline**

```text
           +-------------------+
           |  application.yaml |
           +---------+---------+
                     |
                     v
             +---------------+
             | ConfigLoader  |  (stubbed: provides simple accessors)
             +-------+-------+
                     |
                     v
            +-------------------+
            |   FileIngestor    |
            |  - CSV / JSONL    |
            |  - streaming I/O  |
            +---------+---------+
                      |
                      v
          +--------------------------+
          |  BlockingQueue<Record>   |
          +-------------+------------+
                        |
                        v
        +------------------------------+
        |  Orchestrator (consumers)   |
        |  - virtual threads          |
        |  - metrics loop             |
        +------------------------------+
          |            |            |
          v            v            v
   +------------+ +------------+ +------------+  +-----------------+
   | RestApiSink| | GrpcSink   | | MQ Sink    |  | WideColumnDbSink|
   +-----+------+ +-----+------+ +-----+------+  +---------+-------+
         |              |              |                  |
         v              v              v                  v
  RestTransformer  GrpcTransformer  MqTransformer    DbTransformer
         \              |              |                  /
          +-------------+--------------+-----------------+
                          ^
                          |
                 TransformerFactory
```

- **Key packages**
  - `com.fanout.model`: core data model (`Record`)
  - `com.fanout.engine`: `Orchestrator`, `FileIngestor`
  - `com.fanout.transformers`: `Transformer` interface, concrete transformers, `TransformerFactory`
  - `com.fanout.sinks`: sink interface + mock sink implementations
  - `com.fanout.utils`: supporting utilities (`ConfigLoader`)

---

### 3. Design Decisions

- **Streaming ingestion**
  - **Why**: avoid loading entire files into memory; support large CSV/JSONL inputs.
  - **How**: `FileIngestor` uses `BufferedReader` to read line-by-line and pushes `Record` instances into a `BlockingQueue<Record>`.
  - **Backpressure**: queue capacity (10,000 by default) plus `queue.put(...)` provides basic backpressure to the ingestion side.

- **Virtual-thread concurrency**
  - **Why**: Java 21 virtual threads allow many lightweight consumers without complex thread-pool tuning.
  - **How**:
    - `Orchestrator` starts:
      - A virtual thread for `FileIngestor.ingestFile()`.
      - A set of virtual-thread consumers that poll from `BlockingQueue<Record>` and fan out to sinks.
    - A `ScheduledExecutorService` (platform thread) prints metrics every 5 seconds.

- **Strategy pattern for transformations**
  - **Why**: decouple sink types from transformation logic; make it easy to add new sinks.
  - **How**:
    - `Transformer<T>` interface abstracts "record → payload" conversion.
    - Concrete strategies:
      - `RestTransformer` → JSON (`String`)
      - `GrpcTransformer` → Protobuf-like mock (`String`)
      - `MqTransformer` → XML (`String`)
      - `DbTransformer` → Map-based mock Avro/CQL (`Map<String,Object>`)
    - `TransformerFactory` chooses the correct transformer based on sink type (`REST`, `GRPC`, `MQ`, `DB`).

- **Mock sinks with rate limiting & retries**
  - **Why**: simulate realistic external systems (REST, gRPC, MQ, DB) while keeping the engine self-contained and safe to run locally.
  - **How**:
    - `Sink<T>` interface exposes `sinkType()` and `send(T payload)`.
    - Mock sinks (`RestApiSink`, `GrpcSink`, `MessageQueueSink`, `WideColumnDbSink`) implement:
      - **Rate limiting** with `Thread.sleep()` and a simple records-per-second budget.
      - **Random failures** (~10% simulated) plus up to **3 retries** with small backoff.
      - **Console logging** for sends, retries, and failures.

- **Metrics & observability**
  - `Orchestrator` uses `AtomicLong` counters:
    - Total records processed.
    - Per-sink success/failure.
  - A scheduled metrics task prints:
    - Total processed.
    - Throughput (records/sec) over the last interval.
    - Success/failure counts per sink.

---

### 4. Assumptions & Limitations

- **Input formats**
  - **CSV**:
    - Expected columns (in order): `id,name,email,timestamp`.
    - Parsing is simple: `String.split(",", -1)`; no quoting/escaping support yet.
    - Timestamp is assumed to be ISO-8601 (e.g. `2026-02-17T10:00:00Z`).
  - **JSONL / NDJSON**:
    - Each line is a JSON object with keys: `id`, `name`, `email`, `timestamp`.
    - Timestamp again expected in ISO-8601 form.
  - Blank or malformed lines are skipped with a log message.

- **Configuration**
  - `application.yaml` holds intended config values (input file, per-sink rate limits, thread pool size), but:
    - `ConfigLoader.load()` is currently a stub that returns an empty map.
    - `ConfigLoader` **does** provide accessors like `getInputFilePath()` and `getRateLimitRps(...)`, which can be backed by YAML parsing later.
  - System properties are supported as an immediate way to feed configuration without a full YAML loader.

- **Mock sinks**
  - All sinks are **in-memory mocks**:
    - No real HTTP, gRPC, MQ, or DB connections are made.
    - Payloads are printed to stdout/stderr to simulate external interactions.
  - Retry and failure simulation are intentionally simplistic and non-configurable (fixed ~10% failure probability and max 3 attempts).

- **Threading and shutdown**
  - Virtual-thread consumers and the ingestor are designed for a "batch" run:
    - Ingestor finishes when the file is fully read.
    - Consumers exit once ingestion is done and the queue is empty.
    - Executors are then shut down and a final summary is printed.
  - This is **not yet** tuned for long-lived streaming or hot-reload of configuration.

---

### 5. AI Prompts Used to Generate Code

The following user prompts were used to generate the code and project structure in this repository:

1. **Initial project skeleton**

   > Create a Java 21 Maven project inside this folder with the following:
   >
   > 1. Packages:
   >    - com.fanout.engine
   >    - com.fanout.model
   >    - com.fanout.transformers
   >    - com.fanout.sinks
   >    - com.fanout.utils
   >
   > 2. Classes/Interfaces:
   >    - Record (model): placeholder fields id, name, email, timestamp
   >    - Transformer (interface) with method: Record transform(Record record)
   >    - Sink (interface) with method: void send(Record record)
   >    - Orchestrator (main engine class)
   >    - ConfigLoader (utility for reading application.yaml)
   >
   > 3. Structure:
   >    - src/main/java/… for packages and classes
   >    - src/main/resources/application.yaml (empty placeholder)
   >    - src/test/java/… (empty test package structure)
   >
   > 4. pom.xml with dependencies for:
   >    - YAML parsing (e.g., SnakeYAML)
   >    - JUnit for testing
   >    - Optional: Jackson (JSON processing)
   >    - Include Maven compiler plugin for Java 21
   >
   > 5. Each class should have:
   >    - Placeholders for methods (without full logic)
   >    - Proper package declaration
   >    - Empty constructors where needed
   >
   > Do not implement the full file reading or sink logic yet. Focus only on **project structure, packages, classes, interfaces, and pom.xml** so I can start building on top of it.

2. **File ingestion**

   > Inside package com.fanout.engine, create FileIngestor.java with:
   >
   > 1. Fields:
   >    - BlockingQueue<Record> queue
   >    - String inputFilePath (read from ConfigLoader)
   >
   > 2. Constructor to accept BlockingQueue and ConfigLoader
   >
   > 3. Method: void ingestFile() that:
   >    - Reads CSV or JSONL line by line (streaming)
   >    - Converts each line to a Record object (use simple parsing, e.g., split by comma for CSV)
   >    - Pushes each Record into the queue
   >    - Handles IOExceptions gracefully
   >    - Does NOT load entire file into memory
   >
   > 4. Add simple logging: print every 1000 records ingested

3. **Transformation layer**

   > Inside package com.fanout.transformers, implement the Transformation Layer:
   >
   > 1. Interface: Transformer.java
   >    - Method: Record transform(Record record);
   >
   > 2. Concrete transformer classes:
   >    a) RestTransformer.java → converts Record to JSON string (using Jackson)
   >    b) GrpcTransformer.java → converts Record to Protobuf-like mock (string format)
   >    c) MqTransformer.java → converts Record to XML string (simple manual XML)
   >    d) DbTransformer.java → converts Record to Avro/CQL mock map (e.g., Map<String,Object>)
   >
   > 3. Implement TransformerFactory.java with method:
   >    Transformer getTransformer(String sinkType)
   >    - Returns correct transformer instance based on sinkType: "REST", "GRPC", "MQ", "DB"
   >
   > 4. Keep it simple:
   >    - The transform() method returns the transformed object/string but does NOT send it
   >    - Include minimal logging for debug (optional)

4. **Mock sinks**

   > Inside package com.fanout.sinks, implement the Mock Sinks:
   >
   > 1. Sink interface already exists: void send(Record record);
   >
   > 2. Create 4 concrete sinks:
   >
   > a) RestApiSink.java
   >    - Uses RestTransformer
   >    - Simulates HTTP POST by printing transformed JSON
   >    - Configurable rate limit (records/sec) from ConfigLoader
   >    - Retry failed sends up to 3 times (simulate failure randomly ~10%)
   >
   > b) GrpcSink.java
   >    - Uses GrpcTransformer
   >    - Simulates gRPC streaming by printing
   >    - Rate limit + retry same as above
   >
   > c) MessageQueueSink.java
   >    - Uses MqTransformer
   >    - Simulates publish to topic (print XML)
   >    - Rate limit + retry
   >
   > d) WideColumnDbSink.java
   >    - Uses DbTransformer
   >    - Simulates async UPSERT to DB (print Map)
   >    - Rate limit + retry
   >
   > 3. Implementation details:
   >    - Use Thread.sleep() to implement simple rate limiting per sink
   >    - Randomly fail some sends to test retry logic
   >    - Ensure send() blocks if necessary to respect backpressure
   >    - Log successful sends and failures for metrics

5. **Orchestrator wiring**

   > Inside com.fanout.engine, update Orchestrator.java to integrate the full fan-out pipeline:
   >
   > 1. Fields:
   >    - BlockingQueue<Record> queue
   >    - List<Sink> sinks
   >    - FileIngestor ingestor
   >    - ExecutorService or VirtualThread-per-task for concurrency
   >    - AtomicLong counters for metrics (processed, success, failure per sink)
   >
   > 2. In main run() method:
   >    - Start FileIngestor.ingestFile() in its own thread
   >    - Start a pool of Virtual Threads to consume from the queue:
   >        a) For each Record:
   >            i) For each Sink:
   >                - Get the Transformer from TransformerFactory
   >                - Transform the Record
   >                - Call Sink.send(transformedRecord)
   >    - Handle exceptions properly
   >
   > 3. Metrics:
   >    - ScheduledExecutorService prints every 5 seconds:
   >        - Total records processed
   >        - Throughput (records/sec)
   >        - Success/failure per sink
   >
   > 4. Graceful shutdown:
   >    - Wait until FileIngestor finishes
   >    - Wait until queue is empty
   >    - Shut down executor
   >    - Print final summary of total records, successes, failures

6. **Configuration & sample data**

   > 1. Create src/main/resources/application.yaml with:
   >
   > inputFile: "src/main/resources/sample.csv"
   > sinks:
   >   rest:
   >     rateLimitRps: 50
   >   grpc:
   >     rateLimitRps: 50
   >   mq:
   >     rateLimitRps: 100
   >   db:
   >     rateLimitRps: 1000
   > threadPool:
   >   size: 8
   >
   > 2. Create a sample CSV file src/main/resources/sample.csv with headers:
   > id,name,email,timestamp
   > And 5 sample rows, e.g.,
   > 1,Alice,alice@example.com,2026-02-17T10:00:00Z
   > 2,Bob,bob@example.com,2026-02-17T10:01:00Z
   > 3,Charlie,charlie@example.com,2026-02-17T10:02:00Z
   > 4,Dana,dana@example.com,2026-02-17T10:03:00Z
   > 5,Eve,eve@example.com,2026-02-17T10:04:00Z

