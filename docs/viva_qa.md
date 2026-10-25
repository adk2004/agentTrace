# Viva Voce & Technical Oral Exam Q&A: AI-Trace BigData

This document compiles **20 rigorous, highly technical questions and comprehensive answers** designed to prepare engineering candidates for a university viva voce or technical defense on the **AI-Trace BigData** project.

---

### Question 1: Why did you choose Apache Hadoop MapReduce and Hive over Apache Spark for this project?
**Answer:**  
1. **Resource Constraints on Local Hardware**: Apache Spark executes through in-memory directed acyclic graph (DAG) pipelines and requires substantial JVM heap allocations (typically 8–16+ GB RAM) to cache RDDs/DataFrames without thrashing or memory spilling. Our physical machine had only 8 GB of RAM total, with only 5 GB allocated to WSL2. Hadoop MapReduce and Hive with low-memory container allocations (768 MB map, 1024 MB reduce) can run sequentially and stream data through disk buffers without exhausting system memory.
2. **Explicit Systems & Algorithmic Engineering**: Spark abstracts away fundamental distributed computing primitives behind high-level Catalyst optimizer transformations and DataFrames. MapReduce forces explicit implementation of distributed design patterns—such as custom `WritableComparable` composite keys, custom partitioners, grouping comparators for Secondary Sort, and associative combiners.
3. **Deterministic Fault Tolerance & Disk Spilling**: MapReduce persists intermediate map outputs to local disk partitions before the shuffle phase. If a node or task fails, only the failed map or reduce task needs to be recomputed, whereas an out-of-memory error in Spark often terminates the entire driver program.

---

### Question 2: What is Secondary Sort, and why was it necessary for the Sessionization Job?
**Answer:**  
In standard MapReduce, the framework sorts intermediate keys before invoking the reducer, but the values associated with a given key arrive at the reducer in arbitrary, non-deterministic order. 

In our `SessionizationJob`, each event represents an agent step or run belonging to a `session_id`. To compute session-level duration, inter-run idle times, and error propagation, the events within each session must be processed in strict chronological order by `start_time_ms`. 

To achieve this without sorting in memory inside the reducer:
1. We formed a **composite key**: `RunTimeKey(sessionId, startTimeMs)`.
2. The framework sorts keys using the composite key comparator: first by `sessionId ASC`, then by `startTimeMs ASC`.
3. A **custom partitioner** hashes only `sessionId` so all records for the same session go to the same reducer.
4. A **custom grouping comparator** compares only `sessionId` so the reducer receives all records for a session in a single `reduce(key, values)` invocation, pre-sorted chronologically.

---

### Question 3: What are the mathematical and algorithmic conditions required for a Combiner to be used safely?
**Answer:**  
A Combiner is a "mini-reducer" that runs on the mapper's local spill files before data is shuffled across the network. For a Combiner to produce correct results identical to running without a Combiner, the aggregation operation must be **associative** and **commutative**:
- **Associative**: $(a \oplus b) \oplus c = a \oplus (b \oplus c)$
- **Commutative**: $a \oplus b = b \oplus a$

**Examples in this project**:
- **Sum, Count, Min, Max**: These are strictly associative and commutative. In `ToolStatsJob`, summing `total_calls`, `error_calls`, and `sum_latency` is valid. In `TokenStatsJob`, computing `min_prompt_tokens` and `max_prompt_tokens` is valid.
- **Why Mean / Average cannot use a Combiner directly**: Average is neither associative nor commutative: $\text{avg}(a, \text{avg}(b, c)) \neq \text{avg}(\text{avg}(a, b), c)$. To compute an average with a Combiner, the mapper and combiner must emit a composite object tracking both the `sum` and the `count` (e.g. `sum_latency` and `total_calls`), allowing the combiner to aggregate partial sums and partial counts. The final division is performed only in the Reducer.

---

### Question 4: What is the exact distinction between a Partitioner and a Grouping Comparator?
**Answer:**  
- **Partitioner (`Partitioner<K, V>`)**:
  - *When it executes*: In the Mapper phase, right before records are written to local disk partitions.
  - *Responsibility*: Determines **which physical Reducer container** receives a key-value pair.
  - *Formula*: `(partitioner.getPartition(key, value, numReducers)) % numReducers`.
  - In our project (`RunIdPartitioner`), it hashes **only** the `sessionId` portion of `RunTimeKey(sessionId, startTimeMs)`. This guarantees that all records for a specific session land on the exact same reducer machine, regardless of timestamp.
- **Grouping Comparator (`WritableComparator`)**:
  - *When it executes*: In the Reducer phase, after the shuffle and merge sort, as records are fed into the `reduce()` method.
  - *Responsibility*: Determines **record boundaries** for the `reduce(K key, Iterable<V> values)` method call.
  - In our project (`RunIdGroupingComparator`), it inspects **only** `sessionId`. Even though consecutive keys have different timestamps (e.g., `(session_1, t=100)` and `(session_1, t=250)`), the grouping comparator treats them as identical, bundling all values into a single `Iterable<V>` for `session_1`.

---

### Question 5: Why did you use the ORC (Optimized Row Columnar) file format in Hive instead of plain text or CSV?
**Answer:**  
1. **Columnar Projection (Read Pruning)**:
   Plain text (CSV/TSV) is row-oriented; scanning a table to aggregate a single column (e.g., `total_tokens`) requires deserializing every byte of every column in the row. ORC stores data in vertical column stripes. When a query accesses only `model` and `prompt_tokens`, Hive reads only those specific column streams from disk, reducing disk I/O by >75%.
2. **Built-in Lightweight Compression**:
   ORC employs column-aware encoding (Run-Length Encoding, Dictionary Encoding, Integer Bit-Packing) combined with Snappy or ZLIB block compression. In our cluster, converting raw TSV to ORC Snappy reduced disk consumption from 194.2 KB to 95.4 KB (**2.04x physical disk reduction**).
3. **Stripe Statistics & Predicate Pushdown**:
   ORC files maintain metadata indexes (including min/max/sum values) for every 10,000 rows (a stripe). When a query has a `WHERE dt = '2026-01-15'` or `WHERE status = 'failed'` clause, the reader checks the stripe index before reading the block and skips entire stripes if the target value falls outside the min/max range.

---

### Question 6: Explain the concept of "Schema-on-Read" vs "Schema-on-Write" and how Hive implements it.
**Answer:**  
- **Schema-on-Write (Traditional RDBMS)**:
  When inserting data into a database like PostgreSQL or Oracle, the schema is enforced at write time. The system parses, validates data types, and checks constraints before storing data on disk. If a row violates the schema, the write is rejected.
- **Schema-on-Read (Apache Hive & Big Data)**:
  Data is written or copied directly into the storage layer (HDFS) as raw, unstructured or semi-structured files (TSV, JSON) without validation. The schema is applied only when the data is read by an analytical query.
- **Hive Implementation**:
  Hive separates metadata from data storage. The Hive Metastore stores the table definition (column names, types, delimiters) in a relational database. When a query runs, Hive loads the SerDe (Serializer/Deserializer) defined on the table to parse the raw HDFS files into rows and columns on the fly. If an HDFS field cannot be cast to the specified data type, Hive returns `NULL` for that column rather than aborting the query.

---

### Question 7: What is Data Skew in MapReduce and Hive, and how does it manifest in this telemetry pipeline?
**Answer:**  
**Data Skew** occurs when data is partitioned unevenly across reducers, causing a small subset of reducers (or a single straggler) to process a disproportionate volume of data while other reducers sit idle.

**Manifestation in this project**:
1. **Model Distribution Skew**: In real-world and synthetic traces, popular models like `gpt-4o` or `claude-sonnet` account for significantly more tokens and calls than smaller models. In `TokenStatsJob`, if the partition key is simply `model`, the reducer assigned to `gpt-4o` runs much longer. We mitigated this by using a composite key `(model, task_type)`, creating more fine-grained keys that distribute evenly across partitions.
2. **Long Sessions in Sessionization**: While most agent sessions contain 2–5 runs, anomalous automated loops or recursive coding agents can generate hundreds of runs within a single `session_id`. Since all runs for a `session_id` must go to the same reducer for secondary sort, that reducer experiences a processing spike.

---

### Question 8: What is the "Small Files Problem" in HDFS, and how did you prevent it in your architecture?
**Answer:**  
- **The Problem**: In HDFS, every file, directory, and block is stored as an in-memory metadata object in the NameNode's Java Heap, consuming approximately 150 bytes per object. If a pipeline generates millions of tiny files (e.g. 10 KB each), 1 million files consume ~150 MB of NameNode RAM while storing only 10 GB of data. Furthermore, MapReduce assigns one map task per input split (by default one block/file), leading to massive container scheduling overhead for trivial workloads.
- **Mitigations in this project**:
  1. **Batch File Generation**: The Python generator streams traces into partitioned chunks (e.g., 8–16 files of several hundred MBs) rather than emitting individual files per run.
  2. **Controlled Reducer Cardinality**: In MapReduce jobs, we set `mapreduce.job.reduces=2` rather than leaving it unbounded, ensuring each job outputs 2 consolidated partition files (`part-r-00000`, `part-r-00001`).
  3. **Curated ORC Consolidation**: When loading data into Hive managed ORC tables via `INSERT OVERWRITE`, Hive coalesces intermediate text files into optimized columnar stripes.

---

### Question 9: How does Apache YARN manage fault tolerance for failed Map tasks, Reduce tasks, and the ApplicationMaster?
**Answer:**  
1. **Map Task Failure**:
   If a mapper JVM throws an unhandled exception or hangs, the NodeManager detects the process exit or heartbeat timeout. The ApplicationMaster (AM) receives a container failure notification and reschedules the map task on another container. By default, YARN retries up to 4 times (`mapreduce.map.maxattempts=4`) before failing the job.
2. **Reduce Task Failure**:
   If a reducer fails during shuffle, sort, or reduce execution, the AM allocates a new container and re-runs the reducer. Because intermediate map outputs were saved to disk on the mapper nodes, the new reducer simply fetches the map output partitions again over HTTP without re-running the mappers.
3. **ApplicationMaster (AM) Failure**:
   If the AM itself crashes, the central YARN ResourceManager (RM) detects the missed heartbeats. The RM launches a new AM container and restores the job state from YARN's state store, re-running only incomplete tasks if AM restart is enabled.
4. **NodeManager Failure**:
   If a worker node crashes, the RM marks the node as lost and the AM reschedules all map tasks that were hosted on that node whose outputs were lost.

---

### Question 10: Why were the dataset sizes scaled to 100K, 500K, and 1M runs rather than multi-billion record scales?
**Answer:**  
1. **Strict Local Resource Budget**: The design constraint for this project was to operate completely free without cloud subscriptions on a standard laptop with 8 GB physical RAM, where only 5 GB was allocated to WSL2 Ubuntu 22.04.
2. **Memory Footprint & Disk Limits**: 
   - Each agent run contains on average ~8 nested steps.
   - 100K runs = ~956,000 steps (~1 GB total storage).
   - 500K runs = ~4.77 million steps (~4.5 GB storage).
   - 1.0M runs = ~9.53 million steps (~9 GB raw + intermediate HDFS blocks).
   Generating billions of rows would exceed the 20 GB free disk headroom of the virtual machine and cause YARN container memory allocations to swap to disk, freezing the host operating system.
3. **Algorithmic Validation**: Distributed systems principles (sub-linear vs linear scaling, shuffle network reduction, secondary sort correctness) are fully verified at millions of records. Scaling characteristics from 100K to 1M empirically validate Big-O complexity without requiring petabyte clusters.

---

### Question 11: What is the difference between Hadoop's Pseudo-Distributed mode and a real multi-node cluster?
**Answer:**  
- **Pseudo-Distributed Mode (Our Setup)**:
  - All Hadoop daemons (NameNode, DataNode, ResourceManager, NodeManager, JobHistoryServer) run on a **single physical or virtual machine**, each as a separate Java process.
  - HDFS communication uses localhost network sockets (`hdfs://localhost:9000`).
  - YARN containers run as child processes on the same host CPU and memory.
  - Distributed logic (block replication, split calculation, shuffle transfer, serialization) is fully executed, but all network traffic loops through the virtual loopback adapter (`lo`), and all I/O contends for the same physical disk.
- **Fully-Distributed Production Cluster**:
  - The NameNode and ResourceManager run on dedicated master nodes.
  - DataNodes and NodeManagers run across dozens or thousands of distinct physical worker servers connected by top-of-rack 10G/100G network switches.
  - The shuffle phase involves real physical network transfer between distinct IP addresses, where network bandwidth is a primary bottleneck (making Combiners even more critical).

---

### Question 12: Why did `SessionizationJob` implement a streaming Reducer instead of buffering all steps into an `ArrayList`?
**Answer:**  
In Java MapReduce, the `reduce(KEY key, Iterable<VALUE> values)` method provides an iterator over all records sharing that key.

- **The Naive Approach (Heap Buffering)**:
  ```java
  List<SpanRecord> list = new ArrayList<>();
  for (SpanRecord val : values) {
      list.add(val.clone()); // Stores all records in Java Heap memory
  }
  Collections.sort(list); // Sorts in memory
  ```
  If an agent session has thousands of steps, buffering all objects into an `ArrayList` consumes massive heap space. On our machine, where the reducer JVM heap was strictly capped at `-Xmx800m`, this causes `java.lang.OutOfMemoryError: Java heap space` and crashes the YARN container.
- **Our Streaming Approach**:
  Because Secondary Sort sorted the records prior to reducer invocation, the iterator in `SessionReducer` delivers spans in guaranteed chronological order. The reducer maintains only a few scalar tracking variables (`minStartTime`, `maxEndTime`, `lastRunEndTime`, `totalTokens`, `toolCount`). It processes each span in a single pass in $O(1)$ auxiliary memory, never storing records in a collection.

---

### Question 13: How does GZIP output compression in `TraceFlattenJob` reduce overall job execution time even though compression consumes extra CPU?
**Answer:**  
In our benchmark at 500K runs:
- Uncompressed output took **56 seconds** (written size: 2.47 GB).
- GZIP compressed output took **51 seconds** (written size: 271 MB) — **8.9% faster**.

**Why this occurs**:
In modern computer architecture, disk I/O bandwidth and virtual filesystem bridges (like WSL2's virtual disk layer) are orders of magnitude slower than CPU clock cycles. Writing 2.47 GB of raw uncompressed text saturates the virtual disk write buffer and incurs heavy OS page cache flush stalls. GZIP reduces the volume of bytes written to disk by **9.11x** (from 2.47 GB down to 271 MB). The CPU time spent executing the DEFLATE compression algorithm is significantly smaller than the time saved by not waiting for physical disk write queues to clear.

---

### Question 14: Why did increasing Reducers from 1 to 2 in `SessionizationJob` yield virtually no runtime improvement (125s vs 124s)?
**Answer:**  
In a multi-node cluster, doubling the number of reducers from 1 to 2 cuts reducer processing time almost in half because the computation, network shuffle, and disk writes are distributed across two distinct physical machines with separate network interfaces and separate hard drives.

However, in our **single-node WSL2 pseudo-distributed environment**:
1. Both reducers run as concurrent JVM processes on the same virtualized CPU (4 vCPUs allocated).
2. Both reducers read and write partitions to the exact same physical NVMe SSD storage channel.
3. Running 2 reducers simultaneously creates disk I/O head contention and thread context-switching overhead.
The benchmark accurately reflects systems behavior: on a single disk-bound virtual machine, increasing reducer parallelism does not improve throughput.

---

### Question 15: Explain how Predicate Pushdown and Stripe Statistics work in Apache Hive with ORC files.
**Answer:**  
- **Predicate Pushdown**:
  Instead of having the query execution engine read every row from storage into memory and then evaluate the `WHERE` clause filter, the filter predicate is "pushed down" to the storage reader layer (the ORC RecordReader).
- **ORC Stripe Architecture**:
  An ORC file is divided into independent chunks called **stripes** (typically 64 MB to 256 MB or ~10,000 to 50,000 rows). In the stripe footer, ORC records column-level summary statistics: `min`, `max`, `hasNull`, and `sum`.
- **Execution Mechanism**:
  Suppose the query executes: `SELECT * FROM steps_orc WHERE latency_ms > 30000;`.
  Before reading the compressed column streams of a stripe, the ORC reader inspects the stripe metadata for `latency_ms`. If the `max(latency_ms)` for that stripe is `18,500 ms`, the reader knows that no record in that entire 10,000-row stripe can satisfy the condition. The reader skips the entire block without decompressing or deserializing it.

---

### Question 16: What is the role of the Hive Metastore, and why did you use embedded Derby instead of MySQL/PostgreSQL?
**Answer:**  
- **Role of the Metastore**:
  The Hive Metastore is the central relational catalog that stores table schemas, column data types, SerDe specifications, table properties, HDFS directory locations, and partition keys. Without the metastore, Hive cannot translate SQL statements into distributed MapReduce jobs.
- **Why Embedded Derby**:
  1. **Zero External Overhead**: External relational databases like MySQL or PostgreSQL run as background services that consume 300–600 MB of persistent RAM, which would subtract directly from our tight 5 GB WSL2 memory budget.
  2. **Simplicity and Portability**: Apache Derby is an embedded Java relational database that initializes inside the Hive JVM process and stores its database files in a local directory (`metastore_db`), making the entire repository self-contained and reproducible with zero database configuration.
- **Trade-off**: Embedded Derby does not support concurrent multi-client connections. Only one Hive CLI / script session can access the metastore at any given moment.

---

### Question 17: What caused the classic Hive 3.1.3 Guava conflict and how was it resolved?
**Answer:**  
- **Root Cause**:
  Apache Hive 3.1.3 was released in 2018 and bundled Google's `guava-19.0.jar` in its `/lib` directory. Apache Hadoop 3.3.6 (released in 2023) upgraded internal dependencies to `guava-27.0-jre.jar`. 
  When Hive executes, it places its own `/lib` jars and Hadoop's classpath on the JVM classpath. Because Guava 19.0 lacks methods introduced in later Guava releases (such as `com.google.common.base.Preconditions.checkArgument(ZLjava/lang/String;Ljava/lang/Object;)`), calling Hadoop components from Hive throws a fatal `java.lang.NoSuchMethodError`.
- **Resolution**:
  We removed the legacy `guava-19.0.jar` from `$HIVE_HOME/lib/` and copied `$HADOOP_HOME/share/hadoop/hdfs/lib/guava-27.0-jre.jar` into `$HIVE_HOME/lib/`. This unified both frameworks onto Guava 27.0.

---

### Question 18: How does the pipeline handle corrupted or malformed JSON records without aborting the MapReduce job?
**Answer:**  
In production distributed pipelines, data feeds frequently contain malformed rows (network corruption, truncated writes, invalid UTF-8). A robust pipeline must isolate bad data without failing multi-hour jobs.

**Implementation in `TraceFlattenMapper`**:
1. Each line is read as a raw `Text` record inside a `try-catch` block.
2. The mapper attempts to parse the line using Google's streaming `JsonParser`.
3. If an exception occurs (`JsonSyntaxException`, `NullPointerException`, or invalid numeric format):
   - The exception is caught cleanly.
   - A Hadoop custom counter is incremented:
     ```java
     context.getCounter("MalformedRecords", "INVALID_JSON").increment(1);
     ```
   - The malformed raw text is emitted to a designated quarantine output path using Hadoop's `MultipleOutputs` (e.g. `/data/processed/_malformed/`).
   - The mapper immediately proceeds to the next line.
In our validation test, 10 malformed records (1.14%) were safely caught, counted, and diverted without failing the container.

---

### Question 19: What does the Prompt-to-Completion Token Ratio of 37:1 reveal about AI agent systems architecture?
**Answer:**  
In our Hive analytical query (`q04_token_consumption`), we calculated:
- **Total Prompt Tokens**: 310,955,835 (97.39%)
- **Total Completion Tokens**: 8,320,647 (2.61%)
- **Ratio**: **37.37 prompt tokens for every 1 completion token**.

**Engineering Implications**:
1. **Agent Cost Structure**: In multi-turn autonomous agents, the cost of generation is negligible compared to the cost of repeated context ingestion. With every turn, the agent sends the initial system instructions, conversation history, and large tool output payloads (e.g., JSON schemas, web search HTML snippets) back to the LLM.
2. **Context Window Inflation**: Query `q06_context_growth` showed that average prompt tokens expand from **1,997 tokens at turn 0** to **5,021+ tokens by turn 4** (+151% increase).
3. **Architectural Need for Context Pruning**: This empirical telemetry proves that agent platforms must implement distributed context compression, semantic prompt pruning, or externalized memory systems (vector embeddings, summaries) rather than blindly appending raw tool outputs into the prompt history.

---

### Question 20: Why is a Map-Only configuration optimal for `TraceFlattenJob`?
**Answer:**  
A MapReduce job is configured as **Map-Only** by setting `job.setNumReduceTasks(0)`.

**Why this is optimal for `TraceFlattenJob`**:
1. **Zero Cross-Record Dependencies**: Each JSON line represents a self-contained nested agent run. Parsing and unnesting step arrays does not require grouping, aggregation, or sorting across multiple lines.
2. **Elimination of Shuffle and Sort**: In standard MapReduce, up to 70% of execution time is consumed by hashing partition keys, spilling intermediate outputs to disk, shuffling partitions over the network, and merge-sorting keys before invoking reducers.
3. **Direct Disk Output**: In a Map-Only job, mapper output records bypass the partitioner, shuffle buffer, and sorting engine, writing directly to HDFS blocks.
4. **Maximum Throughput**: In our benchmark, `TraceFlattenJob` processed 1.0M runs (over 9.5 million step unnestings) in just **101 seconds** on our constrained machine, scaling sub-linearly with data size.
