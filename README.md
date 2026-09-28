# 🏦 Day 27 - Real-Time Banking Project with Apache Spark

## 📌 Overview

Day 27 implements a real-time banking transaction monitoring system using Apache Spark Streaming and Scala.

The application receives transaction events through a socket stream, aggregates transactions by account, detects suspicious transaction bursts using window processing, enriches transactions with small branch and risk reference data, demonstrates persistence and partitioning, and explains how the application can run on YARN.

---

## 🎯 Objectives

- Design a transaction event schema
- Process real-time banking transactions
- Aggregate transactions by account
- Detect suspicious transaction bursts using windows
- Join transactions with small branch/risk reference data
- Use cache/persist for streaming data
- Demonstrate partitioning
- Explain YARN execution

---

## 🛠️ Technologies Used

- Apache Spark 3.5.3
- Spark Streaming
- Scala 2.12.18
- SBT 1.12.11
- Netcat (`nc`)
- Ubuntu/Linux
- YARN concepts

---

## 📂 Project Structure

day27-spark/
├── src/
│   └── main/
│       └── scala/
│           └── Day27BankingStreaming.scala
├── project/
│   └── build.properties
├── build.sbt
├── .gitignore
└── README.md

---

## 🏦 Transaction Event Schema

Each incoming banking transaction follows:

transactionId,accountId,branchId,amount

Example:

T001,A001,B001,5000
T002,A001,B001,7000
T003,A002,B002,12000

The four fields represent:

- Transaction ID
- Account ID
- Branch ID
- Transaction Amount

---

## ⚡ Spark Streaming Configuration

### Batch Interval

The application processes incoming transactions every:

5 seconds

### Window Size

Suspicious transaction monitoring uses:

10 seconds

### Sliding Interval

The window slides every:

5 seconds

### Socket

localhost:9999

---

## 📡 Real-Time Transaction Processing

The application receives transaction events using:

ssc.socketTextStream("localhost", 9999)

Each valid transaction is parsed into:

(transactionId, accountId, branchId, amount)

Invalid transaction records are ignored.

---

## 💾 Cache / Persist

The transaction stream is persisted using:

transactions.persist(StorageLevel.MEMORY_AND_DISK)

MEMORY_AND_DISK allows Spark to keep cached data in memory when possible and use disk when the data cannot fit completely in memory.

Persistence is useful because the same transaction stream is used by multiple processing operations such as:

- Transaction display
- Account aggregation
- Branch/risk enrichment
- Suspicious burst detection
- Partitioning

---

## 📊 Account Transaction Aggregation

Transactions are grouped by account ID and their amounts are aggregated.

Example:

A001 -> 15000.00
A002 -> 12000.00

This provides an account-level view of transaction activity.

---

## 🚨 Suspicious Transaction Burst Detection

The application detects sudden increases in transaction activity using:

reduceByKeyAndWindow

Configuration:

Window Size    : 10 seconds
Slide Interval : 5 seconds

Transactions are grouped by account and counted inside the sliding window.

When an account reaches 3 or more transactions inside the monitored window, the application generates:

🚨 SUSPICIOUS BURST ALERT

Example:

Account: A001 | Transactions in window: 3
🚨 SUSPICIOUS BURST ALERT: A001

This demonstrates how streaming windows can be used to monitor unusual transaction activity.

---

## 🔗 Branch and Risk Reference Data

The application uses a small reference dataset containing branch information and risk levels.

Branch reference:

B001 -> Hyderabad -> LOW
B002 -> Bangalore -> MEDIUM
B003 -> Chennai -> LOW
B004 -> Mumbai -> HIGH
B005 -> Delhi -> MEDIUM

The reference data is distributed using a Spark Broadcast Variable.

---

## 📦 Broadcast Reference Data

The branch and risk Map is small and read-only, so it can be broadcast to executors.

The application uses:

val broadcastBranchRisk =
  ssc.sparkContext.broadcast(branchRiskData)

This avoids repeatedly sending the same small reference data with every task.

---

## 🔗 Transaction Enrichment

Each transaction is enriched using its branch ID.

Example:

T006 | A003 | B004 | Mumbai | Risk: HIGH | Amount: 18000.00

This combines transaction information with branch location and risk classification.

---

## 🧩 Partitioning

The application demonstrates partitioning using:

HashPartitioner(4)

The transaction RDD is converted into key-value pairs using the account ID and partitioned across four partitions.

Example output:

Number of partitions: 4
Partitioning method : HashPartitioner

Partitions allow Spark to process data in parallel.

---

## 🧠 Why Partitioning Matters

Partitioning distributes data across Spark partitions.

Good partitioning can:

- Improve parallel processing
- Reduce unnecessary data movement
- Improve aggregation performance
- Help distribute workload across executors

The number of partitions should be selected according to the data volume and available cluster resources.

---

## 🛡️ Fault Tolerance

Spark provides fault tolerance through RDD lineage.

Spark records the transformations used to create RDDs.

If a partition is lost, Spark can recompute the lost partition using the lineage information.

The streaming application also configures checkpointing:

ssc.checkpoint("/tmp/day27-banking-checkpoint")

Checkpointing is important for maintaining streaming state and supporting recovery.

---

## 🔀 DAG - Directed Acyclic Graph

Spark creates a DAG representing the sequence of transformations.

The simplified processing flow for this project is:

Socket Stream
      ↓
Parse Transactions
      ↓
Persist Transactions
      ↓
 ┌───────────────┬────────────────┬──────────────────┐
 ↓               ↓                ↓                  ↓
Aggregation   Enrichment     Window Detection   Partitioning
 ↓               ↓                ↓                  ↓
Account        Branch/Risk     Burst Alert       Parallel
Totals         Information                       Processing

Spark uses the DAG to organize transformations and create execution stages.

---

## 🖥️ YARN Execution

In a production cluster, the Spark banking application can be deployed using YARN.

### Execution Flow

1. The Spark application is submitted to YARN.
2. YARN ResourceManager receives the application request.
3. An ApplicationMaster is created for the application.
4. YARN allocates containers for Spark executors.
5. Spark executors process partitions in parallel.
6. The Spark Driver coordinates jobs and tasks.
7. YARN manages cluster resources and container allocation.

### Example Submission

A production deployment could use:

spark-submit --master yarn --deploy-mode cluster Day27BankingStreaming.jar

The exact command depends on the cluster configuration and packaging method.

---

## 🧪 Testing

### Step 1 - Start the Spark Application

sbt -error run

### Step 2 - Start the Socket Listener

Open another terminal:

nc -lk 9999

### Step 3 - Send Banking Transactions

T001,A001,B001,5000
T002,A001,B001,7000
T003,A002,B002,12000
T004,A001,B001,3000
T005,A003,B004,15000
T006,A003,B004,18000

---

## ✅ Observed Results

The application successfully demonstrated real-time transaction processing.

### Transaction Events

Example:

Transaction: T006 | Account: A003 | Branch: B004 | Amount: 18000.00

### Account Aggregation

Example:

A003 -> 18000.00

### Branch and Risk Enrichment

Example:

T006 | A003 | B004 | Mumbai | Risk: HIGH | Amount: 18000.00

### Suspicious Transaction Window

The application monitored transaction activity using:

Window Size    : 10 seconds
Slide Interval : 5 seconds

### Partitioning

The application successfully displayed:

Number of partitions: 4
Partitioning method : HashPartitioner

---

## 🌍 Real-World Scenario

This architecture can be used for real-time banking transaction monitoring.

Banks can continuously process transaction events and monitor:

- Account transaction activity
- Sudden transaction bursts
- Branch risk information
- High-risk branches
- Account-level transaction totals

Window processing can help identify sudden increases in transaction activity that may require further investigation.

---

## 📚 Concepts Covered

- Spark Streaming
- Micro-batch processing
- Socket streaming
- Transaction event schema
- Account aggregation
- `reduceByKey`
- `reduceByKeyAndWindow`
- Window processing
- Broadcast Variables
- Cache and Persist
- `MEMORY_AND_DISK`
- Partitioning
- `HashPartitioner`
- RDD lineage
- Fault tolerance
- DAG
- Spark executors
- YARN ResourceManager
- ApplicationMaster
- YARN containers
- Real-time banking analytics

---

## ☑️ Day 27 Checklist

- [x] Transaction event schema
- [x] Real-time transaction stream
- [x] Account-level aggregation
- [x] Suspicious transaction burst detection
- [x] Window processing
- [x] Branch reference data
- [x] Risk reference data
- [x] Broadcast reference data
- [x] Transaction enrichment
- [x] Cache/Persist
- [x] MEMORY_AND_DISK
- [x] Partitioning
- [x] HashPartitioner
- [x] Partitions explanation
- [x] DAG explanation
- [x] Fault tolerance explanation
- [x] YARN execution explanation
- [x] End-to-end testing

----
