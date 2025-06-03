<div align="center">

# moonlink 🥮
Real-Time Postgres to Iceberg Mirroring 

[![License](https://img.shields.io/badge/License-BSL-blue)](https://github.com/Mooncake-Labs/moonlink/blob/main/LICENSE)
[![Slack](https://img.shields.io/badge/Mooncake%20Devs-purple?logo=slack)](https://join.slack.com/t/mooncakelabs/shared_invite/zt-2sepjh5hv-rb9jUtfYZ9bvbxTCUrsEEA)
[![Twitter](https://img.shields.io/twitter/url?url=https%3A%2F%2Fx.com%2Fmooncakelabs&label=%40mooncakelabs)](https://x.com/mooncakelabs)
[![Docs](https://img.shields.io/badge/docs-pg?style=flat&logo=readthedocs&logoColor=white)](https://pgmooncake.com/docs)

</div>

## Overview

Moonlink is a Rust library that enables sub-second mirroring (CDC) of Postgres tables into [Iceberg](https://iceberg.apache.org/). It serves as a drop-in replacement for the Debezium + Kafka + Flink + Spark stack. 

Under the hood, it extends Iceberg with a real-time storage engine optimized for low-latency, high-throughput ingestion from update-heavy sources like Postgres logical replication.

> **Note:** Moonlink is in preview. Expect changes. Join our [Community](https://join.slack.com/t/mooncakelabs/shared_invite/zt-2sepjh5hv-rb9jUtfYZ9bvbxTCUrsEEA) to stay updated!

## Features
- **Sub-second Ingestion**: Including updates and deletes
- **Real-time Reads**: Unified view combining in-memory state and Iceberg files
- **Iceberg-native Optimizations**: Implements deletion vectors and compaction to maintain read performance
- **Simple Deployment**: Single Rust library that can be embedded ([pg_mooncake](https://github.com/Mooncake-Labs/pg_mooncake)) or scaled out


Efficient handling of Iceberg State: Compaction, Deletion Vectors

While most replication tools treat Iceberg as a black box, Moonlink makes a lot of optimizations for it as the destination. 





## Architecture

Moonlink extends Iceberg with a thin [Arrow](https://arrow.apache.org/) buffer with indexes and a positional deletion log. This buffer efficiently handles hot incoming data; and will periodically flush to Iceberg. 

<div align="center">
  <a href="assets/v0.2architecture.jpg">
    <img src="assets/v0.2architecture.jpg" width="80%"/>
  </a>
</div>

### Write Path

**Raw Inserts** 
- Rows are written to an Arrow buffer
- Buffer data is efficiently flushed to Parquet when full

**Raw Deletions**
- Moonlink maintains primary key indexes for all rows
- Deletions update the positional deletion log using these indexes
- Periodically, deletion logs are converted to Iceberg v3 deletion vectors.

### Read Path

Moonlink exposes a union read interface that combines its in-memory state with Iceberg files. 

Engines can use this union-read interface to access the most current table state. For eg: [pg_mooncake v0.2](https://github.com/Mooncake-Labs/pg_mooncake/) uses this for sub-second consistency between Postgres and Columnstore (Iceberg) tables.

### Connectors

Moonlink can support multiple input data sources through [moonlink-connectors](https://github.com/Mooncake-Labs/moonlink-connectors). Currently, only Postgres logical replication is supported as a source. 

Feel free [request](https://join.slack.com/t/mooncakelabs/shared_invite/zt-2sepjh5hv-rb9jUtfYZ9bvbxTCUrsEEA) more connectors or open a PR!



## Sample Usage




## Roadmap


## UNDONE
- Write Iceberg (a snapshot should be equvalent to an iceberg snapshot and should write iceberg metadata)
  - Load from snapshot on restart
  - Merge/ Compact Iceberg
  - Advanced Iceberg Options
- Index & Delete of rows that are not in memslice ✅
  - Return deletion vector in read
  - On-Disk index
  - Row identifier other than int primary key
  - RefCount of temp parquet files & deletion vectors
- Handle large writes/ flush before commit
  - Handle streaming write in logical replicate
- Data types
  - Major types ✅
  - Decimal
  - Array
  - Postgres customized types
- Perf, a lot of unoptimized code
- Concurrent test
