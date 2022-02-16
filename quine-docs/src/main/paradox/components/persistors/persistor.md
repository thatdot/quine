# Persistors

@@@index
* @ref:[Cassandra Setup](cassandra_setup.md)
@@@

The graph operates in memory, but saves its data to disk. Because data is durably stored, Quine does not need to define any time-windows for matching up data in memory. This data is managed automatically so that it is transparent to the operation of the graph, and saved in a way that is fast for streaming data.

Quine's primary unit of data is the graph node. A node is defined by its ID and serves as a collection of properties and edges. Changes to the collection of properties and edges are what is saved to disk. These changes, sometimes called "deltas" are very small units of data added to an append-only log using the strategy known as event-sourcing. The persistor is the agent that stores and retrieves event-sourced data.

The format of data saved on disk is conceptually a key-value pair, where the key is a node ID and the value is the append-only log of changes to the properties and edges of that node. While this is conceptually the storage format, in practice the choice of data storage medium can affect the actual format stored on disk. For instance, when using Cassandra as the backing store for the persistor, the key is a compound value of both the Node ID ("QuineID") and a unique timestamp. (Note: timestamps in Quine are a careful combination of human-readable wall-clock time and several variations of logical time.)

## Local Persistors

### RocksDB

@link:[RocksDB](http://rocksdb.org){ open=new } is a widely used implementation of a log-structured merge tree (LSMT). It is an ideal data store for Quine and is the default option for data storage locally.

Using RocksDB in Quine requires no special changes to use it, since it is the default. The setting that would choose RocksDB specifically is:

`thatdot.quine.store.type=rocks-db`

@@@ warning

RocksDB is distributed by its authors as a binary artifact built for specific architectures and used from JVM applications through the Java Native Interface (JNI). At the time of this writing, RocksDB does not yet support the `arm64` architecture needed by Apple's M1 CPUs. As a result, Quine cannot use the RocksDB persistor when running on an M1 Mac. The issue tracking support for an M1-compatible build of the `rocksdbjni` package is here: @link:[https://github.com/facebook/rocksdb/issues/7720](https://github.com/facebook/rocksdb/issues/7720){ open=new } While it appears that issue has been resolved, we are waiting for the merged code to be officially released.

If you try to start Quine with the default settings on an M1 Mac, you will get an error suggesting that you run with the option to use MapDB instead: `-Dquine.store.type=map-db`

@@@

### MapDB

@link:[MapDB](https://mapdb.org){ open=new } is an embedded database engine for the JVM that uses memory-mapped files. Since MapDB is written in a JVM language, it is included in Quine without any binary dependencies built for specific architectures. This makes MapDB the most portable option for a Persistor's data store.

MapDB does have some other limitations though. Memory mapped files are generally limited to 2GB in size. With MapDB, memory mapped files larger than 2GB will become very slow to use. Quine supports sharding the storage of a MapDB persistor into multiple files to work around this limitation. But even if sharded, memory mapped files will cause Quine to use off-heap memory, which in extreme circumstances can cause the process to use large amounts of RAM or lead to the operating system killing the process.

To use MapDB, set the following configuration setting:

`thatdot.quine.store.type=map-db`

<!--
### LMDB (Lightning database)
-->

## Remote Persistors

### Cassandra

@link:[Apache Cassandra](https://cassandra.apache.org/_/index.html){ open=new } is a distributed NoSQL database. It is highly configurable and trusted by enterprise organizations around the world to manage very large amounts of data. Cassandra is an idea data storage mechanism for a Quine persistor. Using Cassandra, Quine instances can achieve extremely high throughput, high-availability, data replication, and failover strategies needed for production operation in the enterprise.

See the @ref:[Cassandra Setup](cassandra_setup.md) page for details on setting up and using Cassandra with Quine.

### AWS S3

@link:[AWS S3](https://aws.amazon.com/s3/){ open=new } is a cloud object store. Is it possible to use S3 as a persistor for some configurations of Quine. S3 has some particular limitations in speed, latency, and object structure which do not make it suitable to store data with the traditional event-sourcing strategy. However, Quine can be configured to operate the graph with a strategy to save only complete snapshots to the persistor. This "snapshot-only" mode works with S3, but carries the risk that data is not stored durably until a node is expired out the in-memory cache (which depends on many internal factors). Because of these limitations, S3 support is considered experimental at this time.
