# Ingest Sources

@@@index
* @ref:[Files and Named Pipes](files_and_named_pipes.md)
* @ref:[Kafka](kafka.md)
* @ref:[Kinesis](kinesis.md)
* @ref:[SQS / SNS](sqs_-_sns.md)
* @ref:[Standard Input](stdin.md)
@@@

The primary mechanism for loading data into Quine is configuring and starting one or more ingest sources. Each ingest source is a (potentially infinite) stream of incoming data. Multiple types of ingest sources are available.

## Ingesting Event-Driven Data

Data enters Quine primarily through streaming data sources like Kafka, Kinesis, or even POSIX named pipes. These data sources are effectively infinite — Quine works with them as if they will never end. Other types of data sources as supported as well, like ordinary files in CSV, JSON, or other formats.

Each ingest stream has four primary steps:

1. **Consume a stream of bytes** - e.g. Open local file on disk, or connect to a Kafka topic.
2. **Delimit into a sequence of finite byte arrays** - e.g. Use newlines to separate individual lines from a file. Kafka provides delimiting records by its design.
3. **Parse byte array into an object** - e.g. Parse as a string into JSON, or use a provided [protobuf](https://developers.google.com/protocol-buffers) schema to deserialize each object.
4. **Ingest query constructs the graph** - e.g. Provide the parsed object as `$that` to a user-defined Cypher query which creates any graph structure desired.

When a new data ingest is configured, Quine will connect to the source and follow the steps described above to use the incoming data stream to update the internal graph. Not all ingest configurations require distinct choices for each step. For instance, a file ingest can define its source as a CSV (Comma Separated Values) file, and the line-delimiting and field parsing are done automatically.

Each ingest stream is backpressured. When Quine is busy with intensive tasks downstream, or possibly waiting for the durable storage to finish processing, Quine will slow down the ingest stream so that it does not overwhelm other components. Backpressured ingest streams ensure maximum throughput while preserving stability of the overall system.