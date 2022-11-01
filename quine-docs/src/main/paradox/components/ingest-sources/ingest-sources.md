---
description: Quine can ingest events from multiple streaming sources and data providers
---
# Data Ingest

@@@index

* @ref:[Files and Named Pipes](files-and-named-pipes.md)
* @ref:[Kafka](kafka.md)
* @ref:[Kinesis](kinesis.md)
* @ref:[SQS / SNS](sqs---sns.md)
* @ref:[Standard Input](stdin.md)
@@@

## Overview

An **ingest stream** connects a potentially infinite stream of incoming events to Quine and prepares the data for the streaming graph. Within the ingest stream, an ingest query, written in Cypher, updates the streaming graph nodes and edges as data is received.

Working with data in Quine is a two-step process:

1. Load a stream of data into the graph with an **ingest stream**: *event-driven data*
2. Monitor the graph for results and act with a **standing query**: *data-driven events*

All data sources require an "ingest stream". The ingest stream is the first opportunity we have to affect the data. In its most basic form, an ingest stream maps JSON to Quine nodes directly. The following query creates a new Quine node and applies all of the properties from the incoming JSON payload. It then adds an "Event" label to help with organization.

```cypher
MATCH (n) WHERE id(n) = idFrom($that) SET n.line = $that
```

@@@ note { title=Hint }
Select nodes, don't search for nodes, since statically the ID of a node is usually not known. Quine adds an `idFrom` function to Cypher that takes any number of arguments and deterministically produces a node ID from that data.

This is similar to a consistent-hashing approach where a collection of values are hashed together to produce a unique result that can be used for an ID.

Quine supports many different kinds of IDs (numbers, UUIDs, strings, tuples of values, and more…), `idFrom` produces consistent results appropriate for the dataset regardless of the which ID type is used.
@@@

Quine parses JSON data into a graph structure according to the following assumptions:

* Each JSON object is treated as a node in the graph.
* Nested objects are treated as separate nodes. In this case, the JSON field name is treated as the name of the outgoing edge.
* The field id is the default field defining the ID of the node. The name of this field is customizable and can be set with the string config setting at quine.json.id-field.
* The ID computed for each object must be a string that can be parsed by the IdProviderType set in the configuration. IDs fields in JSON which do not have the proper type will result in an error returned from the API.
* Objects without any ID will be assigned a random ID. Duplicate identical objects with no ID field may result in multiple separate nodes created—depending on the structure of the data provided.

## Ingesting Event-Driven Data

Data enters Quine from streaming data sources like Kafka, Kinesis, or even POSIX named pipes. These data sources are effectively infinite — Quine works with them as if they will never end. Other types of data sources as supported as well, like ordinary files in CSV, JSON, or other formats.

Each ingest stream performs four primary operations:

1. **Consume a stream of bytes** - e.g. Open local file on disk, or connect to a Kafka topic.
2. **Delimit into a sequence of finite byte arrays** - e.g. Use newlines to separate individual lines from a file. Kafka provides delimiting records by its design.
3. **Parse byte array into an object** - e.g. Parse as a string into JSON, or use a provided [protobuf](https://developers.google.com/protocol-buffers) schema to deserialize each object. Bytes will first be decoded if the ingest has specified **recordDecoders**.
4. **Ingest query constructs the graph** - e.g. Provide the parsed object as `$that` to a user-defined Cypher query which creates any graph structure desired.

When a new ingest stream is configured, Quine will connect to the source and follow the steps described above to use the incoming data stream to update the internal graph. Not all ingest configurations require distinct choices for each step. For instance, a file ingest can define its source as a CSV (Comma Separated Values) file, and the line-delimiting and field parsing are done automatically.

Each ingest stream is backpressured. When Quine is busy with intensive tasks downstream, or possibly waiting for the durable storage to finish processing, Quine will slow down the ingest stream so that it does not overwhelm other components. Backpressured ingest streams ensure maximum throughput while preserving stability of the overall system.

## Data Sources

An ingest stream can receive data from the following data sources

* @ref:[Files and Named Pipes](files-and-named-pipes.md)
* @ref:[Kafka](kafka.md)
* @ref:[Kinesis](kinesis.md)
* @ref:[SQS / SNS](sqs---sns.md)
* @ref:[Standard Input](stdin.md)
  
## Ingest Stream Structure

An ingest stream must contain a `type` and `format`. Review the interactive API documentation for the list of required parameters for each type of ingest source.

An ingest stream `type` is described by the [API documentation](https://docs.quine.io/reference/rest-api.html#/paths/api-v1-ingest-name/post). Quine supports multiple types of ingest streams. Each type has a unique form and requires a specific structure to configure properly.

For example, constructing an ingest stream via the `/api/v1/ingest/{name}` [API](https://docs.quine.io/reference/rest-api.html#/paths/api-v1-ingest-name/post) endpoint to read data from standard in and store each line as a node looks similar to the example below.

```json
{
    "type": "StandardInputIngest",
    "format": {
     "type": "CypherLine",
     "query": "MATCH (n) WHERE id(n) = idFrom($that) SET n.line = $that"
    }
}
```

Quine natively reads from standard-in, passing each line into a Cypher query as a parameter: `$that`. A unique node ID is generated using `idFrom($that)`. Then, each line is stored as a `line` property associated with a new node in the streaming graph.

When creating an ingest stream via the API, you are given the opportunity to name the stream with a name that has meaning. For example, you can name the above ingest stream `standardIn` to make it easier to reference in your application.

Alternatively, creating an ingest stream via a recipe, Quine automatically assigns a name to each stream using the format `INGEST-#` where the first ingest stream defined in the recipe is `INGEST-1` and subsequent ingest streams are named in order with `#` counting up.

Here is the same ingest stream defined in a Quine [Recipe.](https://docs.quine.io/reference/recipe-ref-manual.html)

```yaml
ingestStreams:
  - type: StandardInputIngest
    format:
      type: CypherLine
      query: |-
        MATCH (n)
        WHERE id(n) = idFrom($that)
        SET n.line = $that
```

#### Record Decoding

An Ingest may specify a list of decoders to support decompression. Decoders are applied in the order they are
specified and are applied per-record.
- Decoding is applied in the order specified in the **recordDecoders** array.
- **recordDecoders** are currently supported _only_ for **Kafka**, **Pulsar**, **Kinesis**, **ServerSentEvents**, and **SQS** ingests.
- Decoding types currently supported are **Base64**, **Gzip**, and **Zlib**.
- The **recordDecoders** member is optional.

The following ingest stream specifies that each record is Gzipped, then Base64 encoded:

```json
{
  "type": "KinesisIngest",
  "format": {
    "query": "CREATE ($that)",
    "type": "CypherJson"
  },
  "recordDecoders":["Base64","Gzip"],
   ...
}
```
## Inspecting Ingest Streams via the API

Quine exposes a series of API endpoints that enable you to monitor and manage ingest streams while in operation. The complete endpoint definitions are available in the API documentation.

* @link:[List all running ingest streams](https://docs.quine.io/reference/rest-api.html#/paths/api-v1-ingest/get)
* @link:[Look up a running ingest stream](https://docs.quine.io/reference/rest-api.html#/paths/api-v1-ingest-name/get)
* @link:[Pause an ingest stream](https://docs.quine.io/reference/rest-api.html#/paths/api-v1-ingest-name--pause/put)
* @link:[Unpause an ingest stream](https://docs.quine.io/reference/rest-api.html#/paths/api-v1-ingest-name--start/put)
* @link:[Cancel a running ingest stream](https://docs.quine.io/reference/rest-api.html#/paths/api-v1-ingest-name/delete)
