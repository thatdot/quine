# Data Ingest

@@@index
* @ref:[Files and Named Pipes](files-and-named-pipes.md)
* @ref:[Kafka](kafka.md)
* @ref:[Kinesis](kinesis.md)
* @ref:[SQS / SNS](sqs---sns.md)
* @ref:[Standard Input](stdin.md)
@@@

## Overview

Working with data in Quine is a two-step process:

1. Load a stream of data into the graph with an **ingest stream**: *event-driven data*
2. Monitor the graph for results and act with a **standing query**: *data-driven events*

All data sources require an "ingest stream". The ingest stream is the first opportunity we have to affect the data. In its most basic form, an ingest stream maps JSON to Quine nodes directly. The following query creates a new Quine node and applies all of the properties from the incoming JSON payload. It then adds an "Event" label to help with organization

`WITH $props AS p MATCH(n) WHERE id(n) = idFrom(p) SET n = p, n:Event`

@@@ note { title=Hint }
Since statically, the ID of a node is not usually known, we recommend using the `idFrom` function to have Quine compute the IDs of nodes based on data values. This is similar to a consistent-hashing approach where a collection of values are hashed together to produce a unique result that can be used for an ID. Quine supports many different kinds of IDs (numbers, UUIDs, strings, tuples of values, and more…), `idFrom` produces consistent results appropriate for the dataset regardless of the which ID type is used.
@@@

Quine parses JSON data into a graph structure according to the following assumptions:

* Each JSON object is treated as a node in the graph. 
* Nested objects are treated as separate nodes. In this case, the JSON field name is treated as the name of the outgoing edge.
* The field id is the default field defining the ID of the node. The name of this field is customizable and can be set with the string config setting at quine.json.id-field.
* The ID computed for each object must be a string that can be parsed by the IdProviderType set in the configuration. IDs fields in JSON which do not have the proper type will result in an error returned from the API.
* Objects without any ID will be assigned a random ID. Duplicate identical objects with no ID field may result in multiple separate nodes created—depending on the structure of the data provided.

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

## Data Sources
An ingest stream can receive data from the following data sources

* @ref:[Files and Named Pipes](files-and-named-pipes.md)
* @ref:[Kafka](kafka.md)
* @ref:[Kinesis](kinesis.md)
* @ref:[SQS / SNS](sqs---sns.md)
* @ref:[Standard Input](stdin.md)
  
## Ingest Source Structure
An ingest stream must contain a `type` and `format`. Review the interactive API documentation for the list of required parameters for each type of ingest source. 

### API

A ingest stream is created with a POST to the `/api/v1/ingest/<name>` endpoint. 

```shell
curl -X 'POST' \
  'http://0.0.0.0:8080/api/v1/ingest/Kafka-1' \
  -H 'accept: */*' \
  -H 'Content-Type: application/json' \
  -d '{
  "topics": [
    "e1-source"
  ],
  "bootstrapServers": "localhost:9092",
  "groupId": "quine-e1-ingester",
  "type": "KafkaIngest"
}'
```

### Recipe

A recipe can create an ingest stream using the `ingestStreams` array.

@@@ note
The first ingest stream created via a recipe will always have the name `INGEST-1`. Subsequent ingest streams will be named in order as `INGEST-2`, `INGEST-3`, etc.
@@@

```yaml
ingestStreams:
  - type: FileIngest
    path: $in-file
    format:
      type: CypherJson
      query: |-
        MATCH (event) 
        WHERE id(event) = idFrom($that) 
        SET event = $that,
          event.-type = 'CDN-log-event',
          event:Request
```