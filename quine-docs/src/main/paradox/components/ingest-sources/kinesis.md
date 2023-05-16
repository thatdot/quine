---
description: Quine streaming graph ingest events from a Kinesis source
---
# AWS Kinesis Support

## Reading Records from Kinesis Data Streams

Quine has full support for reading records from Kinesis Data Streams. The means by which Quine interprets a record into some graph-structured data is highly configurable via the @ref:[REST API](../../reference/rest-api.md).

### Example

In this example, we will register a multiple-shard Kinesis stream of JSON objects (one JSON object per Kinesis record) as a data source, creating a single node in the graph for each object.

#### Preparation

For the purposes of this example, you will need @link:[a Kinesis data stream](https://console.aws.amazon.com/kinesis/home#/streams/create){ open=new } and credentials (an access key ID and secret access key) for an @link:[IAM User](https://console.aws.amazon.com/iam/home?#/users$new?step=details){ open=new } with the following privileges:

- kinesis:RegisterStreamConsumer
- kinesis:DeregisterStreamConsumer
- kinesis:SubscribeToShard
- kinesis:DescribeStreamSummary
- kinesis:DescribeStreamConsumer
- kinesis:GetShardIterator
- kinesis:GetRecords
- kinesis:DescribeStream
- kinesis:ListTagsForStream

 For our example, we'll assume we have such a user with access to the `json-logs` stream with access key ID `AKIAMYACCESSKEY` and secret `AWSScRtACCessKeyAWS/ScRtACCessKey`. These will be used to register the data source with Quine.

#### Registering Kinesis as a data source

To register Kinesis as a data source to Quine, we need to describe our stream via the ingest @ref:[REST API](../../reference/rest-api.md).

For example, we'll use the aforementioned Kinesis stream hosted in the region `us-west-2`, named `json-logs` and we'll give the Quine ingest stream the name `kinesis-logs`. Thus, we make our API request a POST to `/api/v1/ingest/kinesis-logs` with the following payload:

```json
{
  "format": {
    "query": "CREATE ($that)",
    "type": "CypherJson"
  },
  "streamName": "json-logs",
  "parallelism": 2,
  "shardIds": [],
  "type": "KinesisIngest",
  "credentials": {
    "region": "us-west-2",
    "accessKeyId": "AKIAMYACCESSKEY",
    "secretAccessKey": "AWSScRtACCessKeyAWS/ScRtACCessKey"
  },
  "iteratorType": "TrimHorizon"
}
```

We pass in an empty list of shard IDs to specify that Quine should read from all shards in the stream. If we wanted to only read from particular shards, we would instead list out the shard IDs from which Quine should read.

Because the Kinesis stream is filled with JSON records, we choose the `CypherJson` import format, which reads each record as a JSON object before passing it as a `Map` to a Cypher query.

The Cypher query can access this object using the name `that`. Thus, our configured query `CREATE ($that)` will create a node for each JSON record with the same property structure as the JSON record.

In this example, we use a Kinesis stream populated with JSON objects as records, though Quine offers other options for how to interpret records from a stream. These options are configurable via the same endpoint by using different `format`s in the above JSON payload.

Finally, we choose to read all records from the Kinesis stream, including records already present in the stream configuring the Quine data source. To get this behavior, we use a `TrimHorizon` Kinesis iterator type. If we wished to only read records written to the Kinesis stream -after- setting up the Quine data source, we would have used the `Latest` iterator type.

<!-- ## Writing Records to Kinesis -->
<!-- Coming soon!  TODO -->