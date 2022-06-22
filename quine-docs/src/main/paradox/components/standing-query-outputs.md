# Standing Query Outputs

Each standing query can have any number of destinations to which `StandingQueryResults` will be routed. 

## Result Type

A `StandingQueryResult` is an object with 2 fields: `meta` and `data`. The `meta` object consists of:

- `resultId`: a UUID identifying unique results
- `isPositiveMatch`: a boolean value signifying whether this result now matches the pattern (`true`), or no longer matches (`false`)

On a positive match (`true`), the `data` object consists of the data returned by the Standing Query. For example, a `StandingQueryResult` may look like the following:

```
{
  "meta": {
    "resultId": "b3c35fa4-2515-442c-8a6a-35a3cb0caf6b", 
    "isPositiveMatch": true
  }, 
  "data": {
    "id(n)": "a0f93a88-ecc8-4bd5-b9ba-faa6e9c5f95d"
  }
}
```

While a negative match (`false`) signifying cancellation of that result might look like the following:

```
{
  "meta": {
    "resultId": "b3c35fa4-2515-442c-8a6a-35a3cb0caf6b", 
    "isPositiveMatch": false
  }, 
  "data": {}
}
```

## Result Outputs

### Run Cypher Query

For each result, assign the result to the parameter `$that`, passing it into and running the provided `query`. The number of simultaneous results that will run is determined by the `parallelism` value.

When running a Cypher Query as the standing query output, the user has the option to pass in another output step in the `andThen` field. This field can include any of the output options described here, even additional Cypher Query steps. Be aware that non-trivial or long-running operations with `StandingQueryResults` will consume system resources and cause the system to backpressure and slow down other processing (like data ingest).

### POST to Webhook

Makes an HTTP[S] POST for each `StandingQueryResult`. The data in the request payload can be customized in a Cypher query preceeding this step.

### Publish to Slack

Sends a message to Slack via a configured Slack App webhook URL. See @link:[https://api.slack.com/messaging/webhooks](https://api.slack.com/messaging/webhooks){ open=new }.

Slack limits the rate of messages which can be posted; limited to 1 message per second at the time of writing. Quine matches that rate-limit internally by aggregating `StandingQueryResult`s together if they arrive faster than can be published to Slack. The aggregated results will be published as a single message to Slack when the rate-limit allows.

### Log JSON to Standard Out

Prints each result as a single-line JSON object to standard output on the Quine server. This output type can be configured with `Complete` to print a line for every `StandingQueryResult`, backpressuring and slowing down the stream as needed to print every result. Or it can be configured with `FastSampling` to log results in a best-effort, but dropping some results if it would slow down the stream. Note that neither option changes the behavior of other StandingQueryResultOutputs registered on the same standing query.

### Log JSON to a File

Write each `StandingQueryResult` as a single-line JSON object to a file on the local filesystem.

### Publish to Kafka Topic

Publishes a record for each `StandingQueryResult` to the provided Apache Kafka topic. Records can be serialized as JSON or Protocol Buffers before being published to Kafka.

### Publish to Kinesis Stream

Publishes a record for each `StandingQueryResult` to the provided Kinesis stream. Records can be serialized as JSON or Protocol Buffers before being published to Kinesis.

### Publish to SNS Topic

Publishes an AWS SNS record to the provided topic containing JSON for each result. For the format of the result, see "Standing Query Result Output".

**Ensure your credentials and topic ARN are correct.** If writing to SNS fails, the write will be retried indefinitely. If the error is unfixable (e.g.: the topic or credentials cannot be found), the outputs will never be emitted and the Standing Query this output is attached to may stop running.
