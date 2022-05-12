# Quick Start

## Startup

Start Quine using the executable or source code (described below). With Quine running on your local system with the default settings, use the interactive API documentation at <http://localhost:8080/docs> for the example below.

### From a Docker container

- With Docker installed, run Quine from Docker Hub.
- `docker run -p 8080:8080 thatdot/quine`

### From an executable

- [Download](https://quine.io/download) the executable `jar` file.
- `java -jar quine-x.x.x.jar`

@@@ note
Quine releases names contain a version number by default. In our examples, we generically refer to Quine as `quine.jar` without the version number. When you see `quine.jar` in future examples, please update your command to include the version number in the filename.
@@@

### From source code

- Download the [source code](https://github.com/thatdot/quine) from GitHub.
- With a recent version of the Java Development Kit (8 or newer) and `sbt` installed.
- From the main directory of the repository on your machine: `sbt quine/run`

## Initial Run

Making use of Quine is a two-step process:

1. Ingest a stream of data into the graph: event-driven data
2. Monitor the graph for results that stream out: data-driven events

### Connect an Ingest Stream

Let's ingest the live stream of new pages created on Wikipedia. This requires only an API call to: `POST http://localhost:8080/api/v1/ingest/{name}` Expand that line in the interactive documentation, click "Try it out", and fill in the `name` field with any name you choose to refer to the ingest stream. Pass in this JSON payload that defines the ingest stream:

```json
{
  "type" : "ServerSentEventsIngest",
  "url" : "https://stream.wikimedia.org/v2/stream/page-create"
}
```

Click `Execute` and this API Call will start an ingest stream to consume data from the live stream of page creations from Wikimedia sites.

### Create a Standing Query

A Standing Query matches some graph structure incrementally while new data is written in. Creating a standing query is done with a single API call to: `POST http://localhost:8080/api/v1/query/standing/{standing-query-name}` Expand that line in the interactive documentation, click "Try it out", and fill in the `standing-query-name` field with any name you choose to refer to this standing query. Pass in this JSON payload that defines the standing query:

```json
{
  "pattern": {
    "query": "MATCH (n) RETURN DISTINCT id(n)",
    "type": "Cypher"
  },
  "outputs": {
    "print-output": {
      "type": "PrintToStandardOut"
    }
  }
}
```

Click `Execute` and this API call will set a standing query to match every single node and print its ID to standard out.

### Done. Now what?

Congratulations, you have successfully started to ingest Wikipedia updates to your graph! With this basic example you should now see output like the following in the console every time an update is made on Wikipedia:

```
2022-02-03 00:10:02,329 Standing query `print-output` match: {"meta":{"isPositiveMatch":true,"resultId":"c71223f9-b927-4d58-cc90-422282f0192a"},"data":{"id(n)":"0aa51d39-2fbe-4ba6-bd74-6251891b9f3c"}}
```

All the data that streams in from Wikipedia is saved in the graph, but we didn't choose to output any of that in the results.

If you'd like to inspect the data in the graph, you can open a web browser pointed to <http://localhost:8080> and run a query like `CALL recentNodes(10)` in the @ref:[Exploration UI](exploration-ui.md). Quine will render a handful of disconnected nodes. The ingest query did nothing interesting, it only wrote each incoming event into a single disconnected node. <!-- A [richer ingest query](enriched-example.md) would make a much more interesting graph. -->

This example is a foundation that you can build on top of to ingest and interpret your own streams of data. Next steps to experiment might be to change things like:

- Which data source is streaming in
- What graph structure is being built from each incoming record
- What data patterns would be more interesting to look for in the graph
- Where else to send results when data is matched
- Consider what you could do if your matches called back in to the graph to trigger other updates; this could be the start of a data cleaning pipeline.
- What if you added a second data source, merged into the same graph?

<!--
## Enriched Example

For a more interesting example, continue to the [Enriched Example](enriched-example.md) on the next page.
-->
