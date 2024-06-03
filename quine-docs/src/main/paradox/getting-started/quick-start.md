---
description: Get up and running with Quine streaming graph
---
# Quick Start

## What is Quine

Quine combines the real-time event stream processing capabilities of systems like Flink and ksqlDB with a graph-structured data model as found in graph databases like Neo4j and TigerGraph. Quine is a key participant in a streaming event data pipeline that consumes data, builds it into a graph structure, runs computation on that graph to answer questions or compute results, and then stream them out.

![Quine Streaming Graph Pipeline](https://uploads-ssl.webflow.com/61f0aecf55af2565526f6a95/62d8b7a7a13f0ca333a8b115_R9g-L0bLE2nguGQ3BRektSDq1d4L9Gtzao1fK3wuwgkX_iGkcgtGYlOR2u3p6DsWbrIrZbUPY6VtLULwj2BoIO2-gVUngIcrk-z-9H3u7a6QPIM7sqBRrkatR1YxA7WLR5CuvP3ZCo6JypuAWww23g.png)

All together, Quine can:

* Ingest high-volume streaming event data from stream processing systems like Kafka, Kinesis, APIs or databases/data warehouses
* Convert and merge it into durable, versioned, connected data (a graph)
* Monitor that connected data for complex structures or values
* Trigger arbitrary computation in the event of each match
* Emit high-value events out to stream processing systems like Kafka, Kinesis, APIs or databases/data warehouses

This collection of capabilities represents a complete system for stateful event-driven arbitrary computation in a platform scalable to any size of data or desired throughput.

## Before you begin

Concepts that you should already be familiar with:

* Event streaming architectures
* Graph database concepts
* Cypher graph query language
* Interacting with REST API endpoints

If you are unsure of any of these concepts, please consider going through the more in depth @ref:[installing Quine](installing-quine-tutorial.md) materials as your introduction to Quine.

## Install Quine

Start Quine using a distribution or locally compiled source code (described below).

### From a Docker container

* With Docker installed, run Quine from Docker Hub.
* `docker run -p 8080:8080 thatdot/quine`

### From an executable

Quine requires a Java 11 or newer JRE.

* @link:[Download](https://quine.io/download) { open=new } the executable `jar` file.
* From a working directory: `java -jar quine-x.x.x.jar`

### From source code

* Clone the @link:[source code](https://github.com/thatdot/quine) { open=new } from GitHub.
* Ensure that you have a recent Java Development Kit (11 or newer) and `sbt` installed.
* From the main directory of the repository on your machine: `sbt quine/run`

## Connect to Quine

There are two main structures that you need to configure in Quine; the @link:[**ingest stream**](https://docs.quine.io/reference/rest-api.html#/schemas/com.thatdot.quine.routes.IngestStreamConfiguration) { open=new } forms the event stream into the graph, and @link:[**standing queries**](https://docs.quine.io/reference/rest-api.html#/schemas/com.thatdot.quine.routes.StandingQueryDefinition) { open=new } which match and take action on nodes in the graph. Follow the links in the tutorial to the API documentation to learn more about the schema for each object.

### Connect an Event Stream

For example, let's ingest the live stream of new pages created on Wikipedia; @link:[mediawiki.page-create](https://stream.wikimedia.org/?doc#/streams/get_v2_stream_mediawiki_page_create) { open=new }.

Create a "server sent events" ingest stream to connect Quine to the `page-create` event stream using the @link:[ingest](https://docs.quine.io/reference/rest-api.html#/paths/api-v1-ingest-name/post) { open=new } API endpoint.

Issue the following `curl` command in a terminal running on the machine were you started Quine.

```shell
curl -X "POST" "http://127.0.0.1:8080/api/v1/ingest/wikipedia-page-create" \
     -H 'Content-Type: application/json' \
     -d $'{
  "format": {
    "query": "CREATE ($that)",
    "parameter": "that",
    "type": "CypherJson"
  },
  "type": "ServerSentEventsIngest",
  "url": "https://stream.wikimedia.org/v2/stream/page-create"
}'
```

**Congratulations!** You are ingesting raw events into Quine and manifesting nodes in the graph.

Let's look at a node to see what it contains by submitting a Cypher request via the @link:[query/cypher](https://docs.quine.io/reference/rest-api.html#/paths/api-v1-query-cypher/post) { open=new } API endpoint.

```shell
curl -X "POST" "http://127.0.0.1:8080/api/v1/query/cypher" \
     -H 'Content-Type: text/plain' \
     -d "CALL recentNodes(1)"
```

This query calls the `recentNodes` Cypher procedure to retrieve the most recent one (1) node.

```json
{
  "columns": [
    "node"
  ],
  "results": [
    [
      {
        "id": "7a9a936f-ae1a-49c5-ba99-0ec6401bfd7d",
        "labels": [],
        "properties": {
          "database": "enwikisource",
          "rev_slots": {
            "main": {
              "rev_slot_content_model": "proofread-page",
              "rev_slot_origin_rev_id": 12558576,
              "rev_slot_sha1": "lqrhvc49cgzegvvfqnzg3c6bpxs55up",
              "rev_slot_size": 1699
            }
          },
          "rev_id": 12558576,
          "rev_timestamp": "2022-08-23T18:34:25Z",
          "rev_len": 1699,
          "rev_minor_edit": false,
          "parsedcomment": "<span dir=\"auto\"><span class=\"autocomment\">Proofread</span></span>",
          "page_title": "Page:The_Works_of_H_G_Wells_Volume_6.pdf/423",
          "rev_content_format": "text/x-wiki",
          "page_id": 4034745,
          "page_is_redirect": false,
          "meta": {
            "domain": "en.wikisource.org",
            "dt": "2022-08-23T18:34:25Z",
            "id": "3c8c9150-19aa-4f33-be5d-bf3ef8d8a994",
            "offset": 241649600,
            "partition": 0,
            "request_id": "0ba80d5c-3c97-4971-b0d2-360c5d20e0f6",
            "stream": "mediawiki.page-create",
            "topic": "eqiad.mediawiki.page-create",
            "uri": "https://en.wikisource.org/wiki/Page:The_Works_of_H_G_Wells_Volume_6.pdf/423"
          },
          "page_namespace": 104,
          "rev_sha1": "lqrhvc49cgzegvvfqnzg3c6bpxs55up",
          "comment": "/* Proofread */",
          "rev_content_model": "proofread-page",
          "$schema": "/mediawiki/revision/create/1.1.0",
          "performer": {
            "user_edit_count": 10176,
            "user_groups": [
              "autopatrolled",
              "*",
              "user",
              "autoconfirmed"
            ],
            "user_id": 141433,
            "user_is_bot": false,
            "user_registration_dt": "2009-07-12T12:33:52Z",
            "user_text": "MER-C"
          }
        }
      }
    ]
  ]
}
```

The API call is the functional equivalent to issuing the `CALL recentNodes(1)` query in the Exploration UI:
![image](https://user-images.githubusercontent.com/99685020/186532706-15ea5919-9391-4d0a-87a1-488d3c551cd2.png)

@@@ note
Your API response will contain a different set of parameters than above because you are ingesting a stream of live events from Wikipedia.
@@@

This ingest stream is performing the most basic of ETL functionality, it manifests a disconnected node directly from each event emitted from the Wikipedia event stream.

### Create a Standing Query

A Standing Query matches some graph structure incrementally while new event data is ingested. Creating a standing query is done with a single call to the @link:[standing/query](https://docs.quine.io/reference/rest-api.html#/paths/api-v1-query-standing-standing-query-name/post) { open=new } API endpoint.

Right now, Quine is the only component in our data pipeline. Let's configure a standing query that watches for new nodes to enter the graph and print the node contents to the console.

@@@ note
A standing query can emit the event data, re-form the event into new events, or trigger actions to inform elements downstream in your data pipeline (e.g., a Kafka topic).
@@@

```shell
curl -X "POST" "http://127.0.0.1:8080/api/v1/query/standing/wikipedia-new-page-node" \
     -H 'Content-Type: application/json' \
     -d $'{
  "pattern": {
    "query": "MATCH (n) RETURN DISTINCT id(n)",
    "type": "Cypher"
  },
  "outputs": {
    "print-output": {
      "type": "PrintToStandardOut"
    }
  }
}'
```

You will see new node events similar to the one below appear in the same console window where you launched Quine immediately after running the `curl` command. These events contain the `id` of each new node created in the graph.

```shell
2022-08-23 14:06:55,174 Standing query `print-output` match: {"meta":{"isPositiveMatch":true},"data":{"id(n)":"911d88e0-413a-42bd-a0f8-dd15bbf6aff6"}}
```

## Ending the Stream

This quick-start is a foundation that you can build on top of to ingest and interpret your own streams of data. But for now, we can @link:[pause](https://docs.quine.io/reference/rest-api.html#/paths/api-v1-ingest-name--pause/put) { open=new } the ingest stream and @link:[shutdown](https://docs.quine.io/reference/rest-api.html#/paths/api-v1-admin-shutdown/post) { open=new } Quine before moving on.

```shell
curl -X "PUT" "http://127.0.01:8080/api/v1/ingest/wikipedia-page-create/pause"
```

`curl` will return a confirmation that the ingest stream is paused and metrics about what had been ingested to that point.

```json
{
  "name": "wikipedia-page-create",
  "status": "Paused",
  "settings": {
    "format": {
      "query": "CREATE ($that)",
      "parameter": "that",
      "type": "CypherJson"
    },
    "url": "https://stream.wikimedia.org/v2/stream/page-create",
    "parallelism": 16,
    "type": "ServerSentEventsIngest"
  },
  "stats": {
    "ingestedCount": 3096,
    "rates": {
      "count": 3096,
      "oneMinute": 1.1004373606561983,
      "fiveMinute": 1.1045410320126854,
      "fifteenMinute": 1.123947504968256,
      "overall": 1.12992666124191
    },
    "byteRates": {
      "count": 4471549,
      "oneMinute": 1529.8568026569903,
      "fiveMinute": 1553.9585381108302,
      "fifteenMinute": 1614.01351325884,
      "overall": 1631.9520432126692
    },
    "startTime": "2022-08-23T18:30:58.571823Z",
    "totalRuntime": 2739271
  }
}
```

You can stop Quine by either typing `CTRL-c` into the terminal window or perform a *graceful shut down* by issuing a POST to the @link:[admin/shutdown](https://docs.quine.io/reference/rest-api.html#/paths/api-v1-admin-shutdown/post) { open=new } endpoint.

```shell
curl -X "POST" "http://127.0.0.1:8080/api/v1/admin/shutdown"
```

## Next Steps

Learn how Quine uses recipes to store a graph configuration and UI enhancements in the @ref:[recipes](recipes-tutorial.md) getting started guide.
