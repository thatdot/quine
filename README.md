![Build and Test](https://github.com/thatdot/quine/workflows/CI/badge.svg)
[![GitHub Release](https://img.shields.io/github/v/release/thatdot/quine)](https://github.com/thatdot/quine/releases)
[![Docker Pulls](https://img.shields.io/docker/pulls/thatdot/quine)](https://hub.docker.com/r/thatdot/quine)
[![slack](https://img.shields.io/badge/slack-Quine-brightgreen.svg?logo=slack)](https://that.re/quine-slack)

<div style="padding-top: 10px;">
  <div style="vertical-align:middle;">
    <img width="400" height="100%" src="https://quine.io/assets/images/quine_logo.svg">
  </div>
  <div style="vertical-align:middle;">
    <p>Quine is a streaming graph interpreter; a server-side program that consumes data, builds it into a stateful graph structure, and runs live computation on that graph to answer questions or compute results. Those results stream out in real-time.</p>
  </div>
</div>

You interact with Quine by connecting it to a data source (Kafka, Kinesis Data Stream, SQS, files, stdin, etc.) and using regular database queries to stream that data in, build the graph structure, and find important patterns.

Three design choices define Quine, setting it apart from all event stream processing systems:

1. A graph-structured data model
2. An asynchronous actor-based graph computational model
3. Standing queries

Standing queries live inside the graph and automatically propagate the incremental results computed from both historical data and new streaming data. Once matches are found, standing queries trigger actions using those results (e.g., execute code, transform other data in the graph, publish data to another system like Apache Kafka or Kinesis).

![](https://uploads-ssl.webflow.com/61f0aecf55af2565526f6a95/62d8b7a7a13f0ca333a8b115_R9g-L0bLE2nguGQ3BRektSDq1d4L9Gtzao1fK3wuwgkX_iGkcgtGYlOR2u3p6DsWbrIrZbUPY6VtLULwj2BoIO2-gVUngIcrk-z-9H3u7a6QPIM7sqBRrkatR1YxA7WLR5CuvP3ZCo6JypuAWww23g.png)

All together, Quine can:

* Consume high-volume streaming event data
* Convert it into durable, versioned, connected data
* Monitor that connected data for complex structures or values
* Trigger arbitrary computation on the event of each match

This collection of capabilities is profoundly powerful! It represents a complete system for stateful event-driven arbitrary computation in a platform scalable to any size of data or desired throughput.

Read the docs at [quine.io](https://quine.io) to learn more.  

## Building from source

In order to build Quine locally, you'll need to have the following installed:

  * A recent version of the Java Development Kit (11 or newer)
  * The [`sbt` build tool](https://www.scala-sbt.org/download.html)
  * Yarn 0.22.0+ (for frontend components of `quine-browser` subproject)

Then:

```
sbt compile           # compile all projects
sbt test              # compile and run all projects' tests
sbt fixall            # reformat and lint all source files
sbt quine/run         # to build and run Quine
sbt quine/assembly    # assemble Quine into a jar 
```

## Launch Quine:

Run Quine from an executable `.jar` file built from this repo or downloaded from the repo [releases](https://github.com/thatdot/quine/releases) page. 

```shell
❯ java -jar quine-x.x.x.jar -h
Quine universal program
Usage: quine [options]

  -W, --disable-web-service
                           disable Quine web service
  -p, --port <value>       web service port (default is 8080)
  -r, --recipe name, file, or URL
                           follow the specified recipe
  -x, --recipe-value key=value
                           recipe parameter substitution
  --force-config           disable recipe configuration defaults
  --no-delete              disable deleting data file when process exits
  -h, --help
  -v, --version            print Quine program version
```

For example, to run the [Wikipedia page ingest](https://quine.io/recipes/wikipedia/) getting started recipe:

``` shell
❯ java -jar quine-x.x.x.jar -r wikipedia
 ```

With Docker installed, run Quine from Docker Hub.

``` shell
❯ docker run -p 8080:8080 thatdot/quine
```

The [quick start](https://quine.io/getting-started/quick-start/) guide will get you up and running the first time, ingesting data, and submitting your first query.

## Quine Recipes

Quine recipes are a great way to get started developing with Quine. A recipe is a document that contains all the information necessary for Quine to execute any data processing task. Ingest data from batch sources like `.json` or `.csv` files hosted locally, or connect streaming data from Kafka or Kinesis. 

[Recipes](https://quine.io/components/recipe-ref-manual/) are `yaml` documents containing the configuration for components including:

* [Ingest Streams](https://quine.io/components/ingest-sources/) to read streaming data from sources and update graph data
* [Standing Queries](https://quine.io/components/standing-queries/) to transform graph data, and to produce aggregates and other outputs
* UI configuration to specialize the web user interface for the use-case that is the subject of the Recipe

Please see [Quine's Recipe repository](https://quine.io/recipes/) for a list of available Recipes. Or create your own and contribute it back to the community for others to use.

## Contributing to Quine

The community is the heart of all open-source projects. We welcome contributions from all people and strive to build a welcoming and open community of contributors, users, participants, speakers, and lurkers. Everyone is welcome.

More information is included in our [contribution](https://github.com/thatdot/quine/blob/main/CONTRIBUTING.md) guidelines.
