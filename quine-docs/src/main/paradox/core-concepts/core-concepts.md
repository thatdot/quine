---
description: Quine is a streaming graph interpreter meant to trigger actions in real-time based on complex patterns pulled from high-volume streaming data
---
# Main Concepts

## Everything Needed Between Streams

Quine represents a significant new architectural component for enterprise data pipelines.

Quine is a stateful streaming graph interpreter. It consumes high volume data streams and publishes processed results to other streaming data consumers.

Quine eliminates the complex technical challenges of managing data ordering, time windowing, vertical and horizontal scalability, and the complex asynchronous processing needed to find compound objects or patterns spread across data streams.

Quine is easily integrated into existing @ref:[data pipelines](streaming-systems.md) and highly scalable across existing and next-generation enterprise infrastructure.

## Graph Model for Data and Computation

Quine is built around a novel design choice: **to represent both the data model and the computational model using a graph**.

In fact, the same graph is used for both models. Using the graph structure for both the data model and the computational model allows Quine to provide remarkable new capabilities including the ability to ingest events from multiple source, materialize complex relationships as a single graph, and emit results with sub-millisecond latency.

### Data Model

A graph consists of nodes connected by edges. Each node is unique from other nodes and has a unique ID to distinguish it.

Nodes contain additional data from key-value pairs stored as properties on that node. In practice, this makes a node similar to a JSON object or a Python dictionary: it stores any arbitrary data.

Nodes are connected to other nodes by edges. This `Node-Edge-Node` pattern that makes up a graph is analogous to the `Subject-Predicate-Object` form used by most human languages.

This pattern makes a graph profoundly expressive, allowing it to represent both data and interrelationships between that data that might otherwise be stored in other systems like a relational database or NO-SQL document store.

The graph model surfaces the interconnected nature of data as a core part of the data model. It also provides the ideal sweet spot between structured data that can be efficiently traversed and schema-less flexibility for fitting in new data, which wasn't considered when the system was first set up.

### Computational Model

In the Quine system, nodes in the graph are backed by actors as needed to perform computation.

An actor is a lightweight computational unit, similar to a thread, but designed to be highly resource-efficient so that thousands or even millions of actors can run simulateneously in the same system.

Actors supervise other actors in a managed hierarchy to ensure that failure is well-contained and the system remains resilient in the presence of failure.

This mechanism, known as @link:[the Actor Model](https://en.wikipedia.org/wiki/Actor-model){ open=new }, goes back to the 1970s and was a vital aspect of the resilient computer systems that made up the telecommunications infrastructure of the 1980s and 1990s. These brilliant ideas remained dormant for decades until the massive parallelism and complexity of modern Internet-scale computing have driven many cutting-edge tools to revive this battle-tested method for building robust, scalable systems.

## Selecting Nodes vs Searching for Nodes

With a graph data model, nodes are the primary unit of data — much like a "row" is the primary unit of data in a relational database. However, unlike traditional graph data systems, a Quine user never has to create a node directly. Instead, the system behaves as if all nodes exist but don't yet contain meaningful data.

> As data streams into the system, the node acquires meaningful data, and Quine begins to create a history for the node.

Quine adds an `idFrom` function to Cypher that takes any number of arguments and deterministically produces a node ID from that data. This is similar to a consistent-hashing strategy, except that the ID produced by this function is always a Quine node ID that matches the selected ID type in the configuration.

The `idFrom` function simplifies the selection of nodes within Cypher queries, enabling MATCH to assume the form:

```cypher
MATCH (n) WHERE id(n) = idFrom($that) SET n.line = $that
```

## Historical Versioning

By default, each node in the graph maintains a record of its historical changes over time. When a node's properties or edges change, the change event and timestamp are saved to an append-only log for that particular node. This technique is known as @link:[Event Sourcing](https://martinfowler.com/eaaDev/EventSourcing.html){ open=new }, applied individually to each node. This historical log can be replayed up to any desired moment in time, allowing for the system to quickly answer questions using the state of the graph *as it was in the past.*

@@@ note

When you delete a node, it's not completely gone, but rather its history is updated to indicate that all its properties and edges were removed at the time the deletion occurred. At any given moment, if a node is empty, it is essentially the same as a non-existent node - the only difference being that because the node has history it will be included in the count returned by queries like `MATCH (a) RETURN count(a)` or show up in query results that perform a full node scan.

@@@

## Real-Time Pattern Matching With Standing Queries

Work in Quine is done primarily through the expression of patterns to be found while incoming streams of data are being processed. First, you must declare the shape of data (the nodes and edges that will define your initial graph state). Then you should determine the sub-graph represening the interesting or valuable pattern of events that you want to find in the stream using Cypher. Finally, you will define the action you want to be triggered each time that pattern is found (this is accomplished using a standing query). The matching and discovery of every instance of each pattern happen automatically, and the corresponding data is used to trigger the desired action.

The action triggered may be one of many possible options upon matching a pattern.

* An update or annotation of the existing data can be triggered to enrich the data.
* An external service can be queried, and the result is fed into this system as another new event.
* The matching data can be packaged up (e.g., as a JSON object) and published to another message queue to be consumed by another service.
* Custom queries, algorithms, or code can even be triggered using data from the matching pattern to execute arbitrary actions.

## Backpressuring

Inevitably, when streaming data producers outpace consumers, the consumer will become overwhelmed. Quine manages the data flow to avoid becoming overwhelmed using "backpressure."

The problem with buffering is that a buffer will eventually run out of space. The system must then decide what to do when the buffer is full: drop new results, drop old results, crash the system, or backpressure.

A backpressured system does not buffer, and it causes producers upstream to not send data at a rate greater than it can process. 

Backpressure is a [protocol](https://www.reactive-streams.org/) that defines how to send a logical signal back UP the stream with information about the downstream consumers readiness to receive more data. If downstream is not ready to consume, then upstream does not send new data.

Quine uses a reactive stream implementation of backpressure, [Akka Streams](https://doc.akka.io/docs/akka/current/stream/stream-flows-and-basics.html#core-concepts), built on top of the actor model to ensure that the ingestion and processing of streams are resilient.

## Stateful Event-Driven Computation

All together, Quine is able to:

* Consume high-volume streaming event data
* Convert it into durable, versioned, connected data
* Monitor that connected data for complex structures or values
* Trigger arbitrary computation on the event of each match

This collection of capabilities is profoundly powerful. It represents a complete system for stateful event-driven arbitrary computation in a platform scalable to any size of data or desired throughput.
