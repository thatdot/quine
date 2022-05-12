# Main Concepts

## Graph Model for Data and Computation

Quine is built around a novel design choice: to represent both the data model and the computational model using a graph. In fact, the same graph is used for both models. Using the same model for both the data model and the computational model helps provide remarkable new capabilities from the Quine system.

### Data Model

A graph is built of nodes connected to each other by edges. Think of nodes as circles on a canvas and edges as arrows that connect them. Each node is unique from other nodes because is has a unique ID to distinguish it. Nodes also contain additional data in the form of key-value pairs stored on that node. In practice, this makes a node similar to a JSON object or a Python dictionary: it stores any arbitrary data.

Nodes are also connected to other nodes by edges. This `Node-Edge-Node` pattern that makes up a graph is analogous to the `Subject-Predicate-Object` pattern used by most human languages. This makes a graph profoundly expressive, able to capture any data that might be stored in other systems like a relational database or NO-SQL document store. But the graph model surfaces the interconnected nature of data as a core part of the data model. It also provides the ideal sweet spot between structured data that can be efficiently traversed and a schema-less flexibility for fitting in new data which wasn't considered when the system was first set up.

<!--
For advice on getting started modeling your data in a graph, see: [Modeling Data in a Graph](../getting-started/modeling-data-in-a-graph.md)
-->

### Computational Model

In the Quine system, nodes in the graph are backed by Actors as needed to perform computation. An Actor is a lightweight computational unit, similar to a thread, but designed to be extremely resource efficient so that thousands, or millions of actors might run in the same system. Actors supervise other actors in a managed hierarchy to ensure that failure is well-contained and the system remains resilient in the presence of failure. This mechanism, known as @link:[the Actor Model](https://en.wikipedia.org/wiki/Actor-model){ open=new }, goes back to the 1970s and was a key aspect of the resilient computer systems that made up the telecommunications infrastructure of the 1980s and 1990s. These brilliant ideas seemed to lay largely dormant for decades until the massive parallelism and complexity of modern Internet-scale computing has driven many cutting edge tools to revive this battle-tested method for building robust scalable systems.

## All Nodes Exist

With a graph data model, nodes are the primary unit of data—much like a "row" or "tuple" is the primary unit of data in a relational data processing system. Unlike traditional graph data systems, a Quine user never has to create a node. Instead, the system functions as if all nodes exist. Since a graph derives its value from the connections between the data, having a node exist without any properties stored on that node or any edges connecting it to others makes that node entirely uninteresting. Quine therefore represents every possible node as an existing empty node with no interesting history. As data streams into the system, it "breaks the symmetry" and creates a history for nodes that are worth looking at more closely.

## Historical Versioning

By default, each node in the graph maintains a record of all of its historical changes over time. When a node's properties or edges are changed, the change event and timestamp are saved to an append-only log for that particular node. This is a technique known as @link:[Event Sourcing](https://martinfowler.com/eaaDev/EventSourcing.html){ open=new }, applied individually to each node. This historical log can be replayed up to any desired moment in time, allowing for the system to quickly answer questions using the state of the graph -as it was in the past.-

## Real-Time Pattern Matching With Standing Queries

Using Quine is done primarily through the expression of patterns to be found while incoming streams of data are being processed. The system supports many possible mechanisms for expressing those patterns, but the matching and discovery of every instance of each pattern happens automatically, and the corresponding data is used to trigger a desired action. Consequently, a user can simply declare the kinds of data they want to find in their stream and specify the action they want triggered each time that pattern is found. Quine will automatically find the patterns and trigger the corresponding action.

Upon matching a pattern, the action triggered may be one of many possible options. An update or annotation of the existing data can be triggered to enrich the data. An external service can be queried and the result fed back into this system as another new event. The matching data can be packaged up (e.g. as a JSON object) and published to another message queue to be consumed by another service. Custom queries, algorithms, or code can even be triggered using data from the matching pattern to execute arbitrary actions.


## Stateful Event-Driven Computation

All together, Quine is able to: 

- consume high-volume streaming event data
- convert it into durable, versioned, connected data
- monitor that connected data for complex structures or values
- trigger arbitrary computation on the event of each match

This collection of capabilities is profoundly powerful! It represents a complete system for stateful event-driven arbitrary computation in a platform scalable to any size of data or desired throughput.


## Everything Needed Between Streams

Quine represents a major new architectural component for enterprise data pipelines. Quine is a stateful streaming graph interpreter. It consumes from high-volume input data streams, and publishes compound and meaningful processed results out to other high-volume data streams. Quine eliminates the difficult technical challenges of managing data ordering, time windowing, vertical and horizontal scalability, and the complex asynchronous processing needed to find compound objects spread out through data streams. It is easily integrated into existing data pipelines and highly scalable across existing and next-generation enterprise infrastructure.
