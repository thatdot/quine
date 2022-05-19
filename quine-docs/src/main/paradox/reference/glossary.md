# Glossary

<!--
Conventions for writing this document: 
- Bold and capitalize each term.
- Multi-word terms should be written like a dictionary entry.
- Separate the term and the definition with ` - `
- The first phrase/sentence of the definition should begin with a lowercase and end with a period.
- Each additional sentence in the definition should be a complete sentence and use proper casing and punctuation.
- Alphabetize the list.
- Use "See:" to suggest further detailed reading on the same topic. Use "See also:" to suggest related topics.
-->

This page is an alphabetical list of technical terms which have a particular meaning for the Quine project.

- **Actor** - a unit of concurrent computation. An actor is a lightweight computational element which is single-threaded, encapsulates state, and communicates via message-passing.
- **Cypher** - a graph query language. It is used by Quine for ingesting data, running ad hoc queries, and setting standing queries. See: "@ref:[Cypher Language](cypher/cypher-language.md)"
- **Edge** - a relationship between exactly two nodes in a graph.
- **Event Sourcing** - the practice of saving -updates- to state instead of the total current state. @link:[Event Sourcing](https://martinfowler.com/eaaDev/EventSourcing.html){ open=new } adds data to an append-only log.
- **Exploration UI** - an interactive user interface served by the Quine web server. See: "@ref:[Exploration UI](../getting-started/exploration-ui.md)"
- **Graph** - a logical data structure composed of Nodes and Edges.
- **Gremlin** - a graph traversal language. It is used by Quine for ad hoc queries and quick queries in the Exploration UI. See: "@ref:[Gremlin Language](gremlin-language.md)"
- **ID Provider** - a user-configurable component of Quine which determines the type of ID used by each node in the graph. See: "@ref:[ID Provider](../components/id-provider.md)"
- **Member** - a computer that participates in a cluster. In other systems this is sometimes called a "node". Quine avoids using the term "node" to refer to a cluster member so as not to be confused with a node or vertex in a graph. See also: "Node"
- **Node** - a vertex in a graph. Node are uniquely defined by their ID and serve as a container for properties, connected to other nodes by edges.
- **Persistor** - a Quine-specific data storage mechanism used to persist data durably on disk. It may be on the same machine (even in the same process) as the Quine, or it may be served on a remote system. See: "@ref:[Persistor](../components/persistors/persistor.md)"
- **Position** - a logical location in a Quine cluster which needs to be filled by a member. 
- **Property** - a key-value pair stored on a node. They key of a property is a string, the value is any one of the supported types in Quine.
- **Quick Query** - a query defined in the Exploration UI which enables easy graph exploration by right-clicking a node and selecting the quick queries enabled on that node. Quick queries are user customizable through the REST API or recipes.
- **QuineId** - the ID of a single node in the graph. See also: "ID Provider"
- **Recipe** - a set of configuration used to execute end-to-end functionality for a specific purpose. Recipes are contributed by community users and listed on the [Recipes](https://quine.io/recipes) page.
- **Shard** - a logical division of the graph, responsible for managing a subset of nodes.
- **Standing Query** - a query set on the graph, which lives inside the graph, efficiently propagates, and produces results immediately.
