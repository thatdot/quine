---
description: Quine supports many ID providers but prefers the built in idFrom() that assumes all nodes exist
---

# Quine Indexing

How do you write a query against an infinite amount of data?

A streaming system like Quine continuously receives data from an upstream source. In a sense, the new data must manifest in the correct location in the graph and index into the previous data.

## ID Providers

Each node in Quine's graph is defined by its ID—referred to internally as `QuineId`. The ID itself is fundamentally an uninterpreted sequence of bytes, but using each ID is mediated by a class of objects referred to as `IdProviders`. ID providers make working with IDs more convenient and allow for multiple types to be used or migration from one type to another.

An ID Provider is chosen at startup for an instance of the graph. the default ID provider creates and expects to find IDs that can be read as UUIDs. This means that every node in the graph is defined by a UUID.

Different ID Providers can implement different strategies for allocating new IDs. For instance, alternate UUID providers can be configured to generate UUIDs which conform to specification of UUID versions 3, 4, or 5.

## Supported ID Providers

- `uuid` - This ID provider will generate RFC compliant UUIDs, but will allow for reading a looser interpretation of UUIDs which allows for any use of the 128 bits available in a UUID. This is the default ID provider.
- `uuid-3` - Generate and read only Version-3 compliant UUIDs.
- `uuid-4` - Generate and read only Version-4 compliant UUIDs. When returning random UUIDs, and using `idFrom` (described below), deterministic UUIDs with Version-4 identifying bytes will be used.
- `uuid-5` - Generate and read only Version-5 compliant UUIDs.
- `long` - Generate random integer IDs in the range: [-(2^53-1), 2^53-1] -- these may be safely used as IEEE double-precision floating-point values without loss of precision. This id scheme is not appropriate for large-scale datasets because of the high likelihood of a collision.
- `byte-array` - generate unstructured byte arrays as IDs.

## idFrom(…)

Quine has a unique challenge: how to maintain state for a potentially infinite stream of data. One key strategy Quine uses is to deterministically generate known IDs from data. The `idFrom` function does exactly that. `idFrom` is a function we've added to Cypher which will take any number of arguments and deterministically produce a ID from that data. This is similar to a consistent-hashing strategy, except that the ID produced from this function is always an ID that conforms to the type chosen for the ID provider.

For example, if data ingested from a stream needs to set a new telephone number for a customer, the Cypher ingest query would use `idFrom` to locate the relevant node in the graph and update its property like this:

```cypher
MATCH (customer) 
WHERE id(customer) = idFrom('customer', $that.customer-id)
SET customer.phone = $that.new-phone-number
```

In this example, `idFrom('customer', $that.customer-id)` is used to generate a specific ID for a single node in the graph, determined by the constant string `'customer'` and the value of `customer-id` being read from the newly streamed record. The ID returned from this function call will be a single fixed ID in the space of whichever ID provider was chosen (`long`, `uuid`, etc.). That ID is used in the `WHERE` clause to select a specific node from the graph, allowing Quine to perform the corresponding update efficiently.

## Using IDs in a Query

Quine fetches data when provided an anchor like a node ID. 
When issuing queries to explore an existing data set, all of the nodes necessary for your query may not be populated. So the first goal of an exploration is to efficiently find starting points for the data you want to explore.

Since the ID of a node is not usually known statically, we recommend using the `idFrom` function to have Quine compute the IDs of nodes based on data values.

```cypher
// Get a particular person's name, and the names of their paternal grandparents
MATCH
  (person :Person)-[:HAS_FATHER]->(dad :Person),
  (grandpa :Person)<-[:HAS_FATHER]-(dad)-[:HAS_MOTHER]->(grandma :Person)
WHERE id(person) = idFrom('person', person.name)
RETURN
  person.name AS person,
  grandpa.name AS paternalGrandfather,
  grandma.name AS maternalGrandmother
```
Unless you are running in a debug environment that has little data, we recommend avoiding queries that involve scanning the entire graph. The simplest way to avoid a scan is to make sure at least some part of a `MATCH (n) …` pattern has a constraint of the form `id(n) = ...` in the `WHERE` clause. This allows the query compiler to optimize the execution plan to begin with a hop to a node with a known ID instead of needing to consider every node ever seen as a potential starting point.

@@@ note
Examples of queries that scan the entire graph are `MATCH (n) RETURN n LIMIT 20` or `MATCH (n) RETURN count(*)`. These queries are tempting to use for exploration on traditional databases, but are not efficient to use in streaming analysis.
@@@

## Finding Recently Accessed Nodes

Another common query pattern is to pull out some small number of nodes from the graph (for instance to verify that data is being written in the desired graph structure). For those cases, the `recentNode` and `recentNodesIds` procedures are fast and efficient ways to get back a sample of recently modified/queried nodes or their IDs. These procedures both take an argument indicating the desired number of elements to sample.

```cypher
// Get 20 sample nodes:
CALL recentNodes(20)
// or with YIELD if you need to use each result in larger queries:
CALL recentNodes(20) YIELD node RETURN node
```

This approach can be used to anchor larger queries too, like the paternal grandparents query from above. This time, we don't need to know the ID of any of the nodes in the pattern -- we just need to constrain at least one of them using the `recentNodesIds`function.

```cypher
// Sample the recent part of the graph looking for people and paternal grandparents
CALL recentNodesIds(1000) YIELD nodeId AS personId
MATCH
  (person :Person)-[:HAS_FATHER]->(dad :Person),
  (grandpa :Person)<-[:HAS_FATHER]-(dad)-[:HAS_MOTHER]->(grandma :Person)
WHERE id(person) = idFrom('person', person.name)
RETURN
  person.name AS person,
  grandpa.name AS paternalGrandfather,
  grandma.name AS maternalGrandmother
LIMIT 20
```

This query will inspect the set of 1000 nodes that have most recently streamed in, and it will run the query on those IDs and return results if the larger structure in the `MATCH` clause matches the data found at the IDs returned from `recentNodesIds`.

The same approach of sampling recently touched nodes can be used to quickly compute some aggregate statistics over recent data. For instance, here is a query for quickly sampling the distributions of labels in recently created or accessed data:

```cypher
// Count the number of each type of node label for 1000 recently accessed nodes
CALL recentNodes(1000) YIELD node
RETURN labels(node), count(*)
```
