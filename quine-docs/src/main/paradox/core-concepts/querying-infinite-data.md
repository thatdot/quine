---
description: Quine is built for real-time operations on infinite datasets
---
# Querying Infinite Data

How do you query an infinite amount of data? Quine is built for real-time operations on infinite datasets. That calls for a slightly different approach to data analysis.

Unless you are running in a debug environment that does not have much data, avoid queries that involve scanning the whole data store. The typical examples of queries that require this sort of scan are `MATCH (n) RETURN n LIMIT 20` or `MATCH (n) RETURN count(*)`. These queries are tempting to use for exploration on traditional databases, but the design of Quine calls for a different approach.

In a streaming system like Quine we have the advantage of having new incoming data in hand at the perfect moment. New data streaming in gives us the starting point needed to find other data efficiently. In a sense, the new data behaves like an index into the old data. However, when issuing queries to explore an existing dataset, you may not have that incoming new data to use as a starting point. So the first goal of an exploration is to efficiently find starting points for the data you want to explore. Quine can fetch data extremely efficiently when provided an ID for a node in question.

## Using IDs in a Query

The simplest way to avoid a scan is to make sure at least some part of a `MATCH (n) …` pattern has a constraint of the form `id(n) = ...` in the `WHERE` clause. This allows the query compiler to optimize the execution plan to begin with a hop to a node with a known ID instead of needing to consider every node ever seen as a potential starting point.

In the example query below, constraining the `id(person)` is enough to avoid any sort of scan since the rest of the pattern is all somehow connected to `person`.

```cypher
// Get a particular person's name, and the names of their paternal grandparents
MATCH
  (person: Person)-[:has-father]->(dad: Person),
  (grandpa: Person)<-[:has-father]-(dad)-[:has-mother]->(grandma: Person)
WHERE id(person) = "a75a9f31-f7c3-32e2-bab6-0e5a2d690c7b"
RETURN
  person.name AS person,
  grandpa.name AS paternal-grandfather,
  grandma.name AS maternal-grandmother
```

Since the ID of a node is not usually known statically, we recommend using the `idFrom` function to have Quine compute the IDs of nodes based on data values. This is similar to a consistent-hashing approach where a collection of values are hashed together to produce a unique result that can be used for an ID. Since Quine supports many different kinds of IDs (numbers, UUIDs, strings, tuples of values, and more…), the `idFrom` will handle producing consistent results appropriate for the dataset regardless of the which ID type is used.

Consequently, the above example can be rewritten to -find- the ID using fields from the data:

```cypher
// Get a particular person's name, and the names of their paternal grandparents
MATCH
  (person: Person)-[:has-father]->(dad: Person),
  (grandpa: Person)<-[:has-father]-(dad)-[:has-mother]->(grandma: Person)
WHERE id(person) = idFrom('person', person.name)
RETURN
  person.name AS person,
  grandpa.name AS paternal-grandfather,
  grandma.name AS maternal-grandmother
```

## Finding Recently Accessed Nodes

Another common pattern for queries is to want to pull out some small number of examples of a structure (for instance to verify that data is being written in the desired graph structure). For those cases, the `recentNode` and `recentNodesIds` procedures are fast and efficient ways to get back a sample of recently modified/queried nodes or their IDs. These procedures both take an argument indicating the desired number of elements to sample.

```cypher
// Get 20 sample nodes:
CALL recentNodes(20)
// or with YIELD if you need to use each result in larger queries:
CALL recentNodes(20) YIELD node RETURN node
```

This approach can be used to anchor larger queries too, like the paternal grandparents query from above. This time, we don't need to know the ID of any of the nodes in the pattern—we just need to constrain at least one of them to be from `recentNodesIds`.

```cypher
// Sample the recent part of the graph looking for people and paternal grandparents
CALL recentNodesIds(1000) YIELD nodeId AS personId
MATCH
  (person: Person)-[:has-father]->(dad: Person),
  (grandpa: Person)<-[:has-father]-(dad)-[:has-mother]->(grandma: Person)
WHERE id(person) = personId
RETURN
  person.name AS person,
  grandpa.name AS paternal-grandfather,
  grandma.name AS maternal-grandmother
LIMIT 20
```

This query will explore the set of 1000 nodes that have most recently streamed in, and it will run the query on those IDs and return results if the larger structure in the `MATCH` clause matches the data found at the IDs returned from `recentNodesIds`.

The same approach of sampling recently touched nodes can be used to quickly compute some aggregate statistics over recent data. For instance, here is a query for quickly sampling the distributions of labels in recently created or accessed data:

```cypher
// Count the number of each type of node label for 1000 recently accessed nodes
CALL recentNodes(1000) YIELD node
RETURN labels(node), count(*)
```
