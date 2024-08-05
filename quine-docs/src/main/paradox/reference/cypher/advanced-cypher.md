---
description: Quine Cypher Enhancements
---
# Quine Cypher Enhancements

Quine extends OpenCypher with functionality unique to processing complex event streams. In addition to functions like `idFrom()` to calculate node IDs, the Quine Cypher interpreter includes functions and procedures used in data application development.

## Casting Property Types

OpenCypher defines three classes of data types; property types, composite types, and structural types. Quine supports storing `INTEGER`, `FLOAT`, `STRING`, and `BOOLEAN` property types as properties. As of release v1.5.2, Quine extends the specification to also include storing composite types `MAP` and `LIST OF ANY` as properties in nodes. Additionally, Quine provides functions to cast between these types in Cypher queries.  

Quine Cypher implements two families of functions to cast property values, `castOrThrow` and `castOrNull`. See the custom @ref:[Cypher functions](cypher-functions.md) page for the complete documentation.

The `castOrThrow` family of cast functions will cause the query to error if an argument of the wrong type is provided. This is the family you should use while writing pipelines to find errors sooner and when you know the structure of your data in advance.

The `castOrNull` family of cast functions will allow the query to continue processing by returning `null` if an argument of the wrong type is provided. This is useful when you don't know the structure of your data in advance, but your query must be more complex to handle possibly receiving a `null` as an indicator of a type mismatch.

For example, in Quine, a `MAP` property like `SET n.family = {dad: "Adam", mom: "Becca", brother: "Charlie"}` is allowed.

@@@ note

While Quine can store both property and composite types within the same node, it cannot store structural types like `NODE`, `RELATIONSHIP`, and `PATH` as properties, even as part of a composite.

@@@

Quine allows the use of composite property values, but the Cypher compiler may fail to detect their validity. Suppose we want to `UNWIND` the above family into individual rows. A natural query would be:

```cypher
MATCH (n) WHERE meta.type(n.family) = 'MAP'
WITH n.family AS family
UNWIND keys(family) AS relation
RETURN relation, family[relation] AS name
```

Running the above query will produce an error indicating `"Type mismatch: expected Map, Node or Relationship"` on `keys(family)`. This occurs when the Cypher compiler assumes `n.family` must be a standard property type and, therefore, can't be `MAP`, a composite type. For this query to run, we must _cast_ `n.family` as a `MAP` to pre-select the property type, so the compiler is not confused.

@@@ note

Cast functions are Cypher functions that have no effect at runtime but provide the compiler a hint about their argument type. Here are some examples of expressions using cast functions:

- `castOrThrow.integer(1)` returns `1`, since `1` is a valid `INTEGER`
- `castOrThrow.float(2)` fails the query, since the `INTEGER` `2` is not a `FLOAT`
- `castOrNull.map(7)` returns `null`, since the `INTEGER` `7` is not a `MAP`
- `castOrNull.float(3.0)` returns `3.0`, since `3.0` is a valid `FLOAT`

@@@

We know that `n.family` is a map (because `meta.type(n.family) = 'MAP'`); we use `castOrThrow.map` as a hint to the query compiler:

```cypher
MATCH (n) WHERE meta.type(n.family) = 'MAP'
WITH castOrThrow.map(n.family) AS family
UNWIND keys(family) AS relation
RETURN relation, family[relation] AS name
```

## Atomic Property Updates

Stateful operations in a streaming system generally read data (like a property) or write to it. In most cases, this is all that is needed since more complicated operations can be broken down into individual reads and writes.

For example, `SET n.x = n.x + 1` can be broken down into a read operation (retrieving `n.x`) and a write operation (`SET`ing `n.x` to its new value). These stateful operations are _atomic_ — that is, they lock the node and will either succeed (getting back or setting the value) or fail (getting back an error or a `null`).

Quine v1.5.2 introduced a family of atomic Cypher procedures for performing more complicated read-operate-write operations that lock the node for the duration of the operation. These atomic procedures are:

- `int.add`, add an integer to a property, or set the property to the integer
- `float.add`, add a float to a property, or set the property to the float
- `set.insert`, insert an element to a list, treating that property like a set
- `set.union`, merge a set (represented as a list) into a list, treating the property as a set

Like read and write operations, a call to these procedures is atomic and will lock the node against other operations while it executes. The procedures can be used to enforce the consistency of properties affected by multiple parallel operations.

Suppose we want to count the number of nodes (spokes) connected to a central node (hub). We could register a standing query looking for the spoke-hub relationship and increment a counter on the hub node in response to the standing query matching a spoke. For example:

```cypher
MATCH (hub) WHERE id(hub) = $that["id(hub)"]
WITH coalesce(hub.numberOfMatches, 0) AS oldMatchCount
SET hub.numberOfMatches = oldMatchCount + 1
RETURN oldMatchCount + 1 AS result
```

This works as long as spokes are connected in strict sequence, always waiting for the `hub`'s `numberOfMatches` to be updated before connecting the next spoke. However, that approach requires a lot of waiting, which means latency for the overall data pipeline. The problem here is that if two spokes are added simultaneously, the `numberOfMatches` might only be increased by 1, depending on the order in which the two update streams execute the read and write events. (This is a classic example of a _race condition_).

@@@ note {title='Hypothetical example of race condition'}

Two streams are both intending to increment `hub.numberOfMatches`

- Stream 1 reads hub.numberOfMatches = 0
- Stream 2 reads hub.numberOfMatches = 0
- Stream 1 writes hub.numberOfMatches = 1
- Stream 2 writes hub.numberOfMatches = 1

@@@

Alternatively, use `int.add` to increment the `numberOfMatches` property on `hub` node. This allows Quine to act on "increment" events instead of "read" and "write" events:

```cypher
MATCH (hub) WHERE id(hub) = $that["id(hub)"]
CALL int.add(hub, "numberOfMatches", 1) YIELD result
RETURN result
```

Or, more succinctly:
```cypher
CALL int.add($that["id(hub)"], "numberOfMatches", 1)
```

@@@ note {title='Sequence of events with atomic update procedure'}

Two streams are both intending to increment `hub.numberOfMatches`

- Stream 2 adds 1 to `hub.numberOfMatches`, yielding the value 1
- Stream 1 adds 1 to `hub.numberOfMatches`, yielding the value 2

@@@

By performing the entire increment update atomically, there is no opportunity for a race condition to arise. Regardless of which stream performs an update first, completing both updates will leave the counter's final value at the correct value of `2`.

All of the functions introduced in v1.5.2 share this same principle of atomicity. For example, `set.union`, not only combines a set into another, it does so atomically. Thus, the query below adds `Paladin` and `Bard` (if they weren't already present) to the property `adventuringParty` on the node `dungeonId`:

```cypher
CALL set.union(dungeonId, "adventuringParty", ["Paladin", "Bard"])
```
