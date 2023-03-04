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
