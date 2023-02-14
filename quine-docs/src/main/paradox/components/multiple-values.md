---
description: Quine has special considerations when an ingest query returns multiple values
---
# Experimental `MultipleValues` mode

@@@ warning

This feature is still in preview mode. It may

* use significantly more RAM
* use significantly more disk space
* degrade throughput of the system overall

@@@

This mode can be enabled by specifying `"mode": "MultipleValues"` when creating a standing query with the `/api/v1/query/standing/{name}` POST endpoint.

## Motivation

When running using this mode, Standing Queries will relax some of the constraints imposed by `DistinctId` (so `MultipleValues` should accept a superset of what `DistinctId` accepts). In particular, the `WHERE` and `RETURN` portions of the query are much more expressive. For example:

```cypher
// Locate people with a maternal grandpa "Joe" born less than 44 years apart
MATCH (person)-[:has-mother]->(mom)-[:has-father]->(grandpa { name: "Joe" })
WHERE person.yearBorn - grandpa.yearBorn < 44
RETURN id(person) AS id, person.name AS name
```

Using `DistinctId`, a filter condition such as `person.yearBorn - grandpa.yearBorn < 44` would need to be defined using a Cypher Standing Query output. Although using a Cypher output is often enough to manage more complex filtering or mapping of results, there still arise situations where this won't be enough since there isn't one canonical node that uniquely identifies the match. For example:

```cypher
// Find a guy how knows a guy who knows a guy who knows a guy
MATCH (n:Guy)-[:knows]->(m:Guy)-[:knows]-(o:Guy)-[:knows]->(p:Guy)
RETURN id(n), id(m), id(o), id(p)
```

In this case, there is no way that we could track the -path- of connections using a Standing Query with the `DistinctId` mode. Not only would it be impossible to distinguish multiple matches, but we would only get notified about the first time there was a match on the root node (which is the node whose ID would be returned).

## Syntax and Structure

The syntax and structure is designed to be a superset of the `DistinctId` mode. That is, any `DistinctId` Standing Query pattern is a valid `MultipleValues` Standing Query, though not the other way around.

### Match query

The `MATCH` portion of Standing Queries using the `MultipleValues` mode have mostly the same syntactic requirements as those running in `DistinctId` mode (see the "Match" section of @ref[Writing Standing Queries](writing-standing-queries.md)) with two exceptions:

  1. Multiple values can be returned in the `RETURN` and those values can be anything that is defined in terms of only the IDs and properties (with specific fixed keys) of matched nodes. For example: `RETURN n.age + 3, strId(n) + " " + m.name` is fine but `RETURN properties(n)` is not.

  2. Constraints in the `WHERE` clause can also be anything that is defined in terms of only the IDs and properties of matched nodes, although the constraints from the `DistinctId` mode will still be more efficient (and should be preferred whenever possible).

Since there isn't exactly one ID being returned, the root of the Standing Query pattern (the place in the pattern from which incremental matching starts) is instead set to be the first node in the `MATCH` pattern. This makes it possible to make any node in the pattern the "root".

### Output action

The output is structurally the same as in the `DistinctId` mode. However, it is now possible for the Query data to contain multiple fields, since multiple values can be returned. For example, a `RETURN` clause such as `RETURN n.name AS personName, strId(m) AS friendId` would produce data containing a `personName` and `friendId` field.

Also, unlike `DistinctId` queries, multiple results can be emitted from each "root" node. This means that the "Find people with friends" example, if run in the `MultipleValues` mode, would produce two results (one for each friend) unlike the single result produced in the `DistinctId` mode.
