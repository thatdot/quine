---
description: Quine streaming graph Gremlin language reference
---
# Gremlin Language

@link:[Gremlin](https://tinkerpop.apache.org/gremlin.html){ open=new } is a graph query language, but one that is less declarative than @ref:[Cypher Language](cypher/cypher-language.md) and more focused on letting users specify exactly the traversal they want. The main strength that Gremlin has is that one of its focuses is traversals: instructions for how to walk the graph structure given some starting points.

In the @ref:[Exploration UI](../getting-started/exploration-ui.md), nodes from the graph can be queried using Gremlin or Cypher interchangeably. Quick queries can also be defined in Gremlin. This is effective because those queries usually take only a couple simple steps from their starting point.

@@@ note

Quine supports only a subset of Gremlin, and uses a custom language parser to do so. It is much faster, but less feature-full than the Gremlin Server application provided in the Tinkerpop package. The source code defining what is supported in Quine's use of Gremlin is found in `GremlinParser.scala`. 

The parts of Gremlin that are implemented are not guaranteed to be compliant. Part of the difficulty here is that some parts of Gremlin were designed to be executed form inside a host language, usually Groovy, and don't extend naturally to remote execution (see for instance @link:[this section](https://tinkerpop.apache.org/docs/3.4.7/reference/#-the-lambda-solution-3){ open=new } of the Gremlin manual for some complexities around anonymous functions).

@@@

## Query Start

All supported Gremlin queries begin in one of two ways:

- `g.`  This "g" refers to the "graph" and can take any traversal step following it. This is the primary use case.
- Assignment to a variable followed by a semi-colon, then another supported Gremlin query. E.g.: `x = 1234; g.V(x)`

## Expressions

- `idFrom( [values] )`
- literal value
- list of values: `[`value`,`value`,`…`]`

## Predicates

- `eq`
- `neq`
- `within`
- `regex`

## Traversal Steps

- `v()` or `V()`
- `v(id)` or `V(id)` where `id` is one or more node IDs.
- `recentV`
- `has`
- `hasNot`
- `hasLabel`
- `hasId`
- `eqToVar`
- `out`
- `outLimit`
- `in`
- `inLimit`
- `both`
- `bothLimit`
- `outE`
- `outELimit`
- `inE`
- `inELimit`
- `bothE`
- `bothELimit`
- `outV`
- `inV`
- `bothV`
- `values`
- `valueMap`
- `dedup`
- `as`
- `select`
- `limit`
- `id`
- `strId`
- `unrollPath`
- `count`
- `groupCount`
- `not`
- `where`
- `or`
- `and`
- `is`
- `union`
