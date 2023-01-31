---
description: Generate random walks from nodes in the graph
---

# Random Walk

Random walks are often the central connection between graph-structured data and machine learning applications. A random walk starts at a graph node and follows one of its edges randomly to reach another node, then follows one of that node's edges randomly to reach another node, and so on. Random walks allow us to translate the possibly-infinite dimensions of graph data into a linear string we can feed to graph neural networks.

## Tuning the Walk Parameters

The random walk algorithms in Quine allow a user to tune the random walks as described in the [Node2Vec paper](https://arxiv.org/abs/1607.00653). The `return` parameter (sometimes called `p`) determines how likely a walk is to return one step back where it came from (to "backtrack" to the previous node). The `in-out` parameter (sometimes called `q`) determines whether a walk is more likely to explore the local region ("neighborhood") around a node or travel far afield to explore the graph far away. These parameters can tune the walks to learn different features of the graph and address different goals.

See the API documentation for a complete description of API parameters, types, allowed values, and description: [`algorithm/walk`](https://docs.quine.io/reference/rest-api.html#/paths/api-v1-algorithm-walk/put)

## Collecting Values While Walking

The standard approach to random walks returns only the ID of each node visited in the graph. This capability can be extended substantially by Quine's ability to run an arbitrary Cypher query at each point in the random walk. This capability supports more advanced algorithms like Graph Convolutional Networks or [GraphSAGE](https://arxiv.org/abs/1706.02216)

Quine's random walk algorithms include the ability to define an aggregation `query` for each node encountered in a random walk. This can be used to explore the local neighborhood and/or aggregate multiple properties which get automatically collected in to random walk output. This can be used instead of, or in addition to, collecting node IDs. 

The value of the `query` parameter should be a Cypher query fragment which returns the desired data. The simplest example of this is a simple RETURN statement. A RETURN statement can return any number of values, separated by `,`s. If returning the same value multiple times, you will need to alias subsequent values with `AS` so that column names are unique. If a list is returned, its content will be flattened out one level and concatenated with the rest of the aggregated values.

The provided query will have the following prefix prepended: `MATCH (thisNode) WHERE id(thisNode) = $n ` where `$n` evaluates to the ID of the node on which the query is executed. The default value of the `query` parameter is: `RETURN id(thisNode)`


## Point in Time Walks

Most use cases for Quine include continuously running data ingests, which continue to modify the graph. To correctly generate a set of random walks, you need a view of the graph at a specific moment — without the graph changing from under the random walker.

Use Quine's built-in historical query functionality by including a timestamp (milliseconds since the Unix epoch) in the `at-time` parameter in your query request to generate random walks from the graph at any fixed historical moment.

The rest of the graph can keep changing, and the walk algorithm will see a consistent view of the graph.

## Node-Anchored Walks

Generating a random walk from a specific node in Quine can be done either by calling a function in a Cypher query:

```cypher
MATCH (n)
WHERE n = {some_constraint}
  CALL random.walk(n, 10)
  YIELD walk
RETURN walk
```

Or through the [`algorithm/walk/{id}`](https://docs.quine.io/reference/rest-api.html#/paths/api-v1-algorithm-walk-id/get) endpoint once that you know the node's ID.

```bash
curl --request GET --url "http://localhost:8080/api/v1/algorithm/walk/{node_id}"
```

## Full Graph Walks

Node-anchored walks start from one node and return one random walk. But most graph A.I. algorithms require building many walks from every node in the graph. To support this, Quine includes an API that will generate a stream of all random walks into a file for an entire graph—regardless of how large the graph is.

With an API `PUT` to the [`algorithm/walk`](https://docs.quine.io/reference/rest-api.html#/paths/api-v1-algorithm-walk/put) endpoint, you can direct Quine to stream all the random walks from every node in the graph to a file stored locally or in an S3 bucket.

```bash
curl --request PUT --url http://localhost:8080/api/v1/algorithm/walk --header 'Content-Type: application/json' --data '{ "bucketName": "your-s3-bucket-name", "type": "S3Bucket" }'
```

### Graph Walk File Output

The output file is a CSV where each row is one random walk. The first column will always be the node ID where the walk originated. Each subsequent column will be either:

a.) by default, the ID of each node encountered (including the starting node ID again in the second
column), or

b.) optionally, the results of Cypher query executed from each node encountered on the walk; where
 multiple columns and rows returned from this query will be concatenated together sequentially into
 the aggregated walk results.
 
**The resulting CSV may have rows of varying length.**
 
The name of the output file is derived from the arguments used to generate it; or a custom file name can be specified in the API request body. If no custom name is specified, the following values are concatenated to produce the final file name:
 
  - the constant prefix: `graph-walk-`
  - the timestamp provided in `at-time` or else the current time when run. A trailing `_T` is appended if no timestamp was specified.
  - the `length` parameter followed by the constant `x`
  - the `count` parameter
  - the constant `-q` follow by the number of characters in the supplied `query` (`0` if not specified)
  - the `return` parameter followed by the constant `x`
  - the `in-out` parameter
  - the `seed` parameter or `_` if none was supplied
  - the constant suffix `.csv`
 
Example file name: `graph-walk-1675122348011_T-10x5-q0-1.0x1.0-_.csv`
 
The name of the actual file being written is returned in the API response body.

<!-- TODO
>Document local and S3 output data forms.
>Describe the nuances of using the s3 bucket … like authentication, or file path…
-->
