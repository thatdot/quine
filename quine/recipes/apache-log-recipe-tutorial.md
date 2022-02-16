# Hello, Recipe

In this walk-through we build a simple Recipe to analyze Apache web server logs.

First, we create a workspace directory and download Quine.

```
❯ mkdir ApacheLogRecipe
❯ cd ApacheLogRecipe
❯ curl -s https://quine-enterprise-builds.s3.us-west-2.amazonaws.com/releases/quine-0.2.0.jar -o quine.jar
❯ ls
```

The next step is to download the dataset. For this tutorial we use a publicly available sample dataset:

```
❯ curl -s https://raw.githubusercontent.com/elastic/examples/master/Common%20Data%20Formats/apache_logs/apache_logs -o apache_log
```

The file contains 10,000 Apache log lines. Each line contains space-separated fields that describe a single HTTP request to an Apache web server.

```
❯ wc -l apache_log
10000 apache_log

❯ head -n 1 apache_log
83.149.9.216 - - [17/May/2015:10:05:03 +0000] "GET /presentations/logstash-monitorama-2013/images/kibana-search.png HTTP/1.1" 200 203023 "http://semicomplete.com/presentations/logstash-monitorama-2013/" "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/32.0.1700.77 Safari/537.36"
```

Next, create the Recipe file.

```
❯ touch recipe.yaml
```

We will build the content of this file iteratively. Start by copying in this boilerplate using your text editor:

```yaml
version: 1
canonicalName: Apache-Log-Analytics
title: Apache Log Analytics
contributor: name@example.com
summary: ""
description: ""
ingestStreams: []
standingQueries: []
nodeAppearances: []
quickQueries: []
sampleQueries: []
statusQuery: null
```

The following are brief descriptions of the Recipe fields:

* `version` is for schema versioning and the only supported value is "1"
* `canonicalName` is the globally unique identifier for this recipe
* `title` identifies the recipe but is not necessarily unique
* `contributor` is the email address of the person or organization responsible for this recipe
* `summary` is brief copy about this recipe
* `description` is longer form copy about this recipe
* `nodeAppearances`, `quickQueries`, and `sampleQueries` are API entities for customizing the UI (see API reference for more information)
* `ingestStreams` define how data is read from a data source (in this case `apache_log`)
* `standingQueries` define how data is processed and transformed
* `statusQuery` defines a Cypher query that is executed and reported to the recipe user

At this point we have the following workspace directory:

```
❯ ls -hl
total 483912
-rw-r--r--  1 landon  staff   2.3M Jan 24 14:59 apache_log
-rw-r--r--  1 landon  staff   234M Jan 24 14:59 quine.jar
-rw-r--r--  1 landon  staff   366B Jan 24 15:07 recipe.yaml
```

Our first change to `recipe.yaml` is to configure ingestion of the file. To do this we replace `ingestStreams: []` with the following text block:

```yaml
ingestStreams:
  - type: FileIngest
    path: apache_log
    format:
      type: CypherLine
      query: |-
        WITH *, text.regexFirstMatch($that,
          '(\\S+)\\s+\\S+\\s+(\\S+)\\s+\\[(.+)\\]\\s+\"(.*)\\s+(.*)\\s+(.*)\"\\s+([0-9]+)\\s+(\\S+)\\s+\"(.*)\"\\s+\"(.*)\"'
        ) AS r CREATE ({
          sourceIp: r[1],
          user: r[2],
          time: r[3],
          verb: r[4],
          path: r[5],
          httpVersion: r[6],
          status: r[7],
          size: r[8],
          referrer: r[9],
          agent: r[10],
          type: 'log'
        })
```

Above describes an Ingest Stream that reads the Apache log file and writes graph nodes.

The above Cypher query first parses the Apache log line using a regular expression, then creates a node in the graph with the parsed information. The Apache log line is represented here by the `$that` variable. The regular expression is used to break apart the line into its constituent fields, which are assigned to variable `r` as an array of strings. Finally, Cypher `CREATE` is used to create a node in the graph, with properties derived from the regular expression result.

This simple Recipe just ingests data to graph nodes, but it is able to run and we can inspect the results in the web UI. To run this Recipe:

```
❯ java -jar quine.jar -w -r recipe.yaml
Graph is ready!
Application state loaded.
Running Recipe Apache Log Analytics
Running Ingest Stream INGEST-1
 web server available at http://0.0.0.0:8080
INGEST-1 status is running and ingested 8320
INGEST-1 status is completed and ingested 10000
```

`-w` enables the web service, and `-r` is followed by the file name or URL to the Recipe.

When ingest is complete, you can click the web link and inspect the graph state visually by accessing the web UI at [http://0.0.0.0:8080](http://0.0.0.0:8080). To terminate Quine, use Control-C.

```
^CQuine is shutting down...
Shutdown complete.
```

The Recipe can be updated with a Cypher query to be run continuously. This also generates a URL to this query in the web UI, making it easy to click to inspect results from a terminal window. To get started with this, replace `statusQuery: null` with the following text block. This will print 1 graph node, and print a link to the visualization in the web UI.

```yaml
statusQuery:
  cypherQuery: MATCH (v) RETURN v LIMIT 1
```

Now when the Recipe is run, the output includes additional information (a link to the query in the web UI, and the query result):

```
❯ java -jar quine.jar -w -r recipe.yaml
Graph is ready!
Application state loaded.
Running Recipe Apache Log Analytics
Running Ingest Stream INGEST-1
Status query URL is http://0.0.0.0:8080#MATCH%20%28v%29%20RETURN%20v%20LIMIT%201
Quine web server available at http://0.0.0.0:8080
INGEST-1 status is running and ingested 9151
INGEST-1 status is completed and ingested 10000
Status query result Vector(Vector(Node(QuineId(83AFA4FB3464402AAFCFFBD273B5076C),Set(),Map('httpVersion -> Str(HTTP/1.1), 'path -> Str(/presentations/logstash-1/file/intro-logging-problems/RageFace.png), 'size -> Str(19917), 'agent -> Str(Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/33.0.1750.91 Safari/537.36), 'status -> Str(200), 'verb -> Str(GET), 'referrer -> Str(http://semicomplete.com/presentations/logstash-1/), 'time -> Str(19/May/2015:22:05:55 +0000), 'type -> Str(log), 'sourceIp -> Str(130.237.218.86), 'user -> Str(-)))))
```

The data is being ingested correctly, so we will continue with additional analysis. We will use a Standing Query to categorize the data by HTTP verb. This information will be derived from the content of the request field. Implement this in your Recipe by copying the following text block replacing `standingQueries: []` in your Recipe:

```yaml
standingQueries:
  - outputs:
      verb:
        type: CypherQuery
        query: >-
          MATCH (l) WHERE id(l) = $that.data.id
          MATCH (v) WHERE id(v) = idFrom('verb', l.verb)
          CREATE (l)-[:verb]->(v)
          SET v.type = 'verb'
          SET v.verb = l.verb
          RETURN null
          SKIP 1
    pattern:
      type: Cypher
      query: MATCH (l) WHERE l.type = 'log' RETURN DISTINCT id(l) AS id
      mode: DistinctId
```

The above Standing Query is executed against Apache log lines as they are added to the graph. It updates the graph by adding a relationship from the node that represents the Apache log line to the node that represents the HTTP method. In other words, it creates a relationship from the log node to the verb node.

The final step in building this Recipe definition is to update `statusQuery` by replacing it with the following text block:

```yaml
statusQuery:
  cypherQuery: >-
    MATCH (l)-[rel:verb]->(v)
    WHERE l.type = 'log' AND v.type = 'verb' AND v.verb = 'GET'
    RETURN count(rel) AS get_count
```

The above Cypher query reports the number of relationships to the node that represents the `GET` HTTP method. In other words, it counts the number edges from the 'GET' verb node. The result of this Cypher query will be reported periodically during Recipe execution.

The Recipe is ready to run:

```
❯ java -jar quine.jar -w -r recipe.yaml
Graph is ready!
Application state loaded.
Running Recipe Apache Log Analytics
Running Standing Query STANDING-1
Running Ingest Stream INGEST-1
Status query URL is http://0.0.0.0:8080#MATCH%20%28l%29%2D%5Brel%3Averb%5D%2D%3E%28v%29%20WHERE%20l%2Etype%20%3D%20%27log%27%20AND%20v%2Etype%20%3D%20%27verb%27%20AND%20v%2Everb%20%3D%20%27GET%27%20RETURN%20count%28rel%29%20AS%20get%5Fcount
Quine web server available at http://0.0.0.0:8080
INGEST-1 status is running and ingested 6795
Status query result Vector(Vector(Integer(5650)))
STANDING-1 count 7657
INGEST-1 status is running and ingested 8629
STANDING-1 count 10000
INGEST-1 status is completed and ingested 10000
Status query result Vector(Vector(Integer(9951)))
```

Summary:

* A Recipe is a YAML document that defines Ingest Streams, Standing Queries, and Cypher Queries
* Run a Recipe using `java -jar quine.jar -r file_or_url`
* A Standing Query is used to organize information for efficient query
