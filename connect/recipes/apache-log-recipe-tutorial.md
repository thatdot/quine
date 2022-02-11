# Hello, Recipe

In this walk-through we build a simple Recipe to analyze Apache web server logs.

First, we create a workspace directory and download Quine.

```
❯ mkdir ApacheLogRecipe
❯ cd ApacheLogRecipe
❯ curl -s https://quine-enterprise-builds.s3.us-west-2.amazonaws.com/releases/connect-0.2.0.jar -o connect.jar
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
❯ touch recipe.json
```

We will build the content of this file iteratively. Start by copying in this boilerplate using your text editor:

```
{
    "recipeDefinitionVersionNumber": 1,
    "canonicalName": "Apache-Log-Analytics",
    "title": "Apache Log Analytics",
    "contributor": "engineering@thatdot.com",
    "summary": "",
    "description": "",
    "ingestStreams": [],
    "standingQueries": [],
    "nodeAppearances": [],
    "quickQueries": [],
    "sampleQueries": [],
    "printQueries": []
}
```

The following are brief descriptions of the Recipe fields:

* `recipeDefinitionVersionNumber` is for schema versioning and the only supported value is "1"
* `canonicalName` is the globally unique identifier for this recipe
* `title` identifies the recipe but is not necessarily unique
* `contributor` is the email address of the person or organization responsible for this recipe
* `summary` is brief copy about this recipe
* `description` is longer form copy about this recipe
* `nodeAppearances`, `quickQueries`, and `sampleQueries` are API entities for customizing the UI (see API reference for more information)
* `ingestStreams` define how data is read from a data source (in this case `apache_log`)
* `standingQueries` define how data is processed and transformed
* `printQueries` define Cypher queries that are executed and reported to the recipe user

At this point we have the following workspace directory:

```
❯ ls -hl
total 483912
-rw-r--r--  1 landon  staff   2.3M Jan 24 14:59 apache_log
-rw-r--r--  1 landon  staff   234M Jan 24 14:59 connect.jar
-rw-r--r--  1 landon  staff   366B Jan 24 15:07 recipe.json
```

Our first change to `recipe.json` is to configure ingestion of the file. To do this we add to `ingestStreams` the following JSON block. This describes an Ingest Stream that reads the Apache log file and writes JSON nodes.

```
{
    "type": "FileIngest",
    "path": "apache_log",
    "format": {
        "type": "CypherLine",
        "query": "WITH *, text.regexFirstMatch($that, '(\\\\S+)\\\\s+\\\\S+\\\\s+(\\\\S+)\\\\s+\\\\[(.+)\\\\]\\\\s+\\\"(.*)\\\\s+(.*)\\\\s+(.*)\\\"\\\\s+([0-9]+)\\\\s+(\\\\S+)\\\\s+\\\"(.*)\\\"\\\\s+\\\"(.*)\\\"') AS r CREATE ({ sourceIp: r[1], user: r[2], time: r[3], verb: r[4], path: r[5], httpVersion: r[6], status: r[7], size: r[8], referrer: r[9], agent: r[10], type: 'log' })"
    }
}
```

The above Cypher query first parses the Apache log line using a regular expression, then creates a node in the graph with the parsed information. The Apache log line is represented here by the `$that` variable. The regular expression is used to break apart the line into its constituent fields, which are assigned to variable `r` as an array of strings. Finally, Cypher `CREATE` is used to create a node in the graph, with properties derived from the regular expression result.

This simple Recipe just ingests data to graph nodes, but it is able to run and we can inspect the results in the web UI. To run this Recipe:

```
❯ java -jar connect.jar -w -r recipe.json
Cluster is still forming...
Cluster is ready!
Application state loaded.
Running recipe Apache Log Analytics
Connect web server available at http://0.0.0.0:8080
Running Ingest Stream INGEST-1
INGEST-1 status is running and ingested 4680
INGEST-1 status is completed and ingested 10000
```

`-w` enables the web service, and `-r` is followed by the file name or URL to the Recipe.

When ingest is complete, you can click the web link and inspect the graph state visually by accessing the web UI at http://0.0.0.0:8080. To terminate Quine, use Control-C.

```
^CConnect is shutting down...
Shutdown complete.
~/tmp/ApacheLogRecipe ❯
```

The Recipe can be updated with a Cypher query to be run continuously. This also generates a URL to this query in the web UI, making it easy to click to inspect results from a terminal window. To get started with this, add the following JSON object to the `printQueries` array in `recipe.json`. This will print 1 graph node, and print a link to the visualization in the web UI.

```
{
    "cypherQuery": "match (v) return v limit 1"
}
```

Now when the Recipe is run, the output includes additional information (a link to the query in the web UI, and the query result):

```
❯ java -jar connect.jar -w -r recipe.json
Cluster is still forming...
Connect web server available at http://0.0.0.0:8080
Cluster is ready!
Application state loaded.
Running Recipe Apache Log Analytics
Running Ingest Stream INGEST-1
PRINT-1 URL is http://0.0.0.0:8080#match%20%28n%29%20return%20n%20limit%201
INGEST-1 status is running and ingested 4056
INGEST-1 status is completed and ingested 10000
PRINT-1 result Vector(Vector(Node(QuineId(10782351D5774D59AF1F865CAEF0D59E),Set(),Map('request -> Str(GET /files/xdotool/docs/html/tab_l.gif HTTP/1.1), 'size -> Str(706), 'agent -> Str(Mozilla/5.0 (X11; Linux x86_64; rv:20.0) Gecko/20100101 Firefox/20.0 Iceweasel/20.0), 'status -> Str(200), 'referrer -> Str(http://www.semicomplete.com/files/xdotool/docs/html/tabs.css), 'time -> Str(20/May/2015:04:05:09 +0000), 'sourceIp -> Str(83.61.80.53), 'user -> Str(-)))))
```

The data is being ingested correctly, so we will continue with additional analysis. We will use a Standing Query to categorize the data by HTTP verb. This information will be derived from the content of the request field. Implement this in your Recipe by copying the following JSON object to the `standingQueries` array in your Recipe:

```
{
    "pattern": {
        "query": "match (l) where l.type = 'log' return distinct id(l) as id",
        "type": "Cypher",
        "mode": "DistinctId"
    },
    "outputs": {
        "verb": {
            "query": "match (l) where id(l) = $that.data.id match (v) where id(v) = idFrom('verb', l.verb) create (l)-[:verb]->(v) set v.type = 'verb' set v.verb = l.verb return null skip 1",
            "type": "CypherQuery"
        }
    }
}
```

The above Standing Query is executed against Apache log lines as they are added to the graph. It updates the graph by adding a relationship from the node that represents the Apache log line to the node that represents the HTTP method. In other words, it creates a relationship from the log node to the verb node.

The final step in building this Recipe definition is to update `printQueries` by adding another JSON object:

```
{
    "cypherQuery": "match (l)-[rel:verb]->(v) where l.type = 'log' and v.type = 'verb' and v.verb = 'GET' return count(rel) as get_count"
}
```

The above Cypher query reports the number of relationships to the node that represents the `GET` HTTP method. In other words, it counts the number edges from the 'GET' verb node. The result of this Cypher query will be reported periodically during Recipe execution.

The Recipe is ready to run:

```
❯ java -jar connect.jar -w -r recipe.json
Cluster is still forming...
Connect web server available at http://0.0.0.0:8080
Cluster is ready!
Application state loaded.
Running recipe Apache Log Analytics
Running Standing Query STANDING-1
2022-01-26 13:23:51,181 WARN [NotFromActor] [quine-cluster-quine.actor.graph-shard-dispatcher-15] com.thatdot.connect.importers.package$ - Could not verify that the provided ingest query is idempotent. If timeouts occur, query execution may be retried and duplicate data may be created.
Running Ingest Stream INGEST-1
PRINT-1 URL is http://0.0.0.0:8080#match%20%28n%29%20return%20n%20limit%201
PRINT-2 URL is http://0.0.0.0:8080#match%20%28l%29%2D%5B%3Averb%5D%2D%3E%28v%29%20where%20l%2Etype%20%3D%20%27log%27%20and%20v%2Etype%20%3D%20%27verb%27%20and%20v%2Everb%20%3D%20%27GET%27%20return%20count%28v%29%20as%20get%5Fcount
STANDING-1 count 480
INGEST-1 status is running and ingested 1351
STANDING-1 count 2968
INGEST-1 status is running and ingested 4478
STANDING-1 count 6711
INGEST-1 status is running and ingested 8596
STANDING-1 count 10000
INGEST-1 status is completed and ingested 10000
PRINT-1 result Vector(Vector(Node(QuineId(B55F379C8AF240ACB0B52DBF881FA4C6),Set(),Map('httpVersion -> Str(HTTP/1.1), 'path -> Str(/blog/tags/puppet?flav=rss20), 'size -> Str(14872), 'agent -> Str(UniversalFeedParser/4.2-pre-314-svn +http://feedparser.org/), 'status -> Str(200), 'verb -> Str(GET), 'referrer -> Str(-), 'time -> Str(17/May/2015:18:05:47 +0000), 'type -> Str(log), 'sourceIp -> Str(46.105.14.53), 'user -> Str(-)))))
PRINT-2 result Vector(Vector(Integer(9951)))
```

We can see from the above output that 9,951 is the number of HTTP GET requests in this file. We can see the rest of the information using this additional `printQuery`:

```
{
    "cypherQuery": "match (v) where v.type = 'verb' return v"
}
```

Run it again; we get:

```
PRINT-3 result Vector(Vector(Node(QuineId(13719E5398BB352F93B8C8E43AE9126F),Set(),Map('type -> Str(verb), 'verb -> Str(OPTIONS)))), Vector(Node(QuineId(FE4AEBEDB6D938EA9E246F8832861A8A),Set(),Map('type -> Str(verb), 'verb -> Str(POST)))), Vector(Node(QuineId(FEEBDFD1BC5D3319B05C4424BD48905B),Set(),Map('type -> Str(verb), 'verb -> Str(HEAD)))), Vector(Node(QuineId(4B5BDCCB3BF632E78E04FEF139704492),Set(),Map('type -> Str(verb), 'verb -> Str(GET)))))
```

Summary:

* A Recipe is a JSON document that defines Ingest Streams, Standing Queries, and Cypher Queries
* Run a Recipe using `java -jar connect.jar -r file_or_url`
* A Standing Query is used to organize information for efficient query
