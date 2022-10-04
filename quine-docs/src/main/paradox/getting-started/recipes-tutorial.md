---
description: Getting Started - Quine Recipes
---

# Quine Recipes

In this article we will cover Quine recipes -- what are they for, their components, and how to use them to quickly iterate through the design-code-test portion of the development lifecycle.

## What is a Quine Recipe

A recipe is a `yaml` file that contains all of the query and exploration UI configuration objects necessary to begin processing streaming event data in Quine. Quine recipe components correspond directly to Quine's REST API elements, though the API accepts JSON rather than YAML. This means patterns and behaviors developed using the API in a `dev` or `test` environment can be applied to the production environment with only slight modifications.

**A recipe allows you to:**

1. Configure ingest streams
2. Configure standing queries
3. Configure the Quine Exploration UI for graph analysis

Recipes are covered in detail inside the @ref:[recipe reference](../reference/recipe-ref-manual.md)

## Differences Between Recipes and the REST API

There are a couple of operational differences that you need to keep in mind when launching Quine along with a recipe.

* **API calls allow naming** - When you create an ingest stream or a standing query using the API, you choose the name for the object in the URL. The corresponding recipe object uses standardized names in the form of `INGEST-#` and `STANDING-#`.

* **Persistent storage** - Starting the Quine application `jar` without providing a configuration file creates a persistent data store in the local directory. That persistor retains the previous graph state and is appended to each time Quine starts. Alternatively, when Quine launches a recipe, a temporary persistent data store is created in the system `tmp` directory. Each subsequent launch of the recipe will replace the data store, and discard the graph from the previous run.

## Recipe Structure

A recipe is stored in a single `YAML` text file. The file must contain a single object with the following attributes:

| Attribute         | Type                              | Description                                                                       |
| ----------------- | --------------------------------- | --------------------------------------------------------------------------------- |
| `version`         | Integer                           | Schema versioning; only supported value is 1                                      |
| `title`           | String                            | Identifies the Recipe                   |
| `contributor`     | String                            | URL to social profile of the person or organization responsible for this Recipe   |
| `summary`         | String                            | Brief information about this Recipe                                                      |
| `description`     | Text Block                        | Long form description about this Recipe                                                  |
| `ingestStreams`   | Array of [IngestStream](https://docs.quine.io/reference/rest-api.html#/schemas/com.thatdot.quine.routes.IngestStreamConfiguration) objects | Define how data is read from data sources, transformed, and loaded into the graph    |
| `standingQueries` | Array of [StandingQuery](https://docs.quine.io/reference/rest-api.html#/schemas/com.thatdot.quine.routes.StandingQueryDefinition) objects  | Define both sub-graph patterns for Quine to match and subsequent output actions                               |
| `nodeAppearances` | Array of [NodeAppearance](https://docs.quine.io/reference/rest-api.html#/schemas/com.thatdot.quine.routes.UiNodeAppearance) objects | Customize node appearance in the exploration UI                                     |
| `quickQueries`    | Array of [QuickQuery](https://docs.quine.io/reference/rest-api.html#/schemas/com.thatdot.quine.routes.UiNodeQuickQuery) objects     | Add queries to node context menus in the exploration UI                             |
| `sampleQueries`   | Array of [SampleQuery](https://docs.quine.io/reference/rest-api.html#/schemas/com.thatdot.quine.routes.SampleQuery) objects    | Customize sample queries listed in the exploration UI                               |
| `statusQuery`     | A [CypherQuery](https://docs.quine.io/reference/rest-api.html#/schemas/com.thatdot.quine.routes.CypherQuery) object            | OPTIONAL Cypher query that is executed and reported to the terminal window during execution |

Use the `yaml` structure below as a starting point when developing your recipes.

```yaml
version: 1
title: Your title here
contributor: Your GitHub profile link here
summary: Single line summary of your recipe
description: |-
  Long format description of your recipe
ingestStreams: [ ]
standingQueries: [ ]
nodeAppearances: [ ]
quickQueries: [ ]
sampleQueries: [ ]
statusQuery: null
```

Now let's build a recipe to reproduce the use case from the @ref:[**Getting Started**](ingest-streams-tutorial.md) scenario. Remember our goal:

>For the sake of this tutorial, assume that you need to separate human-generated events from bot-generated events in the English Wikipedia database and send them to a destination in your data pipeline for additional processing.

## Recipe Metadata

The first few lines of a Quine recipe contain information about who wrote the recipe and what the recipe is intended to do. 

Starting out with the recipe template from above, we can fill in the `version`, `title`, `contributor`, `summary`, and `description` for our recipe.

```yaml
version: 1
title: Wikipedia non-bot page update event stream
contributor: https://github.com/maglietti
summary: Stream page-update events that were not created by bots
description: |-
  This recipe will separate human generated events from bot generated
  events in the english wikipedia database page-update event stream
  and store them for additional processing.
  API Reference: https://stream.wikimedia.org/?doc#/streams/get_v2_stream_mediawiki_revision_create
```

## Ingest Stream

Ok, now we need to transform the ingest stream object that we POSTed to `/api/v1/ingest/{name}` from JSON to YAML. The simplest way to convert the JSON API body to YAML is to use a tool like @link:[YAML ❤ JSON](https://marketplace.visualstudio.com/items?itemName=hilleer.yaml-plus-json) { open=new } in VSCode.  

Here's the JSON version of the ingest stream that we developed earlier in the @ref:[ingest streams](ingest-streams-tutorial.md) getting started tutorial.

```json
{
  "format": {
    "query": "MATCH (revNode),(pageNode),(dbNode),(userNode),(parentNode) WHERE id(revNode) = idFrom('revision', $that.rev_id) AND id(pageNode) = idFrom('page', $that.page_id) AND id(dbNode) = idFrom('db', $that.database) AND id(userNode) = idFrom('id', $that.performer.user_id) AND id(parentNode) = idFrom('revision', $that.rev_parent_id) SET revNode = $that, revNode.bot = $that.performer.user_is_bot, revNode:revision SET parentNode.rev_id = $that.rev_parent_id SET pageNode.id = $that.page_id, pageNode.namespace = $that.page_namespace, pageNode.title = $that.page_title, pageNode.comment = $that.comment, pageNode.is_redirect = $that.page_is_redirect, pageNode:page SET dbNode.database = $that.database, dbNode:db SET userNode = $that.performer, userNode.name = $that.performer.user_text, userNode:user CREATE (revNode)-[:TO]->(pageNode), (pageNode)-[:IN]->(dbNode), (userNode)-[:RESPONSIBLE_FOR]->(revNode), (parentNode)-[:NEXT]->(revNode)",
    "parameter": "that",
    "type": "CypherJson"
  },
  "type": "ServerSentEventsIngest",
  "url": "https://stream.wikimedia.org/v2/stream/mediawiki.revision-create"
}
```

And the converted YAML version, edited for readability and style

```yaml
ingestStreams:
  - type: ServerSentEventsIngest
    url: https://stream.wikimedia.org/v2/stream/mediawiki.revision-create
    format:
      type: CypherJson
      parameter: that
      query: |-
        MATCH (revNode),(pageNode),(dbNode),(userNode),(parentNode)
        WHERE id(revNode) = idFrom('revision', $that.rev_id) 
          AND id(pageNode) = idFrom('page', $that.page_id) 
          AND id(dbNode) = idFrom('db', $that.database)
          AND id(userNode) = idFrom('id', $that.performer.user_id) 
          AND id(parentNode) = idFrom('revision', $that.rev_parent_id)
        
        SET revNode = $that,
            revNode.bot = $that.performer.user_is_bot,
            revNode:revision

        SET parentNode.rev_id = $that.rev_parent_id
        
        SET pageNode.id = $that.page_id, 
            pageNode.namespace = $that.page_namespace, 
            pageNode.title = $that.page_title, 
            pageNode.comment = $that.comment, 
            pageNode.is_redirect = $that.page_is_redirect, 
            pageNode:page 
        
        SET dbNode.database = $that.database, 
            dbNode:db 
        
        SET userNode = $that.performer, 
            userNode.name = $that.performer.user_text, 
            userNode:user 
        
        CREATE (revNode)-[:TO]->(pageNode),
               (pageNode)-[:IN]->(dbNode),
               (userNode)-[:RESPONSIBLE_FOR]->(revNode),
               (parentNode)-[:NEXT]->(revNode)
```

## Standing Query

We can transform the standing query from the @ref:[standing queries](standing-queries-tutorial.md) tutorial the same way that we transformed the ingest stream.

JSON:

```json
{
  "pattern": {
    "query": "MATCH (userNode:user {user_is_bot: false})-[:RESPONSIBLE_FOR]->(revNode:revision {database: 'enwiki'}) RETURN DISTINCT id(revNode) as id",
    "type": "Cypher"
  },
  "outputs": {
    "print-output": {
      "type": "CypherQuery",
      "query": "MATCH (n) WHERE id(n) = $that.data.id RETURN properties(n)",
      "andThen": {
        "type": "PrintToStandardOut"
      }
    }
  }
}
```

YAML:

```yaml
standingQueries:
  - pattern:
      query: |-
        MATCH (userNode:user {user_is_bot: false})-[:RESPONSIBLE_FOR]->(revNode:revision {database: 'enwiki'})
        RETURN DISTINCT id(revNode) as id
      type: Cypher
    outputs:
      print-output:
        type: CypherQuery
        query: |-
          MATCH (n)
          WHERE id(n) = $that.data.id
          RETURN properties(n)
        andThen:
          type: PrintToStandardOut
```

At this point, our recipe will produce exactly the same running configuration that we accomplished with the API calls submitted during the previous sections in this getting started tutorial.

## Running a recipe

Recipes are launched by passing the `YAML` file as an argument to the Quine `jar` file using `-r`.

I saved the recipe elements that we created above into a file named @link:[wikipedia-non-bot-revisions.yaml](https://github.com/thatdot/quine/blob/main/quine/recipes/wikipedia-non-bot-revisions.yaml) { open=new } on my laptop. The recipe file is in the same directory that I have the `quine-x.x.x.jar` file.

Launching Quine and the recipe.

```shell
❯ java -jar quine-1.3.2.jar -r wikipedia-non-bot-revisions.yaml
Graph is ready
Running Recipe Wikipedia non-bot page update event stream
Running Standing Query STANDING-1
Running Ingest Stream INGEST-1
Quine web server available at http://127.0.0.1:8080
```

Notice that Quine announced the recipe title, provided the names it generated for the standing query and ingest stream, then immediately began outputting revision events to the console.

```text
2022-08-30 09:13:58,636 Standing query `print-output` match: {"meta":{"isPositiveMatch":true,"resultId":"9f4650d3-eb01-4a95-9836-fa17e932d430"},"data":{"properties(n)":{"$schema":"/mediawiki/revision/create/1.1.0","comment":"z","database":"enwiki","meta":{"domain":"en.wikipedia.org","dt":"2022-08-30T14:13:57Z","id":"63a721ff-a3d0-4ab8-a9c2-0890642a2696","offset":2843700515,"partition":0,"request_id":"de759ed9-afb4-4c78-812c-70255d2c6b6b","stream":"mediawiki.revision-create","topic":"eqiad.mediawiki.revision-create","uri":"https://en.wikipedia.org/wiki/User:Peter_I._Vardy/sandbox"},"page_id":8188575,"page_is_redirect":false,"page_namespace":2,"page_title":"User:Peter_I._Vardy/sandbox","parsedcomment":"z","performer":{"user_edit_count":205321,"user_groups":["autoreviewer","extendedconfirmed","reviewer","*","user","autoconfirmed"],"user_id":2675188,"user_is_bot":false,"user_registration_dt":"2006-11-06T14:56:58Z","user_text":"Peter I. Vardy"},"rev_content_changed":true,"rev_content_format":"text/x-wiki","rev_content_model":"wikitext","rev_id":1107535873,"rev_len":6319,"rev_minor_edit":false,"rev_parent_id":1107530092,"rev_sha1":"j67dl0l3cxot84gy2qft4rs2jnsb3cz","rev_slots":{"main":{"rev_slot_content_model":"wikitext","rev_slot_origin_rev_id":1107535873,"rev_slot_sha1":"j67dl0l3cxot84gy2qft4rs2jnsb3cz","rev_slot_size":6319}},"rev_timestamp":"2022-08-30T14:13:57Z"}}}
```

Let's pause the ingest stream for now to stop the scrolling in our terminal window. Remember that for recipes, the name of the ingest stream is assigned when the recipe is run.

```shell
curl -X "PUT" "http://127.0.0.1:8080/api/v1/ingest/INGEST-1/pause"
```

Also notice that interleaved with the revision events, Quine displays a running total of events processed by the ingest stream and standing query. These are easier to see once the ingest stream is paused.

```text
 | => STANDING-1 count 50
 | => INGEST-1 status is paused and ingested 715
```

Depending on your application, you may choose to leave the `nodeApperances`, `quickQueries` and `sampleQueries`  arrays empty. When configured, they can improve the readability and ease of analysis of the streaming graph within the exploration UI. We cover the Exploration UI in another tutorial.

## Status Query

Depending on your event stream, including an optional status query in your recipe can provide status updates to the terminal window while a less frequent event is waiting for a standing query match. Consider including a `statusQuery` in your recipe if you need to track more complex metrics about events processed by the ingest stream. Remember that a recipe will output the count of events processed by a standing query by default.

```yaml
statusQuery: 
  cypherQuery: MATCH (n) RETURN distinct labels(n), count(*)
```

Do not include the `statusQuery` attribute in your recipe file if you do not intend to use it.

## Next Steps

Great job making it this far. You should now have the fundamental knowledge to add Quine into an event streaming data pipeline.

Have a question, suggestion, or did you get stuck somewhere? We welcome your feedback! Please drop into @link:[Quine Slack](https://quine-io.slack.com/) { open=new } and let us know. The team is always happy to discuss Quine and answer your questions.
