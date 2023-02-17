---
description: Quine streaming graph recipe definition reference
---
# Recipe Reference

## What is a Quine Recipe

For Quine, a recipe is a document that contains all of the information necessary for Quine to execute any batch or streaming data process. Quine recipes are written in `yaml` and built from components including:

* @ref:[**Ingest Streams**](../components/ingest-sources/ingest-sources.md) to read streaming data from sources and update graph data
* @ref:[**Standing Queries**](../components/writing-standing-queries.md) to transform graph data, and to produce aggregates and other outputs
* @ref:[**Cypher expressions**](../reference/cypher/cypher-language.md) to implement graph operations such as querying and updating data
* @ref:[**Exploration UI**](../getting-started/exploration-ui.md) configuration to customize the web user interface for the use-case that is the subject of the recipe

> You can learn how to write a recipe in the @ref:[recipes tutorial](../getting-started/recipes-tutorial.md) section in the @ref:[getting started guide](../getting-started/index.md).

## When to use a Recipe

Recipes enable you to quickly iterate in Quine and to share what you've built with others so they can reproduce, explore and expand upon a solution.

**Consider writing a recipe:**

* When you are actively developing an event streaming solution
* To preserve a solution whenever a development milestone is achieved
* To configure the visual aspects of the Quine Exploration UI
* To store `quick queries` and `sample queries` that aide in graph analysis
* To share your solution with collaborators or the open source community
* When interacting with Quine support

## Recipe Structure

A recipe is stored in a single `YAML` text file. The file contains a single object with the following attributes:

```yaml
version: 1
title: recipe Title
contributor: https://github.com/example-user
summary: ""
description: ""
ingestStreams: []
standingQueries: []
nodeAppearances: []
quickQueries: []
sampleQueries: []
statusQuery: null
```

Each configuration object is defined by its corresponding API entity in the @ref:[REST API](rest-api.md). Follow the links in the table below for details regarding each attribute.

| Attribute       | Type                              | Description                                                                                |
| --------------- | --------------------------------- | ------------------------------------------------------------------------------------------ |
| `version`         | Integer                           | The recipe schema version, right now the only supported value is 1                                               |
| `title`           | String                            | Identifies the recipe                                                                      |
| `contributor`     | String                            | URL to social profile of the person or organization responsible for this recipe            |
| `summary`         | String                            | Brief information about this recipe                                                        |
| `description`     | Text Block                        | Long form description about this recipe                                                    |
| `ingestStreams`   | Array of [IngestStream](https://docs.quine.io/reference/rest-api.html#/schemas/com.thatdot.quine.routes.IngestStreamConfiguration) objects   | Define how data is read from data sources, transformed, and loaded into the graph           |
| `standingQueries` | Array of [StandingQuery](https://docs.quine.io/reference/rest-api.html#/schemas/com.thatdot.quine.routes.StandingQueryDefinition) objects  | Define both sub-graph patterns for Quine to match and subsequent output actions             |
| `nodeAppearances` | Array of [NodeAppearance](https://docs.quine.io/reference/rest-api.html#/schemas/com.thatdot.quine.routes.UiNodeAppearance) objects | Customize node appearance in the exploration UI                                             |
| `quickQueries`    | Array of [QuickQuery](https://docs.quine.io/reference/rest-api.html#/schemas/com.thatdot.quine.routes.UiNodeQuickQuery) objects     | Add queries to node context menus in the exploration UI                                     |
| `sampleQueries`   | Array of [SampleQuery](https://docs.quine.io/reference/rest-api.html#/schemas/com.thatdot.quine.routes.SampleQuery) objects    | Customize sample queries listed in the exploration UI                                       |
| `statusQuery`     | A [CypherQuery](https://docs.quine.io/reference/rest-api.html#/schemas/com.thatdot.quine.routes.CypherQuery) object            | OPTIONAL Cypher query that is executed and reported to the terminal window during execution |

## Differences Between Recipes and the REST API

Recipes package together Quine config, graph logic/structure, and exploration UI customizations that are run automatically when Quine starts. A recipe file can define the graph and exploration UI configuration directly however, when a recipe is run, it infers a Quine configuration. There are a couple of operational differences that you need to keep in mind when launching Quine along with a recipe.

* **API calls allow naming** - When you create an ingest stream or a standing query using the API, you choose the name for the object in the URL. The corresponding recipe object uses standardized names in the form of `INGEST-#` and `STANDING-#`.

* **Persistent storage** - Starting the Quine application `jar` *without* providing a configuration file, Quine creates a RocksDB-based persistent data store in the local directory. That persistor retains the previous graph state and is appended to each time Quine starts. Alternatively, when Quine launches a recipe, a temporary persistent data store is created in the system `tmp` directory. Each subsequent launch of the recipe will replace the data store, discarding the graph from the previous run.

## Running a Recipe

recipes are interpreted by `quine.jar` using command line arguments. For help on `quine.jar` command line arguments, use the following command:

```shell
❯ java -jar quine.jar -h
Quine universal program
Usage: quine [options]

  -W, --disable-web-service
                           disable Quine web service
  -p, --port <value>       web service port (default is 8080)
  -r, --recipe name, file, URL
                           follow the specified recipe
  -x, --recipe-value key=value
                           recipe parameter substitution
  --force-config           disable recipe configuration defaults
  --no-delete              disable deleting data file when process exits
  -h, --help
  -v, --version            print Quine program version
```

To run a recipe, use `-r`, followed by the name of the recipe (from @link[https://quine.io/recipes](https://quine.io/recipes)), or a local file (ending in `.yaml`), or a URL. recipes can expect input parameters and the parameter values must be specified using command line arguments with `--recipe-value`.

## Recipe Parameters

A recipe may contain parameters that are used to pass information to the recipe. To use a parameter in a recipe file, a value in a recipe must start with the `$` character. The following example demonstrates a recipe with a parameter called `in-file`:

@@snip [template-recipe.yaml]($quine$/recipes/ingest.yaml)

Running the above example (without specifying the parameter value) causes an error:

```shell
❯ java -jar quine.jar -r ingest
Missing required parameter in-file; use --recipe-value in-file=
```

The error message indicates the command must be run with an additional command line argument that specifies the required parameter:

```shell
❯ java -jar quine.jar -r ingest --recipe-value in-file=my-file.txt
```

The parameter value is substituted into the recipe at runtime. Common examples for recipe parameters include file names, URLs, and host names.

## Additional Command Line Arguments

When Quine is started without command line arguments, its default behavior is to start the web service on port 8080. The following options are available to change this behavior:

* `-W, --disable-web-service`: Disables the web service
* `-p, --port`: Specify the TCP port of the web service

Quine is configurable as described in @ref:[Configuration](configuration.md). Normally when running a recipe, the configuration for `store` is overwritten with `PersistenceAgentType.RocksDb` and is configured to use a temporary file. This configuration is appropriate for most use cases of recipes and can be changed with the following command line arguments:

* `--force-config`: Quine will use the `store` that is configured via @ref:[Configuration](configuration.md) (instead of overwriting it as described above)
* `--no-delete`: Quine will not delete the DB file on exit and will print out that path to it

> **Note**: The `--force-config` and `--no-delete` options are mutually exclusive (only one of the two is allowed at a time).

RocksDB may not function on some platforms and you need to use MapDB instead by starting Quine using parameters as follows:

```shell
java -Dquine.store.type=map-db -jar quine.jar --force-config
```

## Recipe Repository

Complete recipes are useful as reference applications for use cases. @link:[Quine's recipe repository](https://quine.io/recipes) highlights recipes shared by our community members. Additionally, you can view all of the shared recipes directory in the @link:[Quine Github repository.](https://github.com/thatdot/quine/tree/main/quine/recipes)

## Contribute

If you make a recipe and think others might find it useful, please consider contributing it back to the repository on Github so that others can use it. They could use it on their own data, or even just use it as a starting point for customization and remixing for other goals. To share with the community, @link:[open a pull request on Github](https://github.com/thatdot/quine/pulls).
