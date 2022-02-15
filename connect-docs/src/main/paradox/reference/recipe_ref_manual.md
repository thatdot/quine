# Recipe Reference

A Recipe is a document that contains all the information necessary for Quine to execute any batch or streaming data processing task.

# Running a Recipe

Recipes are interpreted by `quine.jar` using command line arguments. For help on `quine.jar` command line arguments, use the following command:

```
❯ java -jar quine.jar -h
Quine universal program
Usage: quine [options]

  -w, --web-service        disable the quine web service
  -p, --port <value>       web service port (default is 8080)
  -r, --recipe name, file, or URL
                           follow the specified recipe
  -x, --recipe-value key=value
                           recipe parameter substitution
  --force-config           disable recipe configuration defaults
  --no-delete              disable deleting data file when process exits
  -h, --help
  -v, --version            print Quine program version
```

To run a Recipe, use `-r`, followed by the name of the Recipe (from @link[https://quine.io/recipes](https://quine.io/recipes)), or a local file (ending in `.json` or `.yaml`) or a URL. Many Recipes are parameterized and the parameter values must be specified using command line arguments with `--recipe-value`. For example, to run @link:[`ping` from Quine's Recipe repository](https://quine.io/recipes/ping.html):

```
❯ java -jar quine.jar \
  -r ping \
  --recipe-value in_file=input_filename \
  --recipe-value out_file=output_filename
```

The above example assumes the file `input_filename` is available in the local working directory, and that it contains line-separated text data. Backslash (`\`) is used to continue the command on the next line without invoking the command prematurely. `ping` is a reference to the canonical name of the @link:[recipe on quine.io](https://quine.io/recipes).

# Recipe Repository

Please see @link:[Quine's Recipe repository](https://quine.io/recipes) for other available Recipes. Or create your own and contribute it back to the community for others to use.

# Recipe File

A Recipe is represented by a text file containing either JSON or YAML structured data. The file must contain a single object with the following values:

* `version`: Schema versioning; only supported value is 1 (number)
* `canonicalName`: Globally unique immutable identifier for this Recipe (string)
* `title`: Identifies the Recipe but is not necessarily unique or immutable (string)
* `contributor`: The profile URL (github, twitter, etc.) of the person who contributed this recipe (string)
* `summary`: Brief copy about this Recipe (string)
* `description` Longer form copy about this Recipe (string)
* `ingestStreams`: Define how data is read from data sources (array of `IngestStream` API objects¹)
* `standingQueries`: Define how data is transformed and output (array of `StandingQuery` API objects¹)
* `nodeAppearances`: Customize node appearance in web UI (array of `NodeAppearance` API objects¹)
* `quickQueries`: Add queries to node context menus in web UI (array of `QuickQuery` API objects¹)
* `sampleQueries`: Customize sample queries listed in web UI (array of `SampleQuery` API objects¹)
* `printQueries` Cypher queries that are executed and reported to the Recipe user (array of objects; each object defines `cypherQuery` with strings)

¹: For more information on API entities see @ref:[REST API](rest_api.md).

The following is a template Recipe YAML file that can be used to start building a new Recipe:

@@snip [template-recipe.yaml]($connect$/recipes/template-recipe.yaml)

# Additional Command Line Arguments

When Quine is started without command line arguments, its default behavior is to start the web service.
When using command line arguments, the usage mode is assumed to be for Recipes, and the web service is not started by default. To override this behavior, use the following command line arguments:

* `--web-service`: Disables the web service during and after Recipe execution
* `--port`: Optionally specify the TCP of the web service (default is 8080)

Quine is configurable as described in @ref:[Configuration](configuration.md). Normally when running a Recipe, the configuration for `store` is overwritten with `PersistenceAgentType.RocksDb` and is configured to use a temporary file. This configuration is appropriate for most use cases of Recipes and can be changed with the following command line arguments:

* `--force-config`: Quine will use the `store` that is configured via @ref:[Configuration](configuration.md) (instead of overwriting it as described above)
* `--no-delete`: Quine will use `PersistenceAgentType.MapDb` but it will not delete the temporary file

Note `--force-config` and `--no-delete` are mutually exclusive (only one of the two is allowed at a time).

RocksDB is not compatible on some platforms. To use MapDB instead, run with additional parameters as follows:

```
java -Dquine.store.type=map-db -jar quine.jar --force-config
```

# Summary

* A Recipe is a document that contains all the information necessary for Quine to execute any data processing task.
* A Recipe contains several types of entities, including Ingest Streams, Standing Queries, and Cypher queries.
* Recipes are run using `quine.jar` with command line arguments.
* There is a public @link:[repository](https://quine.io/recipes) with many existing Recipes.
