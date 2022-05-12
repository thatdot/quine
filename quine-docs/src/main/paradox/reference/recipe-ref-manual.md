# Recipe Reference

A Recipe is a document that contains all the information necessary for Quine to execute any batch or streaming data processing task.

# Running a Recipe

Recipes are interpreted by `quine.jar` using command line arguments. For help on `quine.jar` command line arguments, use the following command:

```
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

To run a Recipe, use `-r`, followed by the name of the Recipe (from @link[https://quine.io/recipes](https://quine.io/recipes)), or a local file (ending in `.json` or `.yaml`), or a URL. Many Recipes are parameterized and the parameter values must be specified using command line arguments with `--recipe-value`. For example, to run @link:[`ping` from Quine's Recipe repository](https://quine.io/recipes/ping.html):

```
❯ java -jar quine.jar \
  -r ping \
  --recipe-value in-file=input-filename \
  --recipe-value out-file=output-filename
```

The above example assumes the file `input-filename` is available in the local working directory, and that it contains line-separated text data. Backslash (`\`) is used to continue the command on the next line without invoking the command prematurely. `ping` is a reference to the canonical name of the @link:[recipe on quine.io](https://quine.io/recipes).

# Recipe Repository

Please see @link:[Quine's Recipe repository](https://quine.io/recipes) for other available Recipes. Or create your own and contribute it back to the community for others to use.

# Recipe File

A Recipe is represented by a text file containing either JSON or YAML structured data. The file must contain a single object with the following values:

* `version`: Schema versioning; only supported value is 1 (number)
* `title`: Identifies the Recipe but is not necessarily unique or immutable (string)
* `contributor` URL to social profile of the person or organization responsible for this Recipe
* `summary`: Brief copy about this Recipe (string)
* `description` Longer form copy about this Recipe (string)
* `ingestStreams`: Define how data is read from data sources (array of `IngestStream` API objects¹)
* `standingQueries`: Define how data is transformed and output (array of `StandingQuery` API objects¹)
* `nodeAppearances`: Customize node appearance in web UI (array of `NodeAppearance` API objects¹)
* `quickQueries`: Add queries to node context menus in web UI (array of `QuickQuery` API objects¹)
* `sampleQueries`: Customize sample queries listed in web UI (array of `SampleQuery` API objects¹)
* `statusQuery` Cypher query that is executed and reported to the Recipe user (an object that defines `cypherQuery`)

¹: For more information on API entities see @ref:[REST API](rest-api.md).

The following is a template Recipe YAML file that can be used to start building a new Recipe:

@@snip [template-recipe.yaml]($quine$/recipes/template-recipe.yaml)

# Recipe Parameters

A Recipe may contain parameters. To use a parameter in a Recipe file, a value in a Recipe must start with the `$` character. The following example demonstrates a Recipe with a parameter called `in-file`:

@@snip [template-recipe.yaml]($quine$/recipes/ingest.yaml)

Running the above example (without specifying the parameter value) causes an error:

```
❯ java -jar quine.jar -r ingest
Missing required parameter in-file; use --recipe-value in-file=
```

The error message indicates the command must be run with an additional command line argument that specifies the required parameter:

```
❯ java -jar quine.jar -r ingest --recipe-value in-file=my-file.txt
```

The parameter value is substituted into the Recipe at runtime. Common examples for Recipe parameters include file names, URLs, and host names.

# Additional Command Line Arguments

When Quine is started without command line arguments, its default behavior is to start the web service on port 8080. The following options are available to change this behavior:

* `-W, --disable-web-service`: Disables the web service
* `-p, --port`: Specify the TCP port of the web service

Quine is configurable as described in @ref:[Configuration](configuration.md). Normally when running a Recipe, the configuration for `store` is overwritten with `PersistenceAgentType.RocksDb` and is configured to use a temporary file. This configuration is appropriate for most use cases of Recipes and can be changed with the following command line arguments:

* `--force-config`: Quine will use the `store` that is configured via @ref:[Configuration](configuration.md) (instead of overwriting it as described above)
* `--no-delete`: Quine will not delete the DB file on exit and will print out that path to it

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
