# Time Reification

`reify.time` is a Cypher procedure included with Quine. Its purpose is to facilitate the instantiation (reification) of a graph of nodes representing time. `reify.time` is provided with a timestamp (current wall clock time as the default), and it returns the nodes that represent the passed-in time. `reify.time` does this by determining which nodes must exist, and either reading them from the graph or creating them as necessary. Additionally, `reify.time` relates the nodes it makes to each other and other nodes previously created by this function.

`reify.time` does not do anything Cypher can't do. In this sense, reify.time is unnecessary. So why does it need to exist?

* To reduce the boilerplate necessary to ingest time-series data usefully
* To create a modification point where in the future, changes can be made to how time-series data is modeled in the graph and have this change applied to all usages
* To organize data to be more useful in the Quine web UI
* To settle on a unified convention for representing time so that users don’t have to spend brainpower to create something bespoke (and inconsistent among different users)
* To create the persistent graph structure upon which time-series aggregate values can be stored or related

`reify.time` builds a hierarchy of related nodes for a single datetime value. Each node in this hierarchy represents a different period where the input datetime value belongs. 

* Each node in the hierarchy is defined by a start datetime value and a period. 
* Each node in the hierarchy is related to its parent node (except the largest). 
* Each node in the hierarchy is also related to the next node in time (and the same period).

## Supported Parameters

* ZonedDateTime (optional; defaults to now)
* Periods (optional list of strings; defaults to all periods)

Periods are:

* year
* month
* day
* hour
* minute
* second

## Return Values

`reify.time` returns each of the time nodes reified by this function directly related to the input time. Note that this function creates time nodes that do not exist and reuses time nodes that already exist.

## Examples

Call `reify.time` with default arguments, which will be to reify time nodes at all periods for the current system clock time:

```cypher
CALL reify.time() YIELD node AS n RETURN n
```

Run with a time parsed from a string:

```cypher
CALL reify.time(datetime("2022-04-11T11:06:12-0700"), ["month", "day"]) YIELD node AS n RETURN n
```

Use within a Recipe:

@@snip [template-recipe.yaml]($quine$/recipes/wikipedia.yaml)

The above Recipe consumes an event stream that describes new Wikipedia pages. Each event includes a timestamp, which is passed to `reify.time`.
