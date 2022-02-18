# Quine

Quine - a Streaming Graph 

Documentation: <https://quine.io>

## Building

In order to build, you'll need to have the following installed:

  * A recent version of the Java Development Kit (8 or newer)
  * The [`sbt` build tool](https://www.scala-sbt.org/download.html)

Then:

```
sbt compile           # compile all projects
sbt test              # compile and run all projects' tests
sbt fixall            # reformat and lint all source files
sbt quine/run         # run Quine
sbt quine/assembly    # build an über jar of Quine
```
