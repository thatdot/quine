package com.thatdot.quine.routes

import endpoints4s.algebra.Tag
import endpoints4s.generic.{docs, title, unnamed}

import com.thatdot.quine.routes.exts.EndpointsWithCustomErrorText
import com.thatdot.quine.routes.exts.NamespaceParameterWrapper.NamespaceParameter

trait AlgorithmRoutes
    extends EndpointsWithCustomErrorText
    with endpoints4s.algebra.JsonEntitiesFromSchemas
    with endpoints4s.generic.JsonSchemas
    with exts.QuineEndpoints {

  private val api = path / "api" / "v1"
  private val algorithmsPrefix = api / "algorithm"

  private[this] val algorithmTag = Tag("Graph Algorithms")
    .withDescription(
      Some(
        "High-level operations on the graph to support graph AI, ML, and other algorithms."
      )
    )

  val walkLength: QueryString[Option[Int]] = qs[Option[Int]](
    "length",
    docs = Some("Maximum length of a walk. Default: `10`")
  )

  /* WARNING: these values duplicate `AlgorithmGraph.defaults.walkPrefix` and `walkSuffix` from the
   * `com.thatdot.quine.graph` package which is not available here.
   * Beware of changes in one place not mirrored to the other!
   */
  private val queryPrefix = "MATCH (thisNode) WHERE id(thisNode) = $n "
  private val querySuffix = "RETURN id(thisNode)"

  val onNodeQuery: QueryString[Option[String]] = qs[Option[String]](
    "query",
    docs = Some(
      s"""Cypher query run on each node of the walk. You can use this query to collect properties instead of node IDs.
         |A `RETURN` statement can return any number of values, separated by `,`s. If returning the same value
         |multiple times, you will need to alias subsequent values with `AS` so that column names are unique. If a list
         |is returned, its content will be flattened out one level and concatenated with the rest of the aggregated
         |values.
         |
         |The provided query will have the following prefix prepended: `$queryPrefix` where `${"$n"}` evaluates
         |to the ID of the node on which the query is executed. The default value of this parameter is:
         |`$querySuffix`""".stripMargin
    )
  )

  val numberOfWalks: QueryString[Option[Int]] = qs[Option[Int]](
    "count",
    docs = Some("An optional integer for how many random walks from each node to generate. Default: `5`")
  )

  val returnParameter: QueryString[Option[Double]] = qs[Option[Double]](
    "return",
    docs = Some(
      "the `p` parameter to determine likelihood of returning to the node just visited: `1/p`  Lower is " +
      "more likely; but if `0`, never return to previous node. Default: `1`"
    )
  )

  val inOutParameter: QueryString[Option[Double]] = qs[Option[Double]](
    "in-out",
    docs = Some(
      "the `q` parameter to determine likelihood of visiting a node outside the neighborhood of the" +
      " starting node: `1/q`  Lower is more likely; but if `0`, never visit the neighborhood. Default: `1`"
    )
  )

  val randomSeedOpt: QueryString[Option[String]] = qs[Option[String]](
    name = "seed",
    docs = Some(
      "Optionally specify any string as a random seed for generating walks. This is used to determine all " +
      "randomness, so providing the same seed will always produce the same random walk. If unset, a new seed is " +
      "used each time a random choice is needed."
    )
  )

  @unnamed
  @title("Save Location")
  sealed trait SaveLocation
  @unnamed
  @title("Local File")
  case class LocalFile(
    @docs("Optional name of the file to save in the working directory") fileName: Option[String]
  ) extends SaveLocation

  @unnamed
  @title("S3 Bucket")
  case class S3Bucket(
    @docs("S3 bucket name") bucketName: String,
    @docs("Optional name of the file in the S3 bucket") key: Option[String]
  ) extends SaveLocation

  implicit lazy val localFileSchema: Record[LocalFile] = genericRecord[LocalFile]
  implicit lazy val s3BucketSchema: Record[S3Bucket] = genericRecord[S3Bucket]
  implicit lazy val saveLocationSchema: Tagged[SaveLocation] = genericTagged[SaveLocation]

  final val algorithmSaveRandomWalks: Endpoint[
    (
      Option[Int],
      Option[Int],
      Option[String],
      Option[Double],
      Option[Double],
      Option[String],
      NamespaceParameter,
      AtTime,
      Int,
      SaveLocation
    ),
    Either[ClientErrors, String]
  ] =
    endpoint(
      request = put(
        url = algorithmsPrefix / "walk" /?
          (walkLength & numberOfWalks & onNodeQuery & returnParameter &
          inOutParameter & randomSeedOpt & namespace & atTime & parallelism),
        entity = jsonRequestWithExample[SaveLocation](example = S3Bucket("your-s3-bucket-name", None))
      ),
      response = customBadRequest("Invalid file")
        .orElse(accepted(textResponse)),
      docs = EndpointDocs()
        .withSummary(Some("Save Random Walks"))
        .withDescription(
          Some(
            """Generate random walks from all nodes in the graph (optionally: at a specific historical time), and save
              |the results.
              |
              |The output file is a CSV where each row is one random walk. The first column will always
              |be the node ID where the walk originated. Each subsequent column will be either:
              |
              |a.) by default, the ID of each node encountered (including the starting node ID again in the second
              |column), or
              |
              |b.) optionally, the results of Cypher query executed from each node encountered on the walk; where
              |multiple columns and rows returned from this query will be concatenated together sequentially into
              |the aggregated walk results.
              |
              |**The resulting CSV may have rows of varying length.**
              |
              |The name of the output file is derived from the arguments used to generate it; or a custom file name can
              |be specified in the API request body. If no custom name is specified, the following values are
              |concatenated to produce the final file name:
              |
              | - the constant prefix: `graph-walk-`
              | - the timestamp provided in `at-time` or else the current time when run. A trailing `_T` is appended if no timestamp was specified.
              | - the `length` parameter followed by the constant `x`
              | - the `count` parameter
              | - the constant `-q` follow by the number of characters in the supplied `query` (`0` if not specified)
              | - the `return` parameter followed by the constant `x`
              | - the `in-out` parameter
              | - the `seed` parameter or `_` if none was supplied
              | - the constant suffix `.csv`
              |
              | Example file name: `graph-walk-1675122348011_T-10x5-q0-1.0x1.0-_.csv`
              |
              | The name of the actual file being written is returned in the API response body.""".stripMargin
          )
        )
        .withTags(List(algorithmTag))
    )

  final val algorithmRandomWalk: Endpoint[
    (Id, (Option[Int], Option[String], Option[Double], Option[Double], Option[String], AtTime, NamespaceParameter)),
    Either[ClientErrors, List[String]]
  ] =
    endpoint(
      request = get(
        algorithmsPrefix / "walk" / nodeIdSegment /?
        (walkLength & onNodeQuery & returnParameter & inOutParameter & randomSeedOpt & atTime & namespace)
      ),
      response = badRequest().orElse(ok(jsonResponse[List[String]])),
      docs = EndpointDocs()
        .withSummary(Some("Generate Random Walk"))
        .withDescription(
          Some(
            "Generate a random walk from a node in the graph and return the results."
          )
        )
        .withTags(List(algorithmTag))
    )
}
