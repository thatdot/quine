package com.thatdot.quine.app.v2api.endpoints

import java.nio.file.{Files, Paths}

import scala.concurrent.Future

import org.apache.pekko.stream.IOResult
import org.apache.pekko.stream.connectors.s3.MultipartUploadResult
import org.apache.pekko.stream.connectors.s3.scaladsl.S3
import org.apache.pekko.stream.scaladsl.{FileIO, Sink}
import org.apache.pekko.util.ByteString

import io.circe.generic.extras.auto._
import sttp.model.StatusCode
import sttp.tapir.Schema.annotations.{description, title}
import sttp.tapir._
import sttp.tapir.generic.auto.schemaForCaseClass
import sttp.tapir.server.ServerEndpoint
import sttp.tapir.server.ServerEndpoint.Full

import com.thatdot.api.v2.ErrorResponseHelpers.{badRequestError, serverError}
import com.thatdot.api.v2.schema.V2ApiConfiguration
import com.thatdot.api.v2.{ErrorResponse, SuccessEnvelope, V2EndpointDefinitions}
import com.thatdot.common.quineid.QuineId
import com.thatdot.quine.app.util.StringOps
import com.thatdot.quine.app.v2api.definitions.{
  AlgorithmApiMethods,
  ApplicationApiMethods,
  CommonParameters,
  ParallelismParameter,
}
import com.thatdot.quine.graph.{AlgorithmGraph, BaseGraph, CypherOpsGraph, LiteralOpsGraph}

object V2AlgorithmEndpointEntities extends V2ApiConfiguration with StringOps {
  /* WARNING: these values duplicate `AlgorithmGraph.defaults.walkPrefix` and `walkSuffix` from the
   * `com.thatdot.quine.graph` package which is not available here.
   * Beware of changes in one place not mirrored to the other!
   */
  val queryPrefix = "MATCH (thisNode) WHERE id(thisNode) = $n "
  val querySuffix = "RETURN id(thisNode)"

  implicit val positiveIntCodec: Codec[String, Int, CodecFormat.TextPlain] = Codec.int.validate(Validator.positive)

  /** SQ Output Name path element */
  val walkLengthQs: EndpointInput.Query[Option[Int]] =
    query[Option[Int]]("length").description("Maximum length of a walk.").default(Some(10))

  val onNodeQueryQs: EndpointInput.Query[Option[String]] = query[Option[String]]("query")
    .description(
      s"""Cypher query run on each node of the walk. You can use this query to collect properties instead of node IDs.
         |A `RETURN` statement can return any number of values, separated by `,`s. If returning the same value
         |multiple times, you will need to alias subsequent values with `AS` so that column names are unique. If a list
         |is returned, its content will be flattened out one level and concatenated with the rest of the aggregated
         |values.""".asOneLine + "\n\n" +
      s"""The provided query will have the following prefix prepended: `$queryPrefix` where `${"$n"}` evaluates
         |to the ID of the node on which the query is executed. The default value of this parameter is:
         |`$querySuffix`""".asOneLine,
    )
  val numberOfWalksQs: EndpointInput.Query[Option[Int]] = query[Option[Int]]("count")
    .description("An optional integer for how many random walks from each node to generate.")
    .default(Some(5))

  val returnQs: EndpointInput.Query[Option[Double]] = query[Option[Double]]("return")
    .description(
      """The `p` parameter to determine likelihood of returning to the node just visited: `1/p`.
        |Lower is more likely; but if `0`, never return to previous node.""".asOneLine,
    )
    .default(Some(1))

  val inOutQs: EndpointInput.Query[Option[Double]] = query[Option[Double]]("in-out")
    .description(
      """The `q` parameter to determine likelihood of visiting a node outside the neighborhood of the starting node: `1/q`.
        |Lower is more likely; but if `0`, never visit the neighborhood.""".asOneLine,
    )
    .default(Some(1))

  val randomSeedOptQs: EndpointInput.Query[Option[String]] = query[Option[String]]("seed")
    .description(
      """Optionally specify any string as a random seed for generating walks.
        |This is used to determine all randomness, so providing the same seed will always produce the same random walk.
        |If unset, a new seed is used each time a random choice is needed.""".asOneLine,
    )

  @title("Save Location")
  sealed trait TSaveLocation {
    def fileName(defaultFileName: String): String

    def toSink(filename: String): Sink[ByteString, Future[Object]]
  }

  @title("Local File")
  case class LocalFile(
    @description("Optional name of the file to save in the working directory.") fileName: Option[String],
  ) extends TSaveLocation {

    def fileName(defaultFileName: String): String = fileName match {
      case Some(name) if name.nonEmpty => name
      case _ => defaultFileName
    }

    def toSink(fname: String): Sink[ByteString, Future[IOResult]] = {
      val p = Paths.get(fname)
      Files.createFile(p) // Deliberately cause an error if it is not accessible
      FileIO.toPath(p)
    }
  }

  @title("S3 Bucket")
  case class S3Bucket(
    @description("S3 bucket name.") bucketName: String,
    @description("Optional name of the file in the S3 bucket.") key: Option[String],
  ) extends TSaveLocation {
    def fileName(defaultFileName: String): String = key.getOrElse(defaultFileName)

    def toSink(fname: String): Sink[ByteString, Future[MultipartUploadResult]] = S3.multipartUpload(bucketName, fname)

  }

}

trait V2AlgorithmEndpoints
    extends V2EndpointDefinitions
    with V2IngestApiSchemas
    with CommonParameters
    with ParallelismParameter {
  val appMethods: AlgorithmApiMethods with ApplicationApiMethods {
    val graph: BaseGraph with LiteralOpsGraph with CypherOpsGraph with AlgorithmGraph
  }

  import V2AlgorithmEndpointEntities._

  private val algorithmBase: EndpointBase = rawEndpoint("algorithm")
    .tag("Graph Algorithms")
    .description("High-level operations on the graph to support graph AI, ML, and other algorithms.")
    .errorOut(serverError())

  protected[endpoints] val saveRandomWalks: Endpoint[
    Unit,
    (
      Option[Int],
      Option[Int],
      Option[String],
      Option[Double],
      Option[Double],
      Option[String],
      Option[String],
      Option[AtTime],
      Int,
      TSaveLocation,
    ),
    Either[ErrorResponse.ServerError, ErrorResponse.BadRequest],
    SuccessEnvelope.Accepted,
    Any,
  ] = algorithmBase
    .name("Save Random Walks")
    .description(
      """Generate random walks from all nodes in the graph (optionally: at a specific historical time),
        |and save the results.""".asOneLine + "\n\n" +
      """The output file is a CSV where each row is one random walk.
        |The first column will always be the node ID where the walk originated.
        |Each subsequent column will be either:""".asOneLine + "\n\n" +
      """a.) by default, the ID of each node encountered
        |(including the starting node ID again in the second column), or""".asOneLine + "\n\n" +
      """b.) optionally, the results of Cypher query executed from each node encountered on the walk;
        |where multiple columns and rows returned from this query will be concatenated together
        |sequentially into the aggregated walk results.""".asOneLine + "\n\n" +
      "**The resulting CSV may have rows of varying length.**\n\n" +
      """The name of the output file is derived from the arguments used to generate it;
        |or a custom file name can be specified in the API request body. If no custom name is specified,
        |the following values are concatenated to produce the final file name:""".asOneLine + "\n\n" +
      """ - the constant prefix: `graph-walk-`
        | - the timestamp provided in `at-time` or else the current time when run. A trailing `_T` is appended if no timestamp was specified.
        | - the `length` parameter followed by the constant `x`
        | - the `count` parameter
        | - the constant `-q` follow by the number of characters in the supplied `query` (`0` if not specified)
        | - the `return` parameter followed by the constant `x`
        | - the `in-out` parameter
        | - the `seed` parameter or `_` if none was supplied
        | - the constant suffix `.csv`
        |
        |Example file name: `graph-walk-1675122348011_T-10x5-q0-1.0x1.0-_.csv`
        |
        |The name of the actual file being written is returned in the API response body.""".stripMargin,
    )
    .in("save-walk")
    .in(walkLengthQs)
    .in(numberOfWalksQs)
    .in(onNodeQueryQs)
    .in(returnQs)
    .in(inOutQs)
    .in(randomSeedOptQs)
    .in(namespaceParameter)
    .in(atTimeParameter)
    .in(parallelismParameter)
    .in(jsonOrYamlBody[TSaveLocation](Some(S3Bucket("your-s3-bucket-name", None))))
    .post
    .errorOutEither(badRequestError("Invalid Query", "Invalid Argument", "Invalid file name"))
    .out(statusCode(StatusCode.Accepted))
    .out(jsonBody[SuccessEnvelope.Accepted])

  protected[endpoints] val saveRandomWalksLogic: (
    (
      Option[Int],
      Option[Int],
      Option[String],
      Option[Double],
      Option[Double],
      Option[String],
      Option[String],
      Option[AtTime],
      Int,
      TSaveLocation,
    ),
  ) => Future[Either[Either[ErrorResponse.ServerError, ErrorResponse.BadRequest], SuccessEnvelope.Accepted]] = {
    case (
          walkLengthOpt,
          numWalksOpt,
          queryOpt,
          returnOpt,
          inOutOpt,
          randomSeedOpt,
          namespace,
          atTimeOpt,
          parallelism,
          saveLocation,
        ) =>
      recoverServerErrorEitherWithServerError(
        Future.successful(
          appMethods
            .algorithmSaveRandomWalks(
              walkLengthOpt,
              numWalksOpt,
              queryOpt,
              returnOpt,
              inOutOpt,
              randomSeedOpt,
              namespaceFromParam(namespace),
              atTimeOpt,
              parallelism,
              saveLocation,
            ),
        ),
      )(_ => SuccessEnvelope.Accepted())
  }

  private def saveRandomWalksServerEndpoint: Full[
    Unit,
    Unit,
    (
      Option[Int],
      Option[Int],
      Option[String],
      Option[Double],
      Option[Double],
      Option[String],
      Option[String],
      Option[AtTime],
      Int,
      TSaveLocation,
    ),
    Either[ErrorResponse.ServerError, ErrorResponse.BadRequest],
    SuccessEnvelope.Accepted,
    Any,
    Future,
  ] = saveRandomWalks.serverLogic[Future](saveRandomWalksLogic)

  protected[endpoints] val generateRandomWalk: Endpoint[
    Unit,
    (
      QuineId,
      Option[Int],
      Option[String],
      Option[Double],
      Option[Double],
      Option[String],
      Option[String],
      Option[AtTime],
    ),
    Either[ErrorResponse.ServerError, ErrorResponse.BadRequest],
    SuccessEnvelope.Ok[List[String]],
    Any,
  ] = algorithmBase
    .name("Generate Random Walk")
    .description("Generate a random walk from a node in the graph and return the results.")
    .post
    .in("nodes")
    .in(path[QuineId]("id").description("Node id"))
    .in("walk")
    .in(walkLengthQs)
    .in(onNodeQueryQs)
    .in(returnQs)
    .in(inOutQs)
    .in(randomSeedOptQs)
    .in(namespaceParameter)
    .in(atTimeParameter)
    .errorOutEither(badRequestError("Invalid Query", "Invalid Argument"))
    .out(statusCode(StatusCode.Ok))
    .out(jsonBody[SuccessEnvelope.Ok[List[String]]])

  protected[endpoints] val generateRandomWalkLogic: (
    (
      QuineId,
      Option[Int],
      Option[String],
      Option[Double],
      Option[Double],
      Option[String],
      Option[String],
      Option[AtTime],
    ),
  ) => Future[Either[Either[ErrorResponse.ServerError, ErrorResponse.BadRequest], SuccessEnvelope.Ok[List[String]]]] = {
    case (id, walkLengthOpt, queryOpt, returnOpt, inOutOpt, randomSeedOpt, namespace, atTimeOpt) =>
      recoverServerErrorEitherWithServerError(
        appMethods
          .algorithmRandomWalk(
            id,
            walkLengthOpt,
            queryOpt,
            returnOpt,
            inOutOpt,
            randomSeedOpt,
            namespaceFromParam(namespace),
            atTimeOpt,
          ),
      )(SuccessEnvelope.Ok(_))
  }

  private def generateRandomWalkServerEndpoint: Full[
    Unit,
    Unit,
    (
      QuineId,
      Option[Int],
      Option[String],
      Option[Double],
      Option[Double],
      Option[String],
      Option[String],
      Option[AtTime],
    ),
    Either[ErrorResponse.ServerError, ErrorResponse.BadRequest],
    SuccessEnvelope.Ok[List[String]],
    Any,
    Future,
  ] = generateRandomWalk.serverLogic[Future](generateRandomWalkLogic)

  val algorithmEndpoints: List[ServerEndpoint[Any, Future]] = List(
    generateRandomWalkServerEndpoint,
    saveRandomWalksServerEndpoint,
  )
}
