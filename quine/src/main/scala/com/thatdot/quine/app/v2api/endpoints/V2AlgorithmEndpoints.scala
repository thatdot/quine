package com.thatdot.quine.app.v2api.endpoints

import java.nio.file.{Files, Paths}

import scala.concurrent.Future

import org.apache.pekko.stream.IOResult
import org.apache.pekko.stream.connectors.s3.MultipartUploadResult
import org.apache.pekko.stream.connectors.s3.scaladsl.S3
import org.apache.pekko.stream.scaladsl.{FileIO, Sink}
import org.apache.pekko.util.ByteString

import io.circe.generic.auto._
import io.circe.{Decoder, Encoder}
import sttp.model.StatusCode
import sttp.tapir.Schema.annotations.{description, title}
import sttp.tapir._
import sttp.tapir.generic.auto.schemaForCaseClass
import sttp.tapir.json.circe.TapirJsonCirce
import sttp.tapir.server.ServerEndpoint

import com.thatdot.quine.app.v2api.definitions._
import com.thatdot.quine.graph.NamespaceId
import com.thatdot.quine.model.{Milliseconds, QuineId}
import com.thatdot.quine.routes.IngestRoutes

object V2AlgorithmEndpointEntities extends TapirJsonCirce {
  /* WARNING: these values duplicate `AlgorithmGraph.defaults.walkPrefix` and `walkSuffix` from the
   * `com.thatdot.quine.graph` package which is not available here.
   * Beware of changes in one place not mirrored to the other!
   */

  private val queryPrefix = "MATCH (thisNode) WHERE id(thisNode) = $n "
  private val querySuffix = "RETURN id(thisNode)"

  implicit val positiveIntCodec: Codec[String, Int, CodecFormat.TextPlain] = Codec.int.validate(Validator.positive)

  /** SQ Output Name path element */
  val walkLengthQs: EndpointInput.Query[Option[Int]] =
    query[Option[Int]]("length").description("Maximum length of a walk. Default: `10`")

  val onNodeQueryQs: EndpointInput.Query[Option[String]] = query[Option[String]]("query").description(
    s"""Cypher query run on each node of the walk. You can use this query to collect properties instead of node IDs.
       |A `RETURN` statement can return any number of values, separated by `,`s. If returning the same value
       |multiple times, you will need to alias subsequent values with `AS` so that column names are unique. If a list
       |is returned, its content will be flattened out one level and concatenated with the rest of the aggregated
       |values.
       |
       |The provided query will have the following prefix prepended: `$queryPrefix` where `${"$n"}` evaluates
       |to the ID of the node on which the query is executed. The default value of this parameter is:
       |`$querySuffix`""".stripMargin,
  )
  val numberOfWalksQs: EndpointInput.Query[Option[Int]] = query[Option[Int]]("count")
    .description("An optional integer for how many random walks from each node to generate. Default: `5`")

  val returnQs: EndpointInput.Query[Option[Double]] = query[Option[Double]]("return").description(
    "the `p` parameter to determine likelihood of returning to the node just visited: `1/p`  Lower is " +
    "more likely; but if `0`, never return to previous node. Default: `1`",
  )

  val inOutQs: EndpointInput.Query[Option[Double]] = query[Option[Double]]("in-out").description(
    "the `q` parameter to determine likelihood of visiting a node outside the neighborhood of the" +
    " starting node: `1/q`  Lower is more likely; but if `0`, never visit the neighborhood. Default: `1`",
  )

  val randomSeedOptQs: EndpointInput.Query[Option[String]] = query[Option[String]]("seed").description(
    "Optionally specify any string as a random seed for generating walks. This is used to determine all " +
    "randomness, so providing the same seed will always produce the same random walk. If unset, a new seed is " +
    "used each time a random choice is needed.",
  )

  @title("Save Location")
  sealed trait TSaveLocation {
    def fileName(defaultFileName: String): String

    def toSink(filename: String): Sink[ByteString, Future[Object]]
  }

  @title("Local File")
  case class LocalFile(
    @description("Optional name of the file to save in the working directory") fileName: Option[String],
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
    @description("S3 bucket name") bucketName: String,
    @description("Optional name of the file in the S3 bucket") key: Option[String],
  ) extends TSaveLocation {
    def fileName(defaultFileName: String): String = key.getOrElse(defaultFileName)

    def toSink(fname: String): Sink[ByteString, Future[MultipartUploadResult]] = S3.multipartUpload(bucketName, fname)

  }

}

trait V2AlgorithmEndpoints extends V2EndpointDefinitions {

  import V2AlgorithmEndpointEntities._

  /** Algorithm base path */
  private def algorithmEndpoint[T](implicit
    schema: Schema[ObjectEnvelope[T]],
    encoder: Encoder[T],
    decoder: Decoder[T],
  ) = baseEndpoint[T]("algorithm")
    .tag("Graph Algorithms")
    .description("High-level operations on the graph to support graph AI, ML, and other algorithms.")

  private def saveRandomWalksEndpoint = algorithmEndpoint[Option[String]]
    .name("Save Random Walks")
    .description("""Generate random walks from all nodes in the graph (optionally: at a specific historical time), and save
the results.

The output file is a CSV where each row is one random walk. The first column will always
be the node ID where the walk originated. Each subsequent column will be either:

a.) by default, the ID of each node encountered (including the starting node ID again in the second
column), or

b.) optionally, the results of Cypher query executed from each node encountered on the walk; where
multiple columns and rows returned from this query will be concatenated together sequentially into
the aggregated walk results.

**The resulting CSV may have rows of varying length.**

The name of the output file is derived from the arguments used to generate it; or a custom file name can
be specified in the API request body. If no custom name is specified, the following values are
concatenated to produce the final file name:

 - the constant prefix: `graph-walk-`
 - the timestamp provided in `at-time` or else the current time when run. A trailing `_T` is appended if no timestamp was specified.
 - the `length` parameter followed by the constant `x`
 - the `count` parameter
 - the constant `-q` follow by the number of characters in the supplied `query` (`0` if not specified)
 - the `return` parameter followed by the constant `x`
 - the `in-out` parameter
 - the `seed` parameter or `_` if none was supplied
 - the constant suffix `.csv`

 Example file name: `graph-walk-1675122348011_T-10x5-q0-1.0x1.0-_.csv`

 The name of the actual file being written is returned in the API response body.""")
    .in("walk")
    .in(walkLengthQs)
    .in(numberOfWalksQs)
    .in(onNodeQueryQs)
    .in(returnQs)
    .in(inOutQs)
    .in(randomSeedOptQs)
    .in(namespaceParameter)
    .in(atTimeParameter)
    .in(parallelismParameter)
    .in(jsonOrYamlBody[TSaveLocation])
    .out(statusCode(StatusCode.Accepted))
    .put
    .serverLogic {
      case (
            memberIdx,
            walkLengthOpt,
            numWalksOpt,
            queryOpt,
            returnOpt,
            inOutOpt,
            randomSeedOpt,
            namespace,
            atTimeOpt,
            parallelismOpt,
            saveLocation,
          ) =>
        runServerLogicWithError[
          (
            Option[Int],
            Option[Int],
            Option[String],
            Option[Double],
            Option[Double],
            Option[String],
            NamespaceId,
            Option[Milliseconds],
            Int,
            TSaveLocation,
          ),
          Option[String],
        ](
          SaveRandomWalksApiCmd,
          memberIdx,
          (
            walkLengthOpt,
            numWalksOpt,
            queryOpt,
            returnOpt,
            inOutOpt,
            randomSeedOpt,
            namespaceFromParam(namespace),
            atTimeOpt,
            parallelismOpt.getOrElse(IngestRoutes.defaultWriteParallelism),
            saveLocation,
          ),
          t =>
            Future.successful(app.algorithmSaveRandomWalks(t._1, t._2, t._3, t._4, t._5, t._6, t._7, t._8, t._9, t._10)),
        )
    }

  private def generateRandomWalkEndpoint = algorithmEndpoint[Option[List[String]]]
    .name("Generate Random Walk")
    .description("Generate a random walk from a node in the graph and return the results.")
    .get
    .in("walk")
    .in(path[QuineId]("id").description("Node id"))
    .in(walkLengthQs)
    .in(onNodeQueryQs)
    .in(returnQs)
    .in(inOutQs)
    .in(randomSeedOptQs)
    .in(namespaceParameter)
    .in(atTimeParameter)
    .serverLogic {
      case (memberIdx, id, walkLengthOpt, queryOpt, returnOpt, inOutOpt, randomSeedOpt, namespace, atTimeOpt) =>
        runServerLogicWithError[
          (
            QuineId,
            Option[Int],
            Option[String],
            Option[Double],
            Option[Double],
            Option[String],
            NamespaceId,
            Option[Milliseconds],
          ),
          Option[List[String]],
        ](
          GenerateRandomWalkApiCmd,
          memberIdx,
          (id, walkLengthOpt, queryOpt, returnOpt, inOutOpt, randomSeedOpt, namespaceFromParam(namespace), atTimeOpt),
          t => app.algorithmRandomWalk(t._1, t._2, t._3, t._4, t._5, t._6, t._7, t._8),
        )

    }

  val algorithmEndpoints: List[ServerEndpoint[Any, Future]] = List(
    generateRandomWalkEndpoint,
    saveRandomWalksEndpoint,
  )
}
