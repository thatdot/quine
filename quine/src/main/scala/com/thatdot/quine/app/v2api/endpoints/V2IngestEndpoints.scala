package com.thatdot.quine.app.v2api.endpoints

import scala.concurrent.{ExecutionContext, Future}

import io.circe.generic.extras.auto._
import sttp.model.StatusCode
import sttp.tapir.generic.auto._
import sttp.tapir.server.ServerEndpoint
import sttp.tapir.{EndpointInput, path, statusCode}

import com.thatdot.quine.app.v2api.definitions.CustomError.{badRequestError, notFoundError}
import com.thatdot.quine.app.v2api.definitions.ingest2.ApiIngest
import com.thatdot.quine.app.v2api.definitions.{
  BadRequest,
  ErrorEnvelope,
  NotFound,
  SuccessEnvelope,
  V2QuineEndpointDefinitions,
}

trait V2IngestEndpoints extends V2QuineEndpointDefinitions {

  import com.thatdot.quine.app.v2api.converters.ApiToIngest.OssConversions._

  private val ingestStreamNameElement: EndpointInput.PathCapture[String] =
    path[String]("name").description("Ingest stream name").example("NumbersStream")

  private def ingestEndpoint =
    rawEndpoint("ingests")
      .tag("Ingest Streams")
      .description("Sources of streaming data ingested into the graph interpreter.")

  private val ingestExample = ApiIngest.Oss.QuineIngestConfiguration(
    ApiIngest.NumberIteratorIngest(0, None),
    query = "MATCH (n) WHERE id(n) = idFrom($that) SET n.num = $that ",
    onRecordError = ApiIngest.LogRecordErrorHandler,
    onStreamError = ApiIngest.LogStreamError,
    maxPerSecond = Some(100),
  )

  // TODO: fix the layout of the serverLogicFromEither so this isn't required
  implicit val ec: ExecutionContext = ExecutionContext.parasitic

  // TODO: return resource
  private val createIngestEndpoint = ingestEndpoint
    .name("Create Ingest Stream")
    .description("""Create an [ingest stream](https://docs.quine.io/components/ingest-sources/ingest-sources.html)
                   |that connects a streaming event source to Quine and loads data into the graph.
                   |
                   |An ingest stream is defined by selecting a source `type`, then an appropriate data `format`,
                   |and must be created with a unique name. Many ingest stream types allow a Cypher query to operate
                   |on the event stream data to create nodes and relationships in the graph.""".stripMargin)
    .in(ingestStreamNameElement)
    .in(namespaceParameter)
    .in(jsonOrYamlBody[ApiIngest.Oss.QuineIngestConfiguration](Some(ingestExample)))
    .post
    .out(statusCode(StatusCode.Created).description("Ingest Stream Created"))
    .out(jsonBody[SuccessEnvelope.Created[Unit]])
    .errorOutVariantPrepend(badRequestError("Quine", "Ingest stream with that name already exists"))
    .serverLogic { case (ingestStreamName, ns, ingestStreamConfig) =>
      runServerLogicFromEitherCreated(appMethods.listIngestStreams(namespaceFromParam(ns)).map { streams =>
        streams.get(ingestStreamName) match {
          case Some(_) =>
            Left(ErrorEnvelope(BadRequest(s"Ingest Stream $ingestStreamName already exists")))
          case None =>
            appMethods
              .createIngestStream(ingestStreamName, ingestStreamConfig, namespaceFromParam(ns))
              .map(_ => ())
              .left
              .map(ErrorEnvelope.apply)
        }
      })((inp: Unit) => SuccessEnvelope.Created(inp))
    }

  private val pauseIngestEndpoint = ingestEndpoint
    .name("Pause Ingest Stream")
    .description("Temporarily pause processing new events by the named ingest stream.")
    .in(ingestStreamNameElement)
    .in("pause")
    .in(namespaceParameter)
    .post
    .errorOutVariantPrepend(notFoundError("Ingest stream with that name does not exist"))
    .out(statusCode(StatusCode.Ok))
    .out(jsonBody[SuccessEnvelope.Ok[ApiIngest.IngestStreamInfoWithName]])
    .serverLogic { case (ingestStreamName, ns) =>
      runServerLogicFromEitherOk(appMethods.pauseIngestStream(ingestStreamName, namespaceFromParam(ns)).map {
        _.flatMap {
          case None => Left(NotFound(s"Ingest Stream $ingestStreamName does not exist"))
          case Some(streamInfo) => Right(streamInfo)
        }.left.map(ErrorEnvelope.apply)
      })((id: ApiIngest.IngestStreamInfoWithName) => SuccessEnvelope.Ok(id))
    }

  private val unpauseIngestEndpoint = ingestEndpoint
    .name("Unpause Ingest Stream")
    .description("Resume processing new events by the named ingest stream.")
    .in(ingestStreamNameElement)
    .in("start")
    .in(namespaceParameter)
    .post
    .errorOutVariantPrepend(notFoundError("Quine", "Ingest stream with that name does not exist"))
    .out(statusCode(StatusCode.Ok))
    .out(jsonBody[SuccessEnvelope.Ok[ApiIngest.IngestStreamInfoWithName]])
    .serverLogic { case (ingestStreamName, ns) =>
      runServerLogicFromEitherOk(appMethods.unpauseIngestStream(ingestStreamName, namespaceFromParam(ns)).map {
        _.flatMap {
          case None => Left(NotFound(s"Ingest Stream $ingestStreamName does not exist"))
          case Some(streamInfo) => Right(streamInfo)
        }.left.map(ErrorEnvelope.apply)
      })((id: ApiIngest.IngestStreamInfoWithName) => SuccessEnvelope.Ok(id))
    }

  private val deleteIngestEndpoint = ingestEndpoint
    .name("Delete Ingest Stream")
    .description("""Immediately halt and remove the named ingest stream from Quine.
                    |
                    |The ingest stream will complete any pending operations and return stream information
                    |once the operation is complete.""".stripMargin)
    .in(ingestStreamNameElement)
    .in(namespaceParameter)
    .delete
    .errorOutVariantPrepend(notFoundError("Ingest stream with that name does not exist"))
    .out(statusCode(StatusCode.Ok))
    .out(jsonBody[SuccessEnvelope.Ok[ApiIngest.IngestStreamInfoWithName]])
    .serverLogic { case (ingestStreamName, ns) =>
      runServerLogicFromEitherOk(
        appMethods
          .deleteIngestStream(ingestStreamName, namespaceFromParam(ns))
          .map {
            case None => Left(NotFound(s"Ingest Stream $ingestStreamName does not exist"))
            case Some(streamInfo) => Right(streamInfo)
          }
          .map(f => f.left.map(ErrorEnvelope.apply)),
      )((inp: ApiIngest.IngestStreamInfoWithName) => SuccessEnvelope.Ok(inp))
    }

  private val ingestStatusEndpoint = ingestEndpoint
    .name("Ingest Stream Status")
    .description("Return the ingest stream status information for a configured ingest stream by name.")
    .in(ingestStreamNameElement)
    .in(namespaceParameter)
    .get
    .errorOutVariantPrepend(notFoundError("Ingest stream with that name does not exist"))
    .out(statusCode(StatusCode.Ok))
    .out(jsonBody[SuccessEnvelope.Ok[ApiIngest.IngestStreamInfoWithName]])
    .serverLogic { case (ingestStreamName, ns) =>
      runServerLogicFromEitherOk(
        appMethods
          .ingestStreamStatus(ingestStreamName, namespaceFromParam(ns))
          .map {
            case None => Left(NotFound(s"Ingest Stream $ingestStreamName does not exist"))
            case Some(streamInfo) => Right(streamInfo)
          }
          .map(f => f.left.map(ErrorEnvelope.apply)),
      )((inp: ApiIngest.IngestStreamInfoWithName) => SuccessEnvelope.Ok.apply(inp))
    }

  private val listIngestEndpoint = ingestEndpoint
    .name("List Ingest Streams")
    .description(
      """Return a JSON object containing the configured [ingest streams](https://docs.quine.io/components/ingest-sources/ingest-sources.html)
        |and their associated stream metrics keyed by the stream name. """.stripMargin,
    )
    .in(namespaceParameter)
    .get
    .out(statusCode(StatusCode.Ok))
    .out(jsonBody[SuccessEnvelope.Ok[Map[String, ApiIngest.IngestStreamInfo]]])
    .serverLogic { ns =>
      runServerLogicOk(appMethods.listIngestStreams(namespaceFromParam(ns)))(
        (inp: Map[String, ApiIngest.IngestStreamInfo]) => SuccessEnvelope.Ok.apply(inp),
      )
    }

  val ingestEndpoints: List[ServerEndpoint[Any, Future]] = List(
    createIngestEndpoint,
    pauseIngestEndpoint,
    unpauseIngestEndpoint,
    deleteIngestEndpoint,
    ingestStatusEndpoint,
    listIngestEndpoint,
  )

}
