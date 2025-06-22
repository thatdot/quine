package com.thatdot.quine.app.v2api.endpoints

import scala.concurrent.{ExecutionContext, Future}

import io.circe.generic.extras.auto._
import shapeless.{:+:, CNil, Coproduct}
import sttp.model.StatusCode
import sttp.tapir.generic.auto._
import sttp.tapir.server.ServerEndpoint
import sttp.tapir.server.ServerEndpoint.Full
import sttp.tapir.{EndpointInput, path, statusCode}

import com.thatdot.quine.app.v2api.definitions.ErrorResponse.{BadRequest, NotFound, ServerError}
import com.thatdot.quine.app.v2api.definitions.ErrorResponseHelpers.{badRequestError, notFoundError, serverError}
import com.thatdot.quine.app.v2api.definitions.ingest2.ApiIngest
import com.thatdot.quine.app.v2api.definitions.{SuccessEnvelope, V2QuineEndpointDefinitions}
trait V2IngestEndpoints extends V2QuineEndpointDefinitions {

  import com.thatdot.quine.app.v2api.converters.ApiToIngest.OssConversions._

  private val ingestStreamNameElement: EndpointInput.PathCapture[String] =
    path[String]("name").description("Ingest stream name").example("NumbersStream")

  private def ingestEndpoint =
    rawEndpoint("ingests")
      .tag("Ingest Streams")
      .description("Sources of streaming data ingested into the graph interpreter.")
      .errorOut(serverError())

  private def rawIngestEndpoint =
    rawEndpoint("ingests")
      .tag("Ingest Streams")
      .description("Sources of streaming data ingested into the graph interpreter.")

  private val ingestExample = ApiIngest.Oss.QuineIngestConfiguration(
    ApiIngest.IngestSource.NumberIterator(0, None),
    query = "MATCH (n) WHERE id(n) = idFrom($that) SET n.num = $that ",
    onStreamError = ApiIngest.LogStreamError,
    maxPerSecond = Some(100),
  )

  implicit val ec: ExecutionContext = ExecutionContext.parasitic

  private val createIngestEndpoint =
    ingestEndpoint
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
      .out(jsonBody[SuccessEnvelope.Created[ApiIngest.IngestStreamInfoWithName]])
      .errorOutEither(
        badRequestError(
          "Ingest Stream with that name already exists",
          "Ingest Stream creation failed with config errors",
        ),
      )
      .serverLogic[Future] { case (ingestStreamName, ns, ingestStreamConfig) =>
        recoverServerErrorEitherWithServerError(
          appMethods.handleCreateIngest(ingestStreamName, namespaceFromParam(ns), ingestStreamConfig),
        ) { case (stream, warnings) =>
          SuccessEnvelope.Created(stream, warnings = warnings.toList)
        }
      }

  private val pauseIngestEndpoint: Full[Unit, Unit, (String, Option[String]), Either[
    ServerError,
    Either[NotFound, BadRequest],
  ], SuccessEnvelope.Ok[
    ApiIngest.IngestStreamInfoWithName,
  ], Any, Future] = rawIngestEndpoint
    .name("Pause Ingest Stream")
    .description("Temporarily pause processing new events by the named ingest stream.")
    .in(ingestStreamNameElement)
    .in("pause")
    .in(namespaceParameter)
    .post
    .errorOut(notFoundError("Ingest stream with that name does not exist"))
    .errorOutEither(badRequestError("The Ingest has failed"))
    .errorOutEither(serverError())
    .mapErrorOut(err => err.swap)(err => err.swap)
    .out(statusCode(StatusCode.Ok))
    .out(jsonBody[SuccessEnvelope.Ok[ApiIngest.IngestStreamInfoWithName]])
    .serverLogic[Future] { case (ingestStreamName, ns) =>
      recoverServerErrorEither(
        appMethods
          .pauseIngestStream(ingestStreamName, namespaceFromParam(ns))
          .map {
            _.left
              .map((err: BadRequest) => Coproduct[NotFound :+: BadRequest :+: CNil](err))
              .flatMap {
                case None =>
                  Left(
                    Coproduct[NotFound :+: BadRequest :+: CNil](
                      NotFound(s"Ingest Stream $ingestStreamName does not exist"),
                    ),
                  )
                case Some(streamInfo) => Right(streamInfo)
              }
          },
      )(out => SuccessEnvelope.Ok(out))
    }

  private val unpauseIngestEndpoint: Full[Unit, Unit, (String, Option[String]), Either[
    ServerError,
    Either[NotFound, BadRequest],
  ], SuccessEnvelope.Ok[
    ApiIngest.IngestStreamInfoWithName,
  ], Any, Future] = rawIngestEndpoint
    .name("Unpause Ingest Stream")
    .description("Resume processing new events by the named ingest stream.")
    .in(ingestStreamNameElement)
    .in("start")
    .in(namespaceParameter)
    .post
    .errorOut(notFoundError("Ingest stream with that name does not exist"))
    .errorOutEither(badRequestError("The Ingest has failed"))
    .errorOutEither(serverError())
    .mapErrorOut(err => err.swap)(err => err.swap)
    .out(statusCode(StatusCode.Ok))
    .out(jsonBody[SuccessEnvelope.Ok[ApiIngest.IngestStreamInfoWithName]])
    .serverLogic[Future] { case (ingestStreamName, ns) =>
      recoverServerErrorEither(appMethods.unpauseIngestStream(ingestStreamName, namespaceFromParam(ns)).map {
        _.left
          .map((err: BadRequest) => Coproduct[NotFound :+: BadRequest :+: CNil](err))
          .flatMap {
            case None =>
              Left(
                Coproduct[NotFound :+: BadRequest :+: CNil](
                  NotFound(s"Ingest Stream $ingestStreamName does not exist"),
                ),
              )
            case Some(streamInfo) => Right(streamInfo)
          }
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
    .errorOutEither(notFoundError("Ingest stream with that name does not exist"))
    .out(statusCode(StatusCode.Ok))
    .out(jsonBody[SuccessEnvelope.Ok[ApiIngest.IngestStreamInfoWithName]])
    .serverLogic[Future] { case (ingestStreamName, ns) =>
      recoverServerErrorEither(
        appMethods
          .deleteIngestStream(ingestStreamName, namespaceFromParam(ns))
          .map {
            case None => Left(Coproduct[NotFound :+: CNil](NotFound(s"Ingest Stream $ingestStreamName does not exist")))
            case Some(streamInfo) => Right(streamInfo)
          },
      )((inp: ApiIngest.IngestStreamInfoWithName) => SuccessEnvelope.Ok(inp))
    }

  private val ingestStatusEndpoint = ingestEndpoint
    .name("Ingest Stream Status")
    .description("Return the ingest stream status information for a configured ingest stream by name.")
    .in(ingestStreamNameElement)
    .in(namespaceParameter)
    .get
    .errorOutEither(notFoundError("Ingest stream with that name does not exist"))
    .out(statusCode(StatusCode.Ok))
    .out(jsonBody[SuccessEnvelope.Ok[ApiIngest.IngestStreamInfoWithName]])
    .serverLogic[Future] { case (ingestStreamName, ns) =>
      recoverServerErrorEither(
        appMethods
          .ingestStreamStatus(ingestStreamName, namespaceFromParam(ns))
          .map {
            case None =>
              Left(
                Coproduct[NotFound :+: CNil](NotFound(s"Ingest Stream $ingestStreamName does not exist")),
              )
            case Some(streamInfo) => Right(streamInfo)
          },
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
    .serverLogic[Future] { ns =>
      recoverServerError(appMethods.listIngestStreams(namespaceFromParam(ns)))(
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
