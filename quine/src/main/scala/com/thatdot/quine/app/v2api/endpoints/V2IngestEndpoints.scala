package com.thatdot.quine.app.v2api.endpoints

import scala.concurrent.{ExecutionContext, Future}

import io.circe.generic.extras.auto._
import shapeless.{:+:, CNil, Coproduct}
import sttp.model.StatusCode
import sttp.tapir.generic.auto._
import sttp.tapir.server.ServerEndpoint
import sttp.tapir.server.ServerEndpoint.Full
import sttp.tapir.{Endpoint, EndpointInput, path, statusCode}

import com.thatdot.quine.app.util.StringOps
import com.thatdot.quine.app.v2api.definitions.ErrorResponse.{BadRequest, NotFound, ServerError}
import com.thatdot.quine.app.v2api.definitions.ErrorResponseHelpers.{badRequestError, notFoundError, serverError}
import com.thatdot.quine.app.v2api.definitions.ingest2.ApiIngest
import com.thatdot.quine.app.v2api.definitions.ingest2.ApiIngest.Oss
import com.thatdot.quine.app.v2api.definitions.{SuccessEnvelope, V2QuineEndpointDefinitions}

trait V2IngestEndpoints extends V2QuineEndpointDefinitions with StringOps {

  import com.thatdot.quine.app.v2api.converters.ApiToIngest.OssConversions._

  val ingestStreamNameElement: EndpointInput.PathCapture[String] =
    path[String]("name").description("Ingest Stream name.").example("NumbersStream")

  // We could consolidate `rawIngest` with `ingestBase,` above, by;
  // 1. Replace `rawIngest` uses with `ingestBase`
  // 2. Removing the `.errorOut*(serverError())` builder call
  // 3. Adjusting the `.errorOut*` calls or their dependencies to accommodate the new expected ERROR_OUTPUT
  // 4. Inline `rawIngest` implementation into `ingestBase`
  // But, FYI, step 3 is not immediately straightforward
  protected[endpoints] val rawIngest: Endpoint[Unit, Unit, Nothing, Unit, Any] =
    rawEndpoint("ingests")
      .tag("Ingest Streams")
      .description("Sources of streaming data ingested into the graph interpreter.")

  private val ingestBase: Endpoint[Unit, Unit, ServerError, Unit, Any] = rawIngest.errorOut(serverError())

  private val ingestExample = ApiIngest.Oss.QuineIngestConfiguration(
    ApiIngest.IngestSource.NumberIterator(0, None),
    query = "MATCH (n) WHERE id(n) = idFrom($that) SET n.num = $that",
    onStreamError = ApiIngest.LogStreamError,
    maxPerSecond = Some(100),
  )

  implicit private val ec: ExecutionContext = ExecutionContext.parasitic

  protected[endpoints] val createIngest: Endpoint[Unit, (String, Option[String], Oss.QuineIngestConfiguration), Either[
    ServerError,
    BadRequest,
  ], SuccessEnvelope.Created[ApiIngest.IngestStreamInfoWithName], Any] = ingestBase
    .name("Create Ingest Stream")
    .description(
      """Create an [Ingest Stream](https://docs.quine.io/components/ingest-sources/ingest-sources.html)
        |that connects a streaming event source to Quine and loads data into the graph.""".asOneLine + "\n\n" +
      """An Ingest Stream is defined by selecting a source `type`, then an appropriate data `format`,
        |and must be created with a unique name. Many Ingest Stream types allow a Cypher query to operate
        |on the event stream data to create nodes and relationships in the graph.""".asOneLine,
    )
    .in(ingestStreamNameElement)
    .in(namespaceParameter)
    .in(jsonOrYamlBody[ApiIngest.Oss.QuineIngestConfiguration](Some(ingestExample)))
    .post
    .out(statusCode(StatusCode.Created).description("Ingest Stream created."))
    .out(jsonBody[SuccessEnvelope.Created[ApiIngest.IngestStreamInfoWithName]])
    .errorOutEither(
      badRequestError(
        "Ingest Stream with that name already exists.",
        "Ingest Stream creation failed with config errors.",
      ),
    )

  protected[endpoints] val createIngestLogic: ((String, Option[String], Oss.QuineIngestConfiguration)) => Future[
    Either[Either[ServerError, BadRequest], SuccessEnvelope.Created[ApiIngest.IngestStreamInfoWithName]],
  ] = { case (ingestStreamName, ns, ingestStreamConfig) =>
    recoverServerErrorEitherWithServerError {
      appMethods.handleCreateIngest(ingestStreamName, namespaceFromParam(ns), ingestStreamConfig)
    } { case (stream, warnings) =>
      SuccessEnvelope.Created(stream, warnings = warnings.toList)
    }
  }

  private val createIngestServerEndpoint: Full[
    Unit,
    Unit,
    (String, Option[String], Oss.QuineIngestConfiguration),
    Either[ServerError, BadRequest],
    SuccessEnvelope.Created[ApiIngest.IngestStreamInfoWithName],
    Any,
    Future,
  ] = createIngest.serverLogic[Future](createIngestLogic)

  protected[endpoints] val pauseIngest: Endpoint[
    Unit,
    (String, Option[String]),
    Either[ServerError, Either[NotFound, BadRequest]],
    SuccessEnvelope.Ok[ApiIngest.IngestStreamInfoWithName],
    Any,
  ] = rawIngest
    .name("Pause Ingest Stream")
    .description("Temporarily pause processing new events by the named Ingest Stream.")
    .in(ingestStreamNameElement)
    .in("pause")
    .in(namespaceParameter)
    .post
    .errorOut(notFoundError("Ingest Stream with that name does not exist."))
    .errorOutEither(badRequestError("The Ingest has failed."))
    .errorOutEither(serverError())
    .mapErrorOut(err => err.swap)(err => err.swap)
    .out(statusCode(StatusCode.Ok))
    .out(jsonBody[SuccessEnvelope.Ok[ApiIngest.IngestStreamInfoWithName]])

  protected[endpoints] val pauseIngestLogic: ((String, Option[String])) => Future[
    Either[Either[ServerError, Either[NotFound, BadRequest]], SuccessEnvelope.Ok[ApiIngest.IngestStreamInfoWithName]],
  ] = { case (ingestStreamName, ns) =>
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

  private val pauseIngestServerEndpoint: Full[
    Unit,
    Unit,
    (String, Option[String]),
    Either[ServerError, Either[NotFound, BadRequest]],
    SuccessEnvelope.Ok[ApiIngest.IngestStreamInfoWithName],
    Any,
    Future,
  ] = pauseIngest.serverLogic[Future](pauseIngestLogic)

  protected[endpoints] val unpauseIngest: Endpoint[
    Unit,
    (String, Option[String]),
    Either[ServerError, Either[NotFound, BadRequest]],
    SuccessEnvelope.Ok[ApiIngest.IngestStreamInfoWithName],
    Any,
  ] = rawIngest
    .name("Unpause Ingest Stream")
    .description("Resume processing new events by the named Ingest Stream.")
    .in(ingestStreamNameElement)
    .in("start")
    .in(namespaceParameter)
    .post
    .errorOut(notFoundError("Ingest Stream with that name does not exist."))
    .errorOutEither(badRequestError("The Ingest has failed."))
    .errorOutEither(serverError())
    .mapErrorOut(err => err.swap)(err => err.swap)
    .out(statusCode(StatusCode.Ok))
    .out(jsonBody[SuccessEnvelope.Ok[ApiIngest.IngestStreamInfoWithName]])

  protected[endpoints] val unpauseIngestLogic: ((String, Option[String])) => Future[
    Either[Either[ServerError, Either[NotFound, BadRequest]], SuccessEnvelope.Ok[ApiIngest.IngestStreamInfoWithName]],
  ] = { case (ingestStreamName, ns) =>
    recoverServerErrorEither(appMethods.unpauseIngestStream(ingestStreamName, namespaceFromParam(ns)).map {
      _.left
        .map((err: BadRequest) => Coproduct[NotFound :+: BadRequest :+: CNil](err))
        .flatMap {
          case None =>
            Left(
              Coproduct[NotFound :+: BadRequest :+: CNil](
                NotFound(s"Ingest Stream $ingestStreamName does not exist."),
              ),
            )
          case Some(streamInfo) => Right(streamInfo)
        }
    })((id: ApiIngest.IngestStreamInfoWithName) => SuccessEnvelope.Ok(id))
  }

  private val unpauseIngestServerEndpoint: Full[
    Unit,
    Unit,
    (String, Option[String]),
    Either[ServerError, Either[NotFound, BadRequest]],
    SuccessEnvelope.Ok[ApiIngest.IngestStreamInfoWithName],
    Any,
    Future,
  ] = unpauseIngest.serverLogic[Future](unpauseIngestLogic)

  protected[endpoints] val deleteIngest: Endpoint[
    Unit,
    (String, Option[String]),
    Either[ServerError, NotFound],
    SuccessEnvelope.Ok[ApiIngest.IngestStreamInfoWithName],
    Any,
  ] = ingestBase
    .name("Delete Ingest Stream")
    .description(
      "Immediately halt and remove the named Ingest Stream from Quine.\n\n" +
      """The Ingest Stream will complete any pending operations and return stream
        |information once the operation is complete.""".asOneLine,
    )
    .in(ingestStreamNameElement)
    .in(namespaceParameter)
    .delete
    .errorOutEither(notFoundError("Ingest Stream with that name does not exist."))
    .out(statusCode(StatusCode.Ok))
    .out(jsonBody[SuccessEnvelope.Ok[ApiIngest.IngestStreamInfoWithName]])

  protected[endpoints] val deleteIngestLogic: ((String, Option[String])) => Future[
    Either[Either[ServerError, NotFound], SuccessEnvelope.Ok[ApiIngest.IngestStreamInfoWithName]],
  ] = { case (ingestStreamName, ns) =>
    recoverServerErrorEither(
      appMethods
        .deleteIngestStream(ingestStreamName, namespaceFromParam(ns))
        .map {
          case None => Left(Coproduct[NotFound :+: CNil](NotFound(s"Ingest Stream $ingestStreamName does not exist")))
          case Some(streamInfo) => Right(streamInfo)
        },
    )((inp: ApiIngest.IngestStreamInfoWithName) => SuccessEnvelope.Ok(inp))
  }

  private val deleteIngestServerEndpoint
    : Full[Unit, Unit, (String, Option[String]), Either[ServerError, NotFound], SuccessEnvelope.Ok[
      ApiIngest.IngestStreamInfoWithName,
    ], Any, Future] = deleteIngest.serverLogic[Future](deleteIngestLogic)

  protected[endpoints] val ingestStatus: Endpoint[
    Unit,
    (String, Option[String]),
    Either[ServerError, NotFound],
    SuccessEnvelope.Ok[ApiIngest.IngestStreamInfoWithName],
    Any,
  ] = ingestBase
    .name("Ingest Stream Status")
    .description("Return the Ingest Stream status information for a configured Ingest Stream by name.")
    .in(ingestStreamNameElement)
    .in(namespaceParameter)
    .get
    .errorOutEither(notFoundError("Ingest Stream with that name does not exist."))
    .out(statusCode(StatusCode.Ok))
    .out(jsonBody[SuccessEnvelope.Ok[ApiIngest.IngestStreamInfoWithName]])

  protected[endpoints] val ingestStatusLogic: ((String, Option[String])) => Future[
    Either[Either[ServerError, NotFound], SuccessEnvelope.Ok[ApiIngest.IngestStreamInfoWithName]],
  ] = { case (ingestStreamName, ns) =>
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

  private val ingestStatusServerEndpoint: Full[
    Unit,
    Unit,
    (String, Option[String]),
    Either[ServerError, NotFound],
    SuccessEnvelope.Ok[ApiIngest.IngestStreamInfoWithName],
    Any,
    Future,
  ] = ingestStatus.serverLogic[Future](ingestStatusLogic)

  protected[endpoints] val listIngest
    : Endpoint[Unit, Option[String], ServerError, SuccessEnvelope.Ok[Map[String, ApiIngest.IngestStreamInfo]], Any] =
    ingestBase
      .name("List Ingest Streams")
      .description(
        """Return a JSON object containing the configured [Ingest Streams](https://docs.quine.io/components/ingest-sources/ingest-sources.html)
        |and their associated stream metrics keyed by the stream name.""".asOneLine,
      )
      .in(namespaceParameter)
      .get
      .out(statusCode(StatusCode.Ok))
      .out(jsonBody[SuccessEnvelope.Ok[Map[String, ApiIngest.IngestStreamInfo]]])

  protected[endpoints] val listIngestLogic
    : Option[String] => Future[Either[ServerError, SuccessEnvelope.Ok[Map[String, ApiIngest.IngestStreamInfo]]]] = ns =>
    recoverServerError(appMethods.listIngestStreams(namespaceFromParam(ns)))(
      (inp: Map[String, ApiIngest.IngestStreamInfo]) => SuccessEnvelope.Ok.apply(inp),
    )

  private val listIngestServerEndpoint: Full[
    Unit,
    Unit,
    Option[String],
    ServerError,
    SuccessEnvelope.Ok[Map[String, ApiIngest.IngestStreamInfo]],
    Any,
    Future,
  ] = listIngest.serverLogic[Future](listIngestLogic)

  val ingestEndpoints: List[ServerEndpoint[Any, Future]] = List(
    createIngestServerEndpoint,
    pauseIngestServerEndpoint,
    unpauseIngestServerEndpoint,
    deleteIngestServerEndpoint,
    ingestStatusServerEndpoint,
    listIngestServerEndpoint,
  )

}
