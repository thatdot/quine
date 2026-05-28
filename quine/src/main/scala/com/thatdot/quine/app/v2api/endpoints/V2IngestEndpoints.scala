package com.thatdot.quine.app.v2api.endpoints

import scala.concurrent.{ExecutionContext, Future}

import shapeless.{:+:, CNil, Coproduct}
import sttp.model.StatusCode
import sttp.tapir.server.ServerEndpoint
import sttp.tapir.server.ServerEndpoint.Full
import sttp.tapir.{Endpoint, EndpointInput, header, path, statusCode}

import com.thatdot.api.v2.ErrorResponse.{BadRequest, NotFound, ServerError}
import com.thatdot.api.v2.ErrorResponseHelpers.{badRequestError, notFoundError, serverError}
import com.thatdot.api.v2.{Page, ResourceName}
import com.thatdot.quine.app.util.StringOps
import com.thatdot.quine.app.v2api.definitions.ingest2.ApiIngest
import com.thatdot.quine.app.v2api.definitions.ingest2.ApiIngest.Oss
import com.thatdot.quine.app.v2api.definitions.{
  CommonParameters,
  CustomMethod,
  GraphScopedEndpoints,
  QuineApiMethods,
  V2QuineEndpointDefinitions,
}
import com.thatdot.quine.graph.NamespaceId

object V2IngestEndpoints {

  /** Format a set of advisory warnings as an RFC 7234 `Warning` header value, or `None`
    * if there are no warnings to report. Each warning becomes a `199 quine "<text>"`
    * entry; multiple entries are comma-separated. The `199` warn-code is "Miscellaneous
    * Warning" — appropriate for advisory server-side notices that aren't tied to caching
    * or HTTP-level concerns.
    */
  def warningHeader(warnings: Set[String]): Option[String] =
    if (warnings.isEmpty) None
    else
      Some(
        warnings.iterator.map(w => s"""199 quine "${w.replace("\\", "\\\\").replace("\"", "\\\"")}"""").mkString(", "),
      )
}

trait V2IngestEndpoints
    extends V2QuineEndpointDefinitions
    with CommonParameters
    with GraphScopedEndpoints
    with StringOps {
  import com.thatdot.quine.app.v2api.converters.ApiToIngest.OssConversions._

  val appMethods: QuineApiMethods

  private val ingestExampleName: ResourceName = ResourceName.unsafeFromString("numbers")

  val ingestStreamNameElement: EndpointInput.PathCapture[ResourceName] =
    path[ResourceName]("ingestName").description("Ingest Stream name.").example(ingestExampleName)

  private def ingestStreamNameWithVerb(verb: String): EndpointInput.PathCapture[ResourceName] =
    CustomMethod
      .colonVerbPath[ResourceName]("ingestName", verb)
      .description("Ingest Stream name.")
      .example(ingestExampleName)

  // We could consolidate `rawIngest` with `ingestBase,` above, by;
  // 1. Replace `rawIngest` uses with `ingestBase`
  // 2. Removing the `.errorOut*(serverError())` builder call
  // 3. Adjusting the `.errorOut*` calls or their dependencies to accommodate the new expected ERROR_OUTPUT
  // 4. Inline `rawIngest` implementation into `ingestBase`
  // But, FYI, step 3 is not immediately straightforward
  protected[endpoints] val rawIngest: Endpoint[Unit, NamespaceId, Nothing, Unit, Any] =
    graphScopedEndpoint("ingests")
      .tag("Ingest Streams")
      .description("Sources of streaming data ingested into the graph interpreter.")

  private val ingestBase: Endpoint[Unit, NamespaceId, ServerError, Unit, Any] = rawIngest.errorOut(serverError())

  private val ingestExample = ApiIngest.Oss.QuineIngestConfiguration(
    name = ResourceName.unsafeFromString("numbers"),
    source = ApiIngest.IngestSource.NumberIterator(0, None),
    query = "MATCH (n) WHERE id(n) = idFrom($that) SET n.num = $that",
    onStreamError = ApiIngest.LogStreamError,
    maxPerSecond = Some(100),
  )

  implicit private val ec: ExecutionContext = ExecutionContext.parasitic

  protected[endpoints] val createIngest
    : Endpoint[Unit, (NamespaceId, Option[Int], Oss.QuineIngestConfiguration), Either[
      ServerError,
      Either[BadRequest, NotFound],
    ], (ApiIngest.IngestStreamInfoWithName, Option[String]), Any] = rawIngest
    .name("create-ingest")
    .summary("Create Ingest Stream")
    .description(
      """Create an [Ingest Stream](https://quine.io/learn/ingest-sources/)
        |that connects a streaming event source to Quine and loads data into the graph.""".asOneLine + "\n\n" +
      """An Ingest Stream is defined by selecting a source `type`, then an appropriate data `format`,
        |and must be created with a unique name. Many Ingest Stream types allow a Cypher query to operate
        |on the event stream data to create nodes and relationships in the graph.""".asOneLine,
    )
    .in(memberIdxParameter)
    .in(jsonOrYamlBody[ApiIngest.Oss.QuineIngestConfiguration](Some(ingestExample)))
    .post
    .out(statusCode(StatusCode.Created).description("Ingest Stream created."))
    .out(jsonBody[ApiIngest.IngestStreamInfoWithName])
    .out(
      header[Option[String]]("Warning").description(
        "Advisory warnings about the created Ingest Stream (e.g. Cypher analysis warnings). " +
        "RFC 7234 Warning header; multiple warnings are comma-separated `199` entries.",
      ),
    )
    .errorOut(
      badRequestError(
        "Ingest Stream with that name already exists.",
        "Ingest Stream creation failed with config errors.",
      ),
    )
    .errorOutEither(notFoundError("Graph not found."))
    .errorOutEither(serverError())
    .mapErrorOut(err => err.swap)(err => err.swap)

  protected[endpoints] val createIngestLogic: ((NamespaceId, Option[Int], Oss.QuineIngestConfiguration)) => Future[
    Either[Either[ServerError, Either[BadRequest, NotFound]], (ApiIngest.IngestStreamInfoWithName, Option[String])],
  ] = { case (namespaceId, memberIdx, ingestStreamConfig) =>
    if (!appMethods.graph.getNamespaces.contains(namespaceId))
      Future.successful(Left(Right(Right(NotFound(s"Graph ${namespaceId.name} not found")))))
    else
      recoverServerErrorEitherWithServerError {
        appMethods.createIngestStream(ingestStreamConfig.name.value, namespaceId, ingestStreamConfig, memberIdx)
      } { case (stream, warnings) =>
        (stream, V2IngestEndpoints.warningHeader(warnings))
      }.map(_.left.map(_.map(br => Left(br): Either[BadRequest, NotFound])))(ExecutionContext.parasitic)
  }

  private val createIngestServerEndpoint: Full[
    Unit,
    Unit,
    (NamespaceId, Option[Int], Oss.QuineIngestConfiguration),
    Either[ServerError, Either[BadRequest, NotFound]],
    (ApiIngest.IngestStreamInfoWithName, Option[String]),
    Any,
    Future,
  ] = createIngest.serverLogic[Future](createIngestLogic)

  protected[endpoints] val pauseIngest: Endpoint[
    Unit,
    (NamespaceId, ResourceName, Option[Int]),
    Either[ServerError, Either[NotFound, BadRequest]],
    ApiIngest.IngestStreamInfoWithName,
    Any,
  ] = rawIngest
    .name("pause-ingest")
    .summary("Pause Ingest Stream")
    .description("Temporarily pause processing new events by the named Ingest Stream.")
    .in(ingestStreamNameWithVerb("pause"))
    .in(memberIdxParameter)
    .post
    .errorOut(notFoundError("Ingest Stream with that name does not exist."))
    .errorOutEither(badRequestError("The Ingest has failed."))
    .errorOutEither(serverError())
    .mapErrorOut(err => err.swap)(err => err.swap)
    .out(statusCode(StatusCode.Ok))
    .out(jsonBody[ApiIngest.IngestStreamInfoWithName])

  protected[endpoints] val pauseIngestLogic: ((NamespaceId, ResourceName, Option[Int])) => Future[
    Either[Either[ServerError, Either[NotFound, BadRequest]], ApiIngest.IngestStreamInfoWithName],
  ] = { case (namespaceId, ingestStreamName, maybeMemberIdx) =>
    if (!appMethods.graph.getNamespaces.contains(namespaceId))
      Future.successful(Left(Right(Left(NotFound(s"Graph ${namespaceId.name} not found")))))
    else
      recoverServerErrorEither(
        appMethods
          .pauseIngestStream(ingestStreamName.value, namespaceId, maybeMemberIdx)
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
      )(out => out)
  }

  private val pauseIngestServerEndpoint: Full[
    Unit,
    Unit,
    (NamespaceId, ResourceName, Option[Int]),
    Either[ServerError, Either[NotFound, BadRequest]],
    ApiIngest.IngestStreamInfoWithName,
    Any,
    Future,
  ] = pauseIngest.serverLogic[Future](pauseIngestLogic)

  protected[endpoints] val unpauseIngest: Endpoint[
    Unit,
    (NamespaceId, ResourceName, Option[Int]),
    Either[ServerError, Either[NotFound, BadRequest]],
    ApiIngest.IngestStreamInfoWithName,
    Any,
  ] = rawIngest
    .name("resume-ingest")
    .summary("Resume Ingest Stream")
    .description("Resume processing new events by the named Ingest Stream.")
    .in(ingestStreamNameWithVerb("resume"))
    .in(memberIdxParameter)
    .post
    .errorOut(notFoundError("Ingest Stream with that name does not exist."))
    .errorOutEither(badRequestError("The Ingest has failed."))
    .errorOutEither(serverError())
    .mapErrorOut(err => err.swap)(err => err.swap)
    .out(statusCode(StatusCode.Ok))
    .out(jsonBody[ApiIngest.IngestStreamInfoWithName])

  protected[endpoints] val unpauseIngestLogic: ((NamespaceId, ResourceName, Option[Int])) => Future[
    Either[Either[ServerError, Either[NotFound, BadRequest]], ApiIngest.IngestStreamInfoWithName],
  ] = { case (namespaceId, ingestStreamName, maybeMemberIdx) =>
    if (!appMethods.graph.getNamespaces.contains(namespaceId))
      Future.successful(Left(Right(Left(NotFound(s"Graph ${namespaceId.name} not found")))))
    else
      recoverServerErrorEither(
        appMethods.unpauseIngestStream(ingestStreamName.value, namespaceId, maybeMemberIdx).map {
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
        },
      )((id: ApiIngest.IngestStreamInfoWithName) => id)
  }

  private val unpauseIngestServerEndpoint: Full[
    Unit,
    Unit,
    (NamespaceId, ResourceName, Option[Int]),
    Either[ServerError, Either[NotFound, BadRequest]],
    ApiIngest.IngestStreamInfoWithName,
    Any,
    Future,
  ] = unpauseIngest.serverLogic[Future](unpauseIngestLogic)

  protected[endpoints] val deleteIngest: Endpoint[
    Unit,
    (NamespaceId, ResourceName, Option[Int]),
    Either[ServerError, NotFound],
    ApiIngest.IngestStreamInfoWithName,
    Any,
  ] = ingestBase
    .name("delete-ingest")
    .summary("Delete Ingest Stream")
    .description(
      "Immediately halt and remove the named Ingest Stream from Quine.\n\n" +
      """The Ingest Stream will complete any pending operations and return stream
        |information once the operation is complete.""".asOneLine,
    )
    .in(ingestStreamNameElement)
    .in(memberIdxParameter)
    .delete
    .errorOutEither(notFoundError("Ingest Stream with that name does not exist."))
    .out(statusCode(StatusCode.Ok))
    .out(jsonBody[ApiIngest.IngestStreamInfoWithName])

  protected[endpoints] val deleteIngestLogic: ((NamespaceId, ResourceName, Option[Int])) => Future[
    Either[Either[ServerError, NotFound], ApiIngest.IngestStreamInfoWithName],
  ] = { case (namespaceId, ingestStreamName, memberIdx) =>
    if (!appMethods.graph.getNamespaces.contains(namespaceId))
      Future.successful(Left(Right(NotFound(s"Graph ${namespaceId.name} not found"))))
    else
      recoverServerErrorEither(
        appMethods
          .deleteIngestStream(ingestStreamName.value, namespaceId, memberIdx)
          .map {
            case None =>
              Left(Coproduct[NotFound :+: CNil](NotFound(s"Ingest Stream $ingestStreamName does not exist")))
            case Some(streamInfo) => Right(streamInfo)
          },
      )((inp: ApiIngest.IngestStreamInfoWithName) => inp)
  }

  private val deleteIngestServerEndpoint: Full[Unit, Unit, (NamespaceId, ResourceName, Option[Int]), Either[
    ServerError,
    NotFound,
  ], ApiIngest.IngestStreamInfoWithName, Any, Future] = deleteIngest.serverLogic[Future](deleteIngestLogic)

  protected[endpoints] val ingestStatus: Endpoint[
    Unit,
    (NamespaceId, ResourceName, Option[Int]),
    Either[ServerError, NotFound],
    ApiIngest.IngestStreamInfoWithName,
    Any,
  ] = ingestBase
    .name("get-ingest-status")
    .summary("Ingest Stream Status")
    .description("Return the Ingest Stream status information for a configured Ingest Stream by name.")
    .in(ingestStreamNameElement)
    .in(memberIdxParameter)
    .get
    .errorOutEither(notFoundError("Ingest Stream with that name does not exist."))
    .out(statusCode(StatusCode.Ok))
    .out(jsonBody[ApiIngest.IngestStreamInfoWithName])

  protected[endpoints] val ingestStatusLogic: ((NamespaceId, ResourceName, Option[Int])) => Future[
    Either[Either[ServerError, NotFound], ApiIngest.IngestStreamInfoWithName],
  ] = { case (namespaceId, ingestStreamName, memberIdx) =>
    if (!appMethods.graph.getNamespaces.contains(namespaceId))
      Future.successful(Left(Right(NotFound(s"Graph ${namespaceId.name} not found"))))
    else
      recoverServerErrorEither(
        appMethods
          .ingestStreamStatus(ingestStreamName.value, namespaceId, memberIdx)
          .map {
            case None =>
              Left(
                Coproduct[NotFound :+: CNil](NotFound(s"Ingest Stream $ingestStreamName does not exist")),
              )
            case Some(streamInfo) => Right(streamInfo)
          },
      )((inp: ApiIngest.IngestStreamInfoWithName) => identity(inp))
  }

  private val ingestStatusServerEndpoint: Full[
    Unit,
    Unit,
    (NamespaceId, ResourceName, Option[Int]),
    Either[ServerError, NotFound],
    ApiIngest.IngestStreamInfoWithName,
    Any,
    Future,
  ] = ingestStatus.serverLogic[Future](ingestStatusLogic)

  protected[endpoints] val listIngest: Endpoint[
    Unit,
    (NamespaceId, Option[Int]),
    Either[ServerError, NotFound],
    Page[ApiIngest.IngestStreamInfoWithName],
    Any,
  ] =
    ingestBase
      .name("list-ingests")
      .summary("List Ingest Streams")
      .description(
        """Return a paginated list of the configured
        |[Ingest Streams](https://quine.io/learn/ingest-sources/) and their associated
        |stream metrics. `nextPageToken` is currently always empty; the envelope is in
        |place per AIP-158 so future server-side paging can be added without a breaking
        |wire change.""".asOneLine,
      )
      .in(memberIdxParameter)
      .get
      .errorOutEither(notFoundError("Graph not found."))
      .out(statusCode(StatusCode.Ok))
      .out(jsonBody[Page[ApiIngest.IngestStreamInfoWithName]])

  protected[endpoints] val listIngestLogic: ((NamespaceId, Option[Int])) => Future[
    Either[Either[ServerError, NotFound], Page[ApiIngest.IngestStreamInfoWithName]],
  ] = { case (namespaceId, memberIdx) =>
    recoverServerErrorEitherFlat(appMethods.listIngestStreams(namespaceId, memberIdx))(Page.of(_))
  }

  private val listIngestServerEndpoint: Full[
    Unit,
    Unit,
    (NamespaceId, Option[Int]),
    Either[ServerError, NotFound],
    Page[ApiIngest.IngestStreamInfoWithName],
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
