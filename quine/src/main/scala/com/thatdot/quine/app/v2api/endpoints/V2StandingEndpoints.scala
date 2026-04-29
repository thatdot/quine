package com.thatdot.quine.app.v2api.endpoints

import scala.concurrent.Future

import sttp.model.StatusCode
import sttp.tapir.server.ServerEndpoint
import sttp.tapir.server.ServerEndpoint.Full
import sttp.tapir.{Endpoint, EndpointInput, Validator, emptyOutputAs, path, query, statusCode}

import com.thatdot.api.v2.ErrorResponse.{BadRequest, NotFound, ServerError}
import com.thatdot.api.v2.ErrorResponseHelpers.{badRequestError, notFoundError, serverError}
import com.thatdot.api.v2.{ErrorResponse, Page}
import com.thatdot.quine.app.util.StringOps
import com.thatdot.quine.app.v2api.definitions.V2QuineEndpointDefinitions
import com.thatdot.quine.app.v2api.definitions.query.standing.StandingQuery._
import com.thatdot.quine.app.v2api.definitions.query.standing.StandingQueryPattern.StandingQueryMode.MultipleValues
import com.thatdot.quine.app.v2api.definitions.query.standing.StandingQueryPattern._
import com.thatdot.quine.app.v2api.definitions.query.standing.StandingQueryResultWorkflow
import com.thatdot.quine.routes.exts.NamespaceParameter

trait V2StandingEndpoints extends V2QuineEndpointDefinitions with StringOps {

  /** SQ Name path element */
  val sqName: EndpointInput.PathCapture[String] =
    path[String]("standingQueryName").description("Unique name for a Standing Query.")

  /** SQ Output Name path element */
  private val sqOutputName: EndpointInput.PathCapture[String] =
    path[String]("standingQueryOutputName").description("Unique name for a Standing Query Output.")

  // We could consolidate `rawStandingQuery` with `standingQueryBase,` above, by;
  // 1. Replace `rawStandingQuery` uses with `standingQueryBase`
  // 2. Removing the `.errorOut*(serverError())` builder call
  // 3. Adjusting the `.errorOut*` calls or their dependencies to accommodate the new expected ERROR_OUTPUT
  // 4. Inline `rawStandingQuery` implementation into `standingQueryBase`
  // But, FYI, step 3 is not immediately straightforward
  private val rawStandingQuery: Endpoint[Unit, Unit, Nothing, Unit, Any] =
    rawEndpoint("standingQueries")
      .tag("Standing Queries")

  private val standingQueryBase: EndpointBase = rawStandingQuery.errorOut(serverError())

  private val propagateBase: EndpointBase =
    rawEndpoint("standingQueries:propagate")
      .tag("Standing Queries")
      .errorOut(serverError())

  protected[endpoints] val listStandingQueries
    : Endpoint[Unit, Option[NamespaceParameter], ServerError, Page[RegisteredStandingQuery], Any] =
    standingQueryBase
      .name("list-standing-queries")
      .summary("List Standing Queries")
      .description(
        """Individual Standing Queries are issued into the graph one time;
          |result outputs are produced as new data is written into Quine and matches are found.""".asOneLine + "\n\n" +
        """Compared to traditional queries, Standing Queries are less imperative
          |and more declarative — it doesn't matter in what order the parts of the pattern match,
          |only that the composite structure exists.""".asOneLine + "\n\n" +
        """Learn more about writing [Standing Queries](https://quine.io/learn/standing-queries/standing-queries/)
          |in the docs.""".asOneLine + "\n\n" +
        """Returns a paginated envelope per AIP-158. `nextPageToken` is currently always
          |empty; the wrapper is in place so future server-side paging can be added without
          |a breaking wire change.""".asOneLine,
      )
      .in(namespaceParameter)
      .get
      .out(statusCode(StatusCode.Ok))
      .out(jsonBody[Page[RegisteredStandingQuery]])

  protected[endpoints] val listStandingQueriesLogic
    : Option[NamespaceParameter] => Future[Either[ServerError, Page[RegisteredStandingQuery]]] =
    namespace => recoverServerError(appMethods.listStandingQueries(namespaceFromParam(namespace)))(Page.of(_))

  private val listStandingQueriesServerEndpoint: Full[
    Unit,
    Unit,
    Option[NamespaceParameter],
    ServerError,
    Page[RegisteredStandingQuery],
    Any,
    Future,
  ] = listStandingQueries.serverLogic[Future](listStandingQueriesLogic)

  protected[endpoints] val propagateStandingQuery: Endpoint[
    Unit,
    (Boolean, Option[NamespaceParameter], Int),
    ServerError,
    Unit,
    Any,
  ] =
    propagateBase
      .name("propagate-standing-queries")
      .summary("Propagate Standing Queries")
      .description(
        """When a new Standing Query is registered in the system, it gets automatically
          |registered on new nodes (or old nodes that are loaded back into the cache). This behavior
          |is the default because pro-actively setting the Standing Query on all
          |existing data might be quite costly depending on how much historical data there is.""".asOneLine + "\n\n" +
        """However, sometimes there is a legitimate use-case for eagerly propagating standing queries across the graph, for instance:
          |
          |  * When interactively constructing a Standing Query for already-ingested data
          |  * When creating a new Standing Query that needs to be applied to recent data""".stripMargin,
      )
      .in(
        query[Boolean]("includeSleeping")
          .default(false)
          .description("Propagate to all sleeping nodes. Setting to true can be costly if there is lot of data."),
      )
      .in(namespaceParameter)
      .in(query[Int]("wakeUpParallelism").default(4).validate(Validator.positive))
      .out(statusCode(StatusCode.Accepted))
      .post

  protected[endpoints] val propagateStandingQueryLogic
    : ((Boolean, Option[NamespaceParameter], Int)) => Future[Either[ServerError, Unit]] = {
    case (includeSleeping, namespace, wakeUpParallelism) =>
      recoverServerError(
        appMethods.propagateStandingQuery(includeSleeping, namespaceFromParam(namespace), wakeUpParallelism),
      )((_: Unit) => ())
  }

  private val propagateStandingQueryServerEndpoint: Full[
    Unit,
    Unit,
    (Boolean, Option[NamespaceParameter], Int),
    ServerError,
    Unit,
    Any,
    Future,
  ] = propagateStandingQuery.serverLogic[Future](propagateStandingQueryLogic)

  protected[endpoints] val addSQOutputWorkflow
    : Endpoint[Unit, (String, Option[NamespaceParameter], StandingQueryResultWorkflow), Either[
      ServerError,
      Either[BadRequest, NotFound],
    ], Unit, Any] = rawStandingQuery
    .name("create-standing-query-output")
    .summary("Create Standing Query Output")
    .description(
      "Each Standing Query can have any number of destinations to which `StandingQueryResults` will be routed.",
    )
    .in(sqName)
    .in("outputs")
    .in(namespaceParameter)
    .in(jsonOrYamlBody[StandingQueryResultWorkflow](Some(StandingQueryResultWorkflow.exampleToStandardOut)))
    .errorOut(badRequestError("Output is invalid.", "There is another output with that name already."))
    .errorOutEither(notFoundError("No Standing Queries exist with the provided name."))
    .errorOutEither(serverError())
    .mapErrorOut(err => err.swap)(err => err.swap)
    .out(statusCode(StatusCode.Created))
    .out(emptyOutputAs[Unit](()))
    .post

  protected[endpoints] val addSQOutputWorkflowLogic: (
    (
      String,
      Option[NamespaceParameter],
      StandingQueryResultWorkflow,
    ),
  ) => Future[Either[
    Either[ServerError, Either[BadRequest, NotFound]],
    Unit,
  ]] = { case (sqName, namespace, workflow) =>
    recoverServerErrorEither(
      appMethods
        .addSQOutput(sqName, workflow.name, namespaceFromParam(namespace), workflow),
    )(identity)
  }

  private val addSQOutputWorkflowServerEndpoint: Full[
    Unit,
    Unit,
    (String, Option[NamespaceParameter], StandingQueryResultWorkflow),
    Either[ServerError, Either[BadRequest, NotFound]],
    Unit,
    Any,
    Future,
  ] = addSQOutputWorkflow.serverLogic[Future](addSQOutputWorkflowLogic)

  private val exPattern = "MATCH (n) WHERE n.num % 100 = 0 RETURN n.num"

  private val createSqExample: StandingQueryDefinition = StandingQueryDefinition(
    name = "example-standing-query",
    pattern = Cypher(exPattern, MultipleValues),
    outputs = StandingQueryResultWorkflow.examples,
  )

  protected[endpoints] val createSQ: Endpoint[
    Unit,
    (Option[NamespaceParameter], Boolean, StandingQueryDefinition),
    Either[ServerError, Either[BadRequest, NotFound]],
    RegisteredStandingQuery,
    Any,
  ] = rawStandingQuery
    .name("create-standing-query")
    .summary("Create Standing Query")
    .description(
      """Individual Standing Queries are issued into the graph one time;
        |result outputs are produced as new data is written into Quine and matches are found.""".asOneLine + "\n\n" +
      """Compared to traditional queries, Standing Queries are less imperative
        |and more declarative - it doesn't matter what order parts of the pattern match,
        |only that the composite structure exists.""".asOneLine + "\n\n" +
      """Learn more about writing [Standing Queries](https://quine.io/learn/standing-queries/standing-queries/)
        |in the docs.""".asOneLine,
    )
    .in(namespaceParameter)
    .in(
      query[Boolean]("shouldCalculateResultHashCode")
        .description("For debug and test only.")
        .default(false)
        .schema(_.hidden(true)),
    )
    .in(jsonOrYamlBody[StandingQueryDefinition](Some(createSqExample)))
    .post
    .errorOut(
      badRequestError("A Standing Query with that name already exists.", "There is an issue with the query."),
    )
    .errorOutEither(notFoundError())
    .errorOutEither(serverError())
    .mapErrorOut(err => err.swap)(err => err.swap)
    .out(statusCode(StatusCode.Created))
    .out(jsonBody[RegisteredStandingQuery])

  protected[endpoints] val createSQLogic: ((Option[NamespaceParameter], Boolean, StandingQueryDefinition)) => Future[
    Either[Either[ServerError, Either[BadRequest, NotFound]], RegisteredStandingQuery],
  ] = { case (namespace, shouldCalculateResultHashCode, definition) =>
    recoverServerErrorEither(
      appMethods
        .createSQ(definition.name, namespaceFromParam(namespace), shouldCalculateResultHashCode, definition),
    )(identity)
  }

  private val createSQServerEndpoint: Full[
    Unit,
    Unit,
    (Option[NamespaceParameter], Boolean, StandingQueryDefinition),
    Either[ServerError, Either[BadRequest, NotFound]],
    RegisteredStandingQuery,
    Any,
    Future,
  ] = createSQ.serverLogic[Future](createSQLogic)

  protected[endpoints] val deleteSQ: Endpoint[Unit, (String, Option[NamespaceParameter]), Either[
    ServerError,
    NotFound,
  ], RegisteredStandingQuery, Any] =
    standingQueryBase
      .name("delete-standing-query")
      .summary("Delete Standing Query")
      .description("Immediately halt and remove the named Standing Query from Quine.")
      .in(sqName)
      .in(namespaceParameter)
      .delete
      .out(statusCode(StatusCode.Ok))
      .out(jsonBody[RegisteredStandingQuery])
      .errorOutEither(notFoundError("No Standing Queries exist with the provided name."))

  protected[endpoints] val deleteSQLogic: ((String, Option[NamespaceParameter])) => Future[
    Either[Either[ServerError, NotFound], RegisteredStandingQuery],
  ] = { case (standingQueryName, namespace) =>
    recoverServerErrorEitherFlat(appMethods.deleteSQ(standingQueryName, namespaceFromParam(namespace)))(
      identity,
    )
  }

  private val deleteSQServerEndpoint: Full[
    Unit,
    Unit,
    (String, Option[NamespaceParameter]),
    Either[ServerError, ErrorResponse.NotFound],
    RegisteredStandingQuery,
    Any,
    Future,
  ] = deleteSQ.serverLogic[Future](deleteSQLogic)

  protected[endpoints] val deleteSQOutput: Endpoint[
    Unit,
    (String, String, Option[NamespaceParameter]),
    Either[ServerError, NotFound],
    StandingQueryResultWorkflow,
    Any,
  ] =
    standingQueryBase
      .name("delete-standing-query-output")
      .summary("Delete Standing Query Output")
      .description("Remove an output from a Standing Query.")
      .in(sqName)
      .in("outputs")
      .in(sqOutputName)
      .in(namespaceParameter)
      .delete
      .out(statusCode(StatusCode.Ok))
      .out(jsonBody[StandingQueryResultWorkflow])
      .errorOutEither(notFoundError("No Standing Queries exist with the provided name."))

  protected[endpoints] val deleteSQOutputLogic: ((String, String, Option[NamespaceParameter])) => Future[
    Either[Either[ServerError, NotFound], StandingQueryResultWorkflow],
  ] = { case (sqName, sqOutputName, namespace) =>
    recoverServerErrorEitherFlat(appMethods.deleteSQOutput(sqName, sqOutputName, namespaceFromParam(namespace)))(
      identity,
    )
  }

  private val deleteSQOutputServerEndpoint: Full[
    Unit,
    Unit,
    (String, String, Option[NamespaceParameter]),
    Either[ServerError, ErrorResponse.NotFound],
    StandingQueryResultWorkflow,
    Any,
    Future,
  ] = deleteSQOutput.serverLogic[Future](deleteSQOutputLogic)

  protected[endpoints] val getSq: Endpoint[
    Unit,
    (String, Option[NamespaceParameter]),
    Either[ServerError, NotFound],
    RegisteredStandingQuery,
    Any,
  ] = standingQueryBase
    .name("get-standing-query-status")
    .summary("Standing Query Status")
    .description("Return the status information for a configured Standing Query by name.")
    .in(sqName)
    .in(namespaceParameter)
    .get
    .out(statusCode(StatusCode.Ok))
    .out(jsonBody[RegisteredStandingQuery])
    .errorOutEither(notFoundError("No Standing Queries exist with the provided name."))

  protected[endpoints] val getSqLogic: ((String, Option[NamespaceParameter])) => Future[
    Either[Either[ServerError, NotFound], RegisteredStandingQuery],
  ] = { case (sqName, namespace) =>
    recoverServerErrorEitherFlat(appMethods.getSQ(sqName, namespaceFromParam(namespace)))(
      identity,
    )
  }

  private val getSqServerEndpoint: Full[
    Unit,
    Unit,
    (String, Option[NamespaceParameter]),
    Either[ServerError, ErrorResponse.NotFound],
    RegisteredStandingQuery,
    Any,
    Future,
  ] = getSq.serverLogic[Future](getSqLogic)

  val standingQueryEndpoints: List[ServerEndpoint[Any, Future]] = List(
    listStandingQueriesServerEndpoint,
    getSqServerEndpoint,
    createSQServerEndpoint,
    propagateStandingQueryServerEndpoint,
    addSQOutputWorkflowServerEndpoint,
    deleteSQServerEndpoint,
    deleteSQOutputServerEndpoint,
  )

}
