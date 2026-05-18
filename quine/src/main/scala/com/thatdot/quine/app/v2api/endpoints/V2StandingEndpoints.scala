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
import com.thatdot.quine.app.v2api.definitions.query.standing.StandingQuery._
import com.thatdot.quine.app.v2api.definitions.query.standing.StandingQueryPattern.StandingQueryMode.MultipleValues
import com.thatdot.quine.app.v2api.definitions.query.standing.StandingQueryPattern._
import com.thatdot.quine.app.v2api.definitions.query.standing.StandingQueryResultWorkflow
import com.thatdot.quine.app.v2api.definitions.{GraphScopedEndpoints, V2QuineEndpointDefinitions}
import com.thatdot.quine.graph.NamespaceId

trait V2StandingEndpoints extends V2QuineEndpointDefinitions with GraphScopedEndpoints with StringOps {

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
  private val rawStandingQuery: Endpoint[Unit, NamespaceId, Nothing, Unit, Any] =
    graphScopedEndpoint("standingQueries")
      .tag("Standing Queries")

  private val standingQueryBase: Endpoint[Unit, NamespaceId, ServerError, Unit, Any] =
    rawStandingQuery.errorOut(serverError())

  private val propagateBase: Endpoint[Unit, NamespaceId, ServerError, Unit, Any] =
    graphScopedEndpoint("standingQueries:propagate")
      .tag("Standing Queries")
      .errorOut(serverError())

  protected[endpoints] val listStandingQueries
    : Endpoint[Unit, NamespaceId, Either[ServerError, NotFound], Page[RegisteredStandingQuery], Any] =
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
      .get
      .errorOutEither(notFoundError("Graph not found."))
      .out(statusCode(StatusCode.Ok))
      .out(jsonBody[Page[RegisteredStandingQuery]])

  protected[endpoints] val listStandingQueriesLogic
    : NamespaceId => Future[Either[Either[ServerError, NotFound], Page[RegisteredStandingQuery]]] =
    namespaceId => recoverServerErrorEitherFlat(appMethods.listStandingQueries(namespaceId))(Page.of(_))

  private val listStandingQueriesServerEndpoint: Full[
    Unit,
    Unit,
    NamespaceId,
    Either[ServerError, NotFound],
    Page[RegisteredStandingQuery],
    Any,
    Future,
  ] = listStandingQueries.serverLogic[Future](listStandingQueriesLogic)

  protected[endpoints] val propagateStandingQuery: Endpoint[
    Unit,
    (NamespaceId, Boolean, Int),
    Either[ServerError, NotFound],
    Unit,
    Any,
  ] =
    propagateBase
      .name("propagate-standing-queries")
      .summary("Propagate Standing Queries")
      .description(
        """Applies all currently-registered Standing Queries to data already in the graph. By default,
          |a Standing Query only matches data ingested after the query is registered; this endpoint
          |backfills it against earlier data.""".asOneLine + "\n\n" +
        """Useful when interactively constructing a Standing Query, or when a newly-registered query
          |must match recent history.""".asOneLine,
      )
      .in(
        query[Boolean]("includeSleeping")
          .default(false)
          .description(
            """If false, only nodes currently in the in-memory cache are evaluated. If true, sleeping
              |nodes are also woken and evaluated — this can be expensive on large graphs.""".asOneLine,
          ),
      )
      .in(query[Int]("wakeUpParallelism").default(4).validate(Validator.positive))
      .errorOutEither(notFoundError("Graph not found."))
      .out(statusCode(StatusCode.Accepted))
      .post

  protected[endpoints] val propagateStandingQueryLogic
    : ((NamespaceId, Boolean, Int)) => Future[Either[Either[ServerError, NotFound], Unit]] = {
    case (namespaceId, includeSleeping, wakeUpParallelism) =>
      recoverServerErrorEitherFlat(
        appMethods.propagateStandingQuery(includeSleeping, namespaceId, wakeUpParallelism),
      )((_: Unit) => ())
  }

  private val propagateStandingQueryServerEndpoint: Full[
    Unit,
    Unit,
    (NamespaceId, Boolean, Int),
    Either[ServerError, NotFound],
    Unit,
    Any,
    Future,
  ] = propagateStandingQuery.serverLogic[Future](propagateStandingQueryLogic)

  protected[endpoints] val addSQOutputWorkflow
    : Endpoint[Unit, (NamespaceId, String, StandingQueryResultWorkflow), Either[
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
      NamespaceId,
      String,
      StandingQueryResultWorkflow,
    ),
  ) => Future[Either[
    Either[ServerError, Either[BadRequest, NotFound]],
    Unit,
  ]] = { case (namespaceId, sqName, workflow) =>
    recoverServerErrorEither(
      appMethods
        .addSQOutput(sqName, workflow.name, namespaceId, workflow),
    )(identity)
  }

  private val addSQOutputWorkflowServerEndpoint: Full[
    Unit,
    Unit,
    (NamespaceId, String, StandingQueryResultWorkflow),
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
    (NamespaceId, Boolean, StandingQueryDefinition),
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

  protected[endpoints] val createSQLogic: ((NamespaceId, Boolean, StandingQueryDefinition)) => Future[
    Either[Either[ServerError, Either[BadRequest, NotFound]], RegisteredStandingQuery],
  ] = { case (namespaceId, shouldCalculateResultHashCode, definition) =>
    recoverServerErrorEither(
      appMethods
        .createSQ(definition.name, namespaceId, shouldCalculateResultHashCode, definition),
    )(identity)
  }

  private val createSQServerEndpoint: Full[
    Unit,
    Unit,
    (NamespaceId, Boolean, StandingQueryDefinition),
    Either[ServerError, Either[BadRequest, NotFound]],
    RegisteredStandingQuery,
    Any,
    Future,
  ] = createSQ.serverLogic[Future](createSQLogic)

  protected[endpoints] val deleteSQ: Endpoint[Unit, (NamespaceId, String), Either[
    ServerError,
    NotFound,
  ], RegisteredStandingQuery, Any] =
    standingQueryBase
      .name("delete-standing-query")
      .summary("Delete Standing Query")
      .description("Immediately halt and remove the named Standing Query from Quine.")
      .in(sqName)
      .delete
      .out(statusCode(StatusCode.Ok))
      .out(jsonBody[RegisteredStandingQuery])
      .errorOutEither(notFoundError("No Standing Queries exist with the provided name."))

  protected[endpoints] val deleteSQLogic: ((NamespaceId, String)) => Future[
    Either[Either[ServerError, NotFound], RegisteredStandingQuery],
  ] = { case (namespaceId, standingQueryName) =>
    recoverServerErrorEitherFlat(appMethods.deleteSQ(standingQueryName, namespaceId))(
      identity,
    )
  }

  private val deleteSQServerEndpoint: Full[
    Unit,
    Unit,
    (NamespaceId, String),
    Either[ServerError, ErrorResponse.NotFound],
    RegisteredStandingQuery,
    Any,
    Future,
  ] = deleteSQ.serverLogic[Future](deleteSQLogic)

  protected[endpoints] val deleteSQOutput: Endpoint[
    Unit,
    (NamespaceId, String, String),
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
      .delete
      .out(statusCode(StatusCode.Ok))
      .out(jsonBody[StandingQueryResultWorkflow])
      .errorOutEither(notFoundError("No Standing Queries exist with the provided name."))

  protected[endpoints] val deleteSQOutputLogic: ((NamespaceId, String, String)) => Future[
    Either[Either[ServerError, NotFound], StandingQueryResultWorkflow],
  ] = { case (namespaceId, sqName, sqOutputName) =>
    recoverServerErrorEitherFlat(appMethods.deleteSQOutput(sqName, sqOutputName, namespaceId))(
      identity,
    )
  }

  private val deleteSQOutputServerEndpoint: Full[
    Unit,
    Unit,
    (NamespaceId, String, String),
    Either[ServerError, ErrorResponse.NotFound],
    StandingQueryResultWorkflow,
    Any,
    Future,
  ] = deleteSQOutput.serverLogic[Future](deleteSQOutputLogic)

  protected[endpoints] val getSq: Endpoint[
    Unit,
    (NamespaceId, String),
    Either[ServerError, NotFound],
    RegisteredStandingQuery,
    Any,
  ] = standingQueryBase
    .name("get-standing-query-status")
    .summary("Standing Query Status")
    .description("Return the status information for a configured Standing Query by name.")
    .in(sqName)
    .get
    .out(statusCode(StatusCode.Ok))
    .out(jsonBody[RegisteredStandingQuery])
    .errorOutEither(notFoundError("No Standing Queries exist with the provided name."))

  protected[endpoints] val getSqLogic: ((NamespaceId, String)) => Future[
    Either[Either[ServerError, NotFound], RegisteredStandingQuery],
  ] = { case (namespaceId, sqName) =>
    recoverServerErrorEitherFlat(appMethods.getSQ(sqName, namespaceId))(
      identity,
    )
  }

  private val getSqServerEndpoint: Full[
    Unit,
    Unit,
    (NamespaceId, String),
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
