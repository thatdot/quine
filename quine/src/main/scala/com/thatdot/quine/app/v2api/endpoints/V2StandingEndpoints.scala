package com.thatdot.quine.app.v2api.endpoints

import scala.concurrent.Future

import sttp.model.StatusCode
import sttp.tapir.server.ServerEndpoint
import sttp.tapir.server.ServerEndpoint.Full
import sttp.tapir.{Endpoint, EndpointInput, Validator, emptyOutputAs, path, query, statusCode}

import com.thatdot.api.v2.ErrorResponse.{BadRequest, NotFound, ServerError}
import com.thatdot.api.v2.ErrorResponseHelpers.{badRequestError, notFoundError, serverError}
import com.thatdot.api.v2.{ErrorResponse, Page, ResourceName}
import com.thatdot.quine.app.util.StringOps
import com.thatdot.quine.app.v2api.definitions.query.standing.StandingQuery._
import com.thatdot.quine.app.v2api.definitions.query.standing.StandingQueryPattern.StandingQueryMode.MultipleValues
import com.thatdot.quine.app.v2api.definitions.query.standing.StandingQueryPattern._
import com.thatdot.quine.app.v2api.definitions.query.standing.{PropagateTo, StandingQueryResultWorkflow}
import com.thatdot.quine.app.v2api.definitions.{GraphScopedEndpoints, V2QuineEndpointDefinitions}
import com.thatdot.quine.graph.NamespaceId

trait V2StandingEndpoints extends V2QuineEndpointDefinitions with GraphScopedEndpoints with StringOps {

  /** SQ Name path element */
  val sqName: EndpointInput.PathCapture[ResourceName] =
    path[ResourceName]("standingQueryName").description("Unique name for a Standing Query.")

  /** SQ Output Name path element */
  private val sqOutputName: EndpointInput.PathCapture[ResourceName] =
    path[ResourceName]("standingQueryOutputName").description("Unique name for a Standing Query Output.")

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
          |must match recent history.""".asOneLine + "\n\n" +
        """Propagation applies to all currently-registered Standing Queries, not only a single one.
          |Propagating multiple Standing Queries has essentially the same cost as propagating
          |one, because the expensive part is visiting nodes, not checking individual Standing
          |Query registrations. If you are creating multiple Standing Queries, create them all
          |first (with `propagateTo=NONE` on the create endpoint), then call this endpoint once
          |to propagate all Standing Queries in a single pass.""".asOneLine,
      )
      .in(
        query[Boolean]("includeSleeping")
          .default(false)
          .description(
            """If false, only nodes currently in the in-memory cache are evaluated. If true, sleeping
              |nodes are also woken and evaluated. This is significantly more expensive because it
              |requires disk and network IO to iterate through and wake sleeping nodes in the
              |background.""".asOneLine,
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
    : Endpoint[Unit, (NamespaceId, ResourceName, StandingQueryResultWorkflow), Either[
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
      ResourceName,
      StandingQueryResultWorkflow,
    ),
  ) => Future[Either[
    Either[ServerError, Either[BadRequest, NotFound]],
    Unit,
  ]] = { case (namespaceId, sqName, workflow) =>
    recoverServerErrorEither(
      appMethods
        .addSQOutput(sqName.value, workflow.name.value, namespaceId, workflow),
    )(identity)
  }

  private val addSQOutputWorkflowServerEndpoint: Full[
    Unit,
    Unit,
    (NamespaceId, ResourceName, StandingQueryResultWorkflow),
    Either[ServerError, Either[BadRequest, NotFound]],
    Unit,
    Any,
    Future,
  ] = addSQOutputWorkflow.serverLogic[Future](addSQOutputWorkflowLogic)

  private val exPattern = "MATCH (n) WHERE n.num % 100 = 0 RETURN n.num"

  private val createSqExample: StandingQueryDefinition = StandingQueryDefinition(
    name = ResourceName.unsafeFromString("example-standing-query"),
    pattern = Cypher(exPattern, MultipleValues),
    outputs = StandingQueryResultWorkflow.examples,
  )

  protected[endpoints] val createSQ: Endpoint[
    Unit,
    (NamespaceId, Boolean, PropagateTo, Int, StandingQueryDefinition),
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
    .in(
      query[PropagateTo]("propagateTo")
        .default(PropagateTo.ExcludeSleeping)
        .description(
          """Controls whether all registered Standing Queries are propagated to existing graph
            |data after creation. Without propagation, a Standing Query only matches data changed
            |after it is registered; propagation backfills it against existing data.
            |Defaults to `EXCLUDE_SLEEPING`.""".asOneLine + "\n\n" +
          """`NONE`: no propagation. The query only applies to nodes awoken and changed after Standing Query registration.""" + "\n\n" +
          """`EXCLUDE_SLEEPING` (default): propagates to nodes currently in the in-memory cache only.
            |This is relatively inexpensive.""".asOneLine + "\n\n" +
          """`INCLUDE_SLEEPING`: propagates to all nodes, including sleeping nodes that must be
            |woken from the persistor. This is significantly more expensive than `EXCLUDE_SLEEPING`
            |because it requires disk and network IO to iterate through and all wake sleeping nodes
            |in the background. Use `wakeUpParallelism` to control how many
            |nodes are woken at a time. Higher parallelism will iterate faster, but will also backpressure 
            |ingest more than lower parallelism.""".asOneLine + "\n\n" +
          """Propagation applies all currently-registered Standing Queries, not only the
            |newly-created one. Propagating for multiple Standing Queries has essentially the
            |same cost as propagating for one. If you are creating multiple Standing Queries,
            |create them all with `propagateTo=NONE` first, then use the
            |`propagate-standing-queries` endpoint to propagate them all in a single pass.""".asOneLine,
        ),
    )
    .in(
      query[Int]("wakeUpParallelism")
        .default(4)
        .validate(Validator.positive)
        .description(
          """Number of sleeping nodes to wake in parallel when `propagateTo` is
            |`INCLUDE_SLEEPING`.""".asOneLine,
        ),
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

  protected[endpoints] val createSQLogic: ((NamespaceId, Boolean, PropagateTo, Int, StandingQueryDefinition)) => Future[
    Either[Either[ServerError, Either[BadRequest, NotFound]], RegisteredStandingQuery],
  ] = { case (namespaceId, shouldCalculateResultHashCode, propagateTo, wakeUpParallelism, definition) =>
    val effectivePropagateTo = propagateTo match {
      case PropagateTo.IncludeSleeping(_) => PropagateTo.IncludeSleeping(wakeUpParallelism)
      case other => other
    }
    recoverServerErrorEither(
      appMethods
        .createSQ(
          definition.name.value,
          namespaceId,
          shouldCalculateResultHashCode,
          definition,
          effectivePropagateTo,
        ),
    )(identity)
  }

  private val createSQServerEndpoint: Full[
    Unit,
    Unit,
    (NamespaceId, Boolean, PropagateTo, Int, StandingQueryDefinition),
    Either[ServerError, Either[BadRequest, NotFound]],
    RegisteredStandingQuery,
    Any,
    Future,
  ] = createSQ.serverLogic[Future](createSQLogic)

  protected[endpoints] val deleteSQ: Endpoint[Unit, (NamespaceId, ResourceName), Either[
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

  protected[endpoints] val deleteSQLogic: ((NamespaceId, ResourceName)) => Future[
    Either[Either[ServerError, NotFound], RegisteredStandingQuery],
  ] = { case (namespaceId, standingQueryName) =>
    recoverServerErrorEitherFlat(appMethods.deleteSQ(standingQueryName.value, namespaceId))(
      identity,
    )
  }

  private val deleteSQServerEndpoint: Full[
    Unit,
    Unit,
    (NamespaceId, ResourceName),
    Either[ServerError, ErrorResponse.NotFound],
    RegisteredStandingQuery,
    Any,
    Future,
  ] = deleteSQ.serverLogic[Future](deleteSQLogic)

  protected[endpoints] val deleteSQOutput: Endpoint[
    Unit,
    (NamespaceId, ResourceName, ResourceName),
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

  protected[endpoints] val deleteSQOutputLogic: ((NamespaceId, ResourceName, ResourceName)) => Future[
    Either[Either[ServerError, NotFound], StandingQueryResultWorkflow],
  ] = { case (namespaceId, sqName, sqOutputName) =>
    recoverServerErrorEitherFlat(appMethods.deleteSQOutput(sqName.value, sqOutputName.value, namespaceId))(
      identity,
    )
  }

  private val deleteSQOutputServerEndpoint: Full[
    Unit,
    Unit,
    (NamespaceId, ResourceName, ResourceName),
    Either[ServerError, ErrorResponse.NotFound],
    StandingQueryResultWorkflow,
    Any,
    Future,
  ] = deleteSQOutput.serverLogic[Future](deleteSQOutputLogic)

  protected[endpoints] val getSq: Endpoint[
    Unit,
    (NamespaceId, ResourceName),
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

  protected[endpoints] val getSqLogic: ((NamespaceId, ResourceName)) => Future[
    Either[Either[ServerError, NotFound], RegisteredStandingQuery],
  ] = { case (namespaceId, sqName) =>
    recoverServerErrorEitherFlat(appMethods.getSQ(sqName.value, namespaceId))(
      identity,
    )
  }

  private val getSqServerEndpoint: Full[
    Unit,
    Unit,
    (NamespaceId, ResourceName),
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
