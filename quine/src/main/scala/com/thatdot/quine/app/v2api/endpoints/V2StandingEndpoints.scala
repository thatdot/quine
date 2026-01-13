package com.thatdot.quine.app.v2api.endpoints

import scala.concurrent.Future

import sttp.model.StatusCode
import sttp.tapir.server.ServerEndpoint
import sttp.tapir.server.ServerEndpoint.Full
import sttp.tapir.{Endpoint, EndpointInput, emptyOutputAs, path, query, statusCode}

import com.thatdot.api.v2.ErrorResponse.{BadRequest, NotFound, ServerError}
import com.thatdot.api.v2.ErrorResponseHelpers.{badRequestError, notFoundError, serverError}
import com.thatdot.api.v2.{ErrorResponse, SuccessEnvelope}
import com.thatdot.quine.app.util.StringOps
import com.thatdot.quine.app.v2api.definitions.V2QuineEndpointDefinitions
import com.thatdot.quine.app.v2api.definitions.query.standing.StandingQuery._
import com.thatdot.quine.app.v2api.definitions.query.standing.StandingQueryPattern.StandingQueryMode.MultipleValues
import com.thatdot.quine.app.v2api.definitions.query.standing.StandingQueryPattern._
import com.thatdot.quine.app.v2api.definitions.query.standing.StandingQueryResultWorkflow

trait V2StandingEndpoints extends V2QuineEndpointDefinitions with StringOps {

  /** SQ Name path element */
  val sqName: EndpointInput.PathCapture[String] =
    path[String]("standing-query-name").description("Unique name for a Standing Query.")

  /** SQ Output Name path element */
  private val sqOutputName: EndpointInput.PathCapture[String] =
    path[String]("standing-query-output-name").description("Unique name for a Standing Query Output.")

  // We could consolidate `rawStandingQuery` with `standingQueryBase,` above, by;
  // 1. Replace `rawStandingQuery` uses with `standingQueryBase`
  // 2. Removing the `.errorOut*(serverError())` builder call
  // 3. Adjusting the `.errorOut*` calls or their dependencies to accommodate the new expected ERROR_OUTPUT
  // 4. Inline `rawStandingQuery` implementation into `standingQueryBase`
  // But, FYI, step 3 is not immediately straightforward
  private val rawStandingQuery: Endpoint[Unit, Unit, Nothing, Unit, Any] =
    rawEndpoint("standing-queries")
      .tag("Standing Queries")

  private val standingQueryBase: EndpointBase = rawStandingQuery.errorOut(serverError())

  protected[endpoints] val listStandingQueries
    : Endpoint[Unit, Option[String], ServerError, SuccessEnvelope.Ok[List[RegisteredStandingQuery]], Any] =
    standingQueryBase
      .name("list-standing-queries")
      .summary("List Standing Queries")
      .description(
        """Individual Standing Queries are issued into the graph one time;
          |result outputs are produced as new data is written into Quine and matches are found.""".asOneLine + "\n\n" +
        """Compared to traditional queries, Standing Queries are less imperative
          |and more declarative — it doesn't matter in what order the parts of the pattern match,
          |only that the composite structure exists.""".asOneLine + "\n\n" +
        """Learn more about writing [Standing Queries](https://docs.quine.io/components/writing-standing-queries.html)
          |in the docs.""".asOneLine,
      )
      .in(namespaceParameter)
      .get
      .out(statusCode(StatusCode.Ok))
      .out(jsonBody[SuccessEnvelope.Ok[List[RegisteredStandingQuery]]])

  protected[endpoints] val listStandingQueriesLogic
    : Option[String] => Future[Either[ServerError, SuccessEnvelope.Ok[List[RegisteredStandingQuery]]]] = namespace =>
    recoverServerError(appMethods.listStandingQueries(namespaceFromParam(namespace)))(
      (inp: List[RegisteredStandingQuery]) => SuccessEnvelope.Ok(inp),
    )

  private val listStandingQueriesServerEndpoint: Full[
    Unit,
    Unit,
    Option[String],
    ServerError,
    SuccessEnvelope.Ok[List[RegisteredStandingQuery]],
    Any,
    Future,
  ] = listStandingQueries.serverLogic[Future](listStandingQueriesLogic)

  protected[endpoints] val propagateStandingQuery: Endpoint[
    Unit,
    (Boolean, Option[String], Int),
    ServerError,
    SuccessEnvelope.Accepted,
    Any,
  ] =
    standingQueryBase
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
      .in("control")
      .in("propagate")
      .in(
        query[Boolean]("include-sleeping")
          .default(false)
          .description("Propagate to all sleeping nodes. Setting to true can be costly if there is lot of data."),
      )
      .in(namespaceParameter)
      .in(query[Int]("wake-up-parallelism").default(4))
      .out(statusCode(StatusCode.Accepted))
      .out(jsonBody[SuccessEnvelope.Accepted])
      .put

  protected[endpoints] val propagateStandingQueryLogic
    : ((Boolean, Option[String], Int)) => Future[Either[ServerError, SuccessEnvelope.Accepted]] = {
    case (includeSleeping, namespace, wakeUpParallelism) =>
      recoverServerError(
        appMethods.propagateStandingQuery(includeSleeping, namespaceFromParam(namespace), wakeUpParallelism),
      )((_: Unit) => SuccessEnvelope.Accepted())
  }

  private val propagateStandingQueryServerEndpoint: Full[
    Unit,
    Unit,
    (Boolean, Option[String], Int),
    ServerError,
    SuccessEnvelope.Accepted,
    Any,
    Future,
  ] = propagateStandingQuery.serverLogic[Future](propagateStandingQueryLogic)

  protected[endpoints] val addSQOutputWorkflow
    : Endpoint[Unit, (String, Option[String], StandingQueryResultWorkflow), Either[
      ServerError,
      Either[BadRequest, NotFound],
    ], SuccessEnvelope.Created[Unit], Any] = rawStandingQuery
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
    .out(emptyOutputAs[SuccessEnvelope.Created[Unit]](SuccessEnvelope.Created(())))
    .post

  protected[endpoints] val addSQOutputWorkflowLogic: (
    (
      String,
      Option[String],
      StandingQueryResultWorkflow,
    ),
  ) => Future[Either[
    Either[ServerError, Either[BadRequest, NotFound]],
    SuccessEnvelope.Created[Unit],
  ]] = { case (sqName, namespace, workflow) =>
    recoverServerErrorEither(
      appMethods
        .addSQOutput(sqName, workflow.name, namespaceFromParam(namespace), workflow),
    )(SuccessEnvelope.Created(_))
  }

  private val addSQOutputWorkflowServerEndpoint: Full[
    Unit,
    Unit,
    (String, Option[String], StandingQueryResultWorkflow),
    Either[ServerError, Either[BadRequest, NotFound]],
    SuccessEnvelope.Created[Unit],
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
    (Option[String], Boolean, StandingQueryDefinition),
    Either[ServerError, Either[BadRequest, NotFound]],
    SuccessEnvelope.Created[RegisteredStandingQuery],
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
      """Learn more about writing [Standing Queries](https://docs.quine.io/components/writing-standing-queries.html)
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
    .out(jsonBody[SuccessEnvelope.Created[RegisteredStandingQuery]])

  protected[endpoints] val createSQLogic: ((Option[String], Boolean, StandingQueryDefinition)) => Future[
    Either[Either[ServerError, Either[BadRequest, NotFound]], SuccessEnvelope.Created[RegisteredStandingQuery]],
  ] = { case (namespace, shouldCalculateResultHashCode, definition) =>
    recoverServerErrorEither(
      appMethods
        .createSQ(definition.name, namespaceFromParam(namespace), shouldCalculateResultHashCode, definition),
    )(SuccessEnvelope.Created(_))
  }

  private val createSQServerEndpoint: Full[
    Unit,
    Unit,
    (Option[String], Boolean, StandingQueryDefinition),
    Either[ServerError, Either[BadRequest, NotFound]],
    SuccessEnvelope.Created[RegisteredStandingQuery],
    Any,
    Future,
  ] = createSQ.serverLogic[Future](createSQLogic)

  protected[endpoints] val deleteSQ
    : Endpoint[Unit, (String, Option[String]), Either[ServerError, NotFound], SuccessEnvelope.Ok[
      RegisteredStandingQuery,
    ], Any] =
    standingQueryBase
      .name("delete-standing-query")
      .summary("Delete Standing Query")
      .description("Immediately halt and remove the named Standing Query from Quine.")
      .in(sqName)
      .in(namespaceParameter)
      .delete
      .out(statusCode(StatusCode.Ok))
      .out(jsonBody[SuccessEnvelope.Ok[RegisteredStandingQuery]])
      .errorOutEither(notFoundError("No Standing Queries exist with the provided name."))

  protected[endpoints] val deleteSQLogic: ((String, Option[String])) => Future[
    Either[Either[ServerError, NotFound], SuccessEnvelope.Ok[RegisteredStandingQuery]],
  ] = { case (standingQueryName, namespace) =>
    recoverServerErrorEitherFlat(appMethods.deleteSQ(standingQueryName, namespaceFromParam(namespace)))(
      SuccessEnvelope.Ok(_),
    )
  }

  private val deleteSQServerEndpoint: Full[
    Unit,
    Unit,
    (String, Option[String]),
    Either[ServerError, ErrorResponse.NotFound],
    SuccessEnvelope.Ok[RegisteredStandingQuery],
    Any,
    Future,
  ] = deleteSQ.serverLogic[Future](deleteSQLogic)

  protected[endpoints] val deleteSQOutput: Endpoint[
    Unit,
    (String, String, Option[String]),
    Either[ServerError, NotFound],
    SuccessEnvelope.Ok[StandingQueryResultWorkflow],
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
      .out(jsonBody[SuccessEnvelope.Ok[StandingQueryResultWorkflow]])
      .errorOutEither(notFoundError("No Standing Queries exist with the provided name."))

  protected[endpoints] val deleteSQOutputLogic: ((String, String, Option[String])) => Future[
    Either[Either[ServerError, NotFound], SuccessEnvelope.Ok[StandingQueryResultWorkflow]],
  ] = { case (sqName, sqOutputName, namespace) =>
    recoverServerErrorEitherFlat(appMethods.deleteSQOutput(sqName, sqOutputName, namespaceFromParam(namespace)))(
      SuccessEnvelope.Ok(_),
    )
  }

  private val deleteSQOutputServerEndpoint: Full[
    Unit,
    Unit,
    (String, String, Option[String]),
    Either[ServerError, ErrorResponse.NotFound],
    SuccessEnvelope.Ok[StandingQueryResultWorkflow],
    Any,
    Future,
  ] = deleteSQOutput.serverLogic[Future](deleteSQOutputLogic)

  protected[endpoints] val getSq: Endpoint[
    Unit,
    (String, Option[String]),
    Either[ServerError, NotFound],
    SuccessEnvelope.Ok[RegisteredStandingQuery],
    Any,
  ] = standingQueryBase
    .name("get-standing-query-status")
    .summary("Standing Query Status")
    .description("Return the status information for a configured Standing Query by name.")
    .in(sqName)
    .in(namespaceParameter)
    .get
    .out(statusCode(StatusCode.Ok))
    .out(jsonBody[SuccessEnvelope.Ok[RegisteredStandingQuery]])
    .errorOutEither(notFoundError("No Standing Queries exist with the provided name."))

  protected[endpoints] val getSqLogic: ((String, Option[String])) => Future[
    Either[Either[ServerError, NotFound], SuccessEnvelope.Ok[RegisteredStandingQuery]],
  ] = { case (sqName, namespace) =>
    recoverServerErrorEitherFlat(appMethods.getSQ(sqName, namespaceFromParam(namespace)))(
      SuccessEnvelope.Ok(_),
    )
  }

  private val getSqServerEndpoint: Full[
    Unit,
    Unit,
    (String, Option[String]),
    Either[ServerError, ErrorResponse.NotFound],
    SuccessEnvelope.Ok[RegisteredStandingQuery],
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
