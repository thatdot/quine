package com.thatdot.quine.app.v2api.endpoints

import scala.concurrent.Future

import io.circe.generic.extras.auto._
import sttp.model.StatusCode
import sttp.tapir.generic.auto._
import sttp.tapir.server.ServerEndpoint
import sttp.tapir.server.ServerEndpoint.Full
import sttp.tapir.{EndpointInput, emptyOutputAs, path, query, statusCode}

import com.thatdot.quine.app.util.StringOps
import com.thatdot.quine.app.v2api.definitions.ErrorResponse.{BadRequest, NotFound, ServerError}
import com.thatdot.quine.app.v2api.definitions.ErrorResponseHelpers.{badRequestError, notFoundError, serverError}
import com.thatdot.quine.app.v2api.definitions.query.standing.StandingQuery._
import com.thatdot.quine.app.v2api.definitions.query.standing.StandingQueryPattern.StandingQueryMode.MultipleValues
import com.thatdot.quine.app.v2api.definitions.query.standing.StandingQueryPattern._
import com.thatdot.quine.app.v2api.definitions.query.standing.StandingQueryResultWorkflow
import com.thatdot.quine.app.v2api.definitions.{ErrorResponse, SuccessEnvelope, V2QuineEndpointDefinitions}

trait V2StandingEndpoints extends V2QuineEndpointDefinitions with V2StandingApiSchemas with StringOps {

  /** `io.circe.generic.extras.auto._` appears to require a local reference to a
    * Configuration in order to find implicit Encoders through a mixed-in TapirJsonCirce
    * (here provided via V2StandingApiSchemas->V2ApiConfiguration), even though such
    * a Configuration is also available in a mixin or ancestor. Thus, a trait-named
    * config that refers to our standard config.
    */
  implicit val v2StandingEndpointsConfig: Configuration = typeDiscriminatorConfig

  /** SQ Name path element */
  private val sqName: EndpointInput.PathCapture[String] =
    path[String]("standing-query-name").description("Unique name for a Standing Query.")

  /** SQ Output Name path element */
  private val sqOutputName: EndpointInput.PathCapture[String] =
    path[String]("standing-query-output-name").description("Unique name for a Standing Query Output.")

  /** SQ Base path */
  private def standingQueryEndpoint =
    rawEndpoint("standing-queries")
      .tag("Standing Queries")
      .errorOut(serverError())

  private def rawStandingQueryEndpoint =
    rawEndpoint("standing-queries")
      .tag("Standing Queries")

  private val listStandingQueriesEndpoint: ServerEndpoint.Full[Unit, Unit, Option[
    String,
  ], ServerError, SuccessEnvelope.Ok[List[RegisteredStandingQuery]], Any, Future] =
    standingQueryEndpoint
      .name("List Standing Queries")
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
      .serverLogic[Future] { namespace =>
        recoverServerError(appMethods.listStandingQueries(namespaceFromParam(namespace)))(
          (inp: List[RegisteredStandingQuery]) => SuccessEnvelope.Ok(inp),
        )
      }

  private val propagateStandingQueryEndpoint: Full[
    Unit,
    Unit,
    (Boolean, Option[String], Int),
    ServerError,
    SuccessEnvelope.Accepted,
    Any,
    Future,
  ] =
    standingQueryEndpoint
      .name("Propagate Standing Queries")
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
      .serverLogic[Future] { case (includeSleeping, namespace, wakeUpParallelism) =>
        recoverServerError(
          appMethods.propagateStandingQuery(includeSleeping, namespaceFromParam(namespace), wakeUpParallelism),
        )((_: Unit) => SuccessEnvelope.Accepted())
      }

  private val addSQOutputWorkflowEndpoint = rawStandingQueryEndpoint
    .name("Create Standing Query Output Workflow")
    .description(
      "Each Standing Query can have any number of destinations to which `StandingQueryResults` will be routed.",
    )
    .in(sqName)
    .in("outputs")
    .in(sqOutputName)
    .in(namespaceParameter)
    .in(jsonOrYamlBody[StandingQueryResultWorkflow](Some(exampleStandingQueryResultWorkflowToStandardOut)))
    .errorOut(badRequestError("Output is invalid.", "There is another output with that name already."))
    .errorOutEither(notFoundError("No Standing Queries exist with the provided name."))
    .errorOutEither(serverError())
    .mapErrorOut(err => err.swap)(err => err.swap)
    .out(statusCode(StatusCode.Created))
    .out(emptyOutputAs[SuccessEnvelope.Created[Unit]](SuccessEnvelope.Created(())))
    .post
    .serverLogic[Future] { case (sqName, sqOutputName, namespace, workflow) =>
      recoverServerErrorEither(
        appMethods
          .addSQOutput(sqName, sqOutputName, namespaceFromParam(namespace), workflow),
      )(SuccessEnvelope.Created(_))
    }

  private val exPattern = "MATCH (n) WHERE n.num % 100 = 0 RETURN n.num"

  private val createSqExample: StandingQueryDefinition = StandingQueryDefinition(
    pattern = Cypher(exPattern, MultipleValues),
    outputs = exampleStandingQueryResultWorkflowMap,
  )

  private val createSQEndpoint: Full[Unit, Unit, (String, Option[String], Boolean, StandingQueryDefinition), Either[
    ServerError,
    Either[BadRequest, NotFound],
  ], SuccessEnvelope.Created[RegisteredStandingQuery], Any, Future] = rawStandingQueryEndpoint
    .name("Create Standing Query")
    .description(
      """Individual Standing Queries are issued into the graph one time;
        |result outputs are produced as new data is written into Quine and matches are found.""".asOneLine + "\n\n" +
      """Compared to traditional queries, Standing Queries are less imperative
        |and more declarative - it doesn't matter what order parts of the pattern match,
        |only that the composite structure exists.""".asOneLine + "\n\n" +
      """Learn more about writing [Standing Queries](https://docs.quine.io/components/writing-standing-queries.html)
        |in the docs.""".asOneLine,
    )
    .in(sqName)
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
    .serverLogic[Future] { case (sqName, namespace, shouldCalculateResultHashCode, definition) =>
      recoverServerErrorEither(
        appMethods
          .createSQ(sqName, namespaceFromParam(namespace), shouldCalculateResultHashCode, definition),
      )(SuccessEnvelope.Created(_))
    }

  private val deleteSQEndpoint
    : Full[Unit, Unit, (String, Option[String]), Either[ServerError, ErrorResponse.NotFound], SuccessEnvelope.Ok[
      RegisteredStandingQuery,
    ], Any, Future] =
    standingQueryEndpoint
      .name("Delete Standing Query")
      .description("Immediately halt and remove the named Standing Query from Quine.")
      .in(sqName)
      .in(namespaceParameter)
      .delete
      .out(statusCode(StatusCode.Ok))
      .out(jsonBody[SuccessEnvelope.Ok[RegisteredStandingQuery]])
      .errorOutEither(notFoundError("No Standing Queries exist with the provided name."))
      .serverLogic[Future] { case (standingQueryName, namespace) =>
        recoverServerErrorEitherFlat(appMethods.deleteSQ(standingQueryName, namespaceFromParam(namespace)))(
          SuccessEnvelope.Ok(_),
        )
      }

  private val deleteSQOutputEndpoint: Full[Unit, Unit, (String, String, Option[String]), Either[
    ServerError,
    ErrorResponse.NotFound,
  ], SuccessEnvelope.Ok[StandingQueryResultWorkflow], Any, Future] =
    standingQueryEndpoint
      .name("Delete Standing Query Output")
      .description("Remove an output from a Standing Query.")
      .in(sqName)
      .in("outputs")
      .in(sqOutputName)
      .in(namespaceParameter)
      .delete
      .out(statusCode(StatusCode.Ok))
      .out(jsonBody[SuccessEnvelope.Ok[StandingQueryResultWorkflow]])
      .errorOutEither(notFoundError("No Standing Queries exist with the provided name."))
      .serverLogic[Future] { case (sqName, sqOutputName, namespace) =>
        recoverServerErrorEitherFlat(appMethods.deleteSQOutput(sqName, sqOutputName, namespaceFromParam(namespace)))(
          SuccessEnvelope.Ok(_),
        )
      }

  private val getSqEndpoint
    : Full[Unit, Unit, (String, Option[String]), Either[ServerError, ErrorResponse.NotFound], SuccessEnvelope.Ok[
      RegisteredStandingQuery,
    ], Any, Future] =
    standingQueryEndpoint
      .name("Standing Query Status")
      .description("Return the status information for a configured Standing Query by name.")
      .in(sqName)
      .in(namespaceParameter)
      .get
      .out(statusCode(StatusCode.Ok))
      .out(jsonBody[SuccessEnvelope.Ok[RegisteredStandingQuery]])
      .errorOutEither(notFoundError("No Standing Queries exist with the provided name."))
      .serverLogic[Future] { case (sqName, namespace) =>
        recoverServerErrorEitherFlat(appMethods.getSQ(sqName, namespaceFromParam(namespace)))(
          SuccessEnvelope.Ok(_),
        )
      }

  val standingQueryEndpoints: List[ServerEndpoint[Any, Future]] = List(
    listStandingQueriesEndpoint,
    getSqEndpoint,
    createSQEndpoint,
    propagateStandingQueryEndpoint,
    addSQOutputWorkflowEndpoint,
    deleteSQEndpoint,
    deleteSQOutputEndpoint,
  )

}
