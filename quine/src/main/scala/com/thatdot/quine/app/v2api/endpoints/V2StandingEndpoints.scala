package com.thatdot.quine.app.v2api.endpoints

import scala.concurrent.Future

import io.circe.generic.extras.auto._
import io.circe.{Decoder, Encoder}
import sttp.model.StatusCode
import sttp.tapir.generic.auto._
import sttp.tapir.server.ServerEndpoint
import sttp.tapir.server.ServerEndpoint.Full
import sttp.tapir.{EndpointInput, Schema, path, query, statusCode}

import com.thatdot.quine.app.v2api.definitions.query.standing.StandingQuery._
import com.thatdot.quine.app.v2api.definitions.query.standing.StandingQueryPattern.StandingQueryMode.MultipleValues
import com.thatdot.quine.app.v2api.definitions.query.standing.StandingQueryPattern._
import com.thatdot.quine.app.v2api.definitions.query.standing.StandingQueryResultOutputUserDef
import com.thatdot.quine.app.v2api.definitions.query.standing.StandingQueryResultOutputUserDef.PrintToStandardOut
import com.thatdot.quine.app.v2api.definitions.{
  CreateSQApiCmd,
  CreateSQOutputApiCmd,
  CustomError,
  DeleteSQOutputApiCmd,
  ErrorEnvelope,
  GetSQApiCmd,
  ListSQsApiCmd,
  ObjectEnvelope,
  PropagateSQsApiCmd,
  V2QuineEndpointDefinitions,
}
import com.thatdot.quine.graph.NamespaceId

trait V2StandingEndpoints extends V2QuineEndpointDefinitions with V2StandingApiSchemas with V2ApiConfiguration {

  /** SQ Name path element */
  private val sqName: EndpointInput.PathCapture[String] =
    path[String]("standing-query-name").description("Unique name for a standing query")

  /** SQ Output Name path element */
  private val sqOutputName: EndpointInput.PathCapture[String] =
    path[String]("standing-query-output-name").description("Unique name for a standing query output")

  /** SQ Base path */
  private def standingQueryEndpoint[T](implicit
    schema: Schema[ObjectEnvelope[T]],
    encoder: Encoder[T],
    decoder: Decoder[T],
  ) = baseEndpoint[T]("standing-queries").tag("Standing Queries")

  private val listStandingQueriesEndpoint: ServerEndpoint.Full[Unit, Unit, (Option[Int], Option[String]), ErrorEnvelope[
    _ <: CustomError,
  ], ObjectEnvelope[List[RegisteredStandingQuery]], Any, Future] =
    standingQueryEndpoint[List[RegisteredStandingQuery]]
      .name("List Standing Queries")
      .description("""|Individual standing queries are issued into the graph one time;
                      |result outputs are produced as new data is written into Quine and matches are found.
                      |
                      |Compared to traditional queries, standing queries are less imperative
                      |and more declarative - it doesn't matter what order parts of the pattern match,
                      |only that the composite structure exists.
                      |
                      |Learn more about writing
                      |[standing queries](https://docs.quine.io/components/writing-standing-queries.html)
                      |in the docs.""".stripMargin)
      .in(query[Option[String]]("namespace"))
      .get
      .serverLogic { case (memberIdx, namespace) =>
        runServerLogic[NamespaceId, List[RegisteredStandingQuery]](
          ListSQsApiCmd,
          memberIdx,
          namespaceFromParam(namespace),
          ns => appMethods.listStandingQueries(ns),
        )
      }

  private val propagateStandingQueryEndpoint
    : Full[Unit, Unit, (Option[Int], Option[Boolean], Option[String], Option[Int]), ErrorEnvelope[
      _ <: CustomError,
    ], ObjectEnvelope[Unit], Any, Future] =
    standingQueryEndpoint[Unit]
      .name("Propagate Standing Queries")
      .description("""When a new standing query is registered in the system, it gets automatically
                     |registered on new nodes (or old nodes that are loaded back into the cache). This
                     |behavior is the default because pro-actively setting the standing query on all
                     |existing data might be quite costly depending on how much historical data there is.
                     |
                     |However, sometimes there is a legitimate use-case for eagerly propagating standing
                     |queries across the graph, for instance:
                     |
                     |  * When interactively constructing a standing query for already-ingested data
                     |  * When creating a new standing query that needs to be applied to recent data
                     |""".stripMargin)
      .in("control")
      .in("propagate")
      .in(
        query[Option[Boolean]]("include-sleeping").description(
          "Propagate to all sleeping nodes. Setting to true can be costly if there is lot of data. Default is false.",
        ),
      )
      .in(namespaceParameter)
      .in(query[Option[Int]]("wake-up-parallelism"))
      .post
      .serverLogic { case (memberIdx, includeSleeping, namespace, wakeUpParallelism) =>
        runServerLogic[(Option[Boolean], NamespaceId, Int), Unit](
          PropagateSQsApiCmd,
          memberIdx,
          (includeSleeping, namespaceFromParam(namespace), wakeUpParallelism.getOrElse(4)),
          t => appMethods.propagateStandingQuery(t._1, t._2, t._3),
        )
      }

  private val addSQOutputEndpoint
    : Full[Unit, Unit, (Option[Int], String, String, Option[String], StandingQueryResultOutputUserDef), ErrorEnvelope[
      _ <: CustomError,
    ], ObjectEnvelope[Unit], Any, Future] = standingQueryEndpoint[Unit]
    .name("Create Standing Query Output")
    .description(
      "Each standing query can have any number of destinations to which `StandingQueryResults` will be routed.",
    )
    .in(sqName)
    .in("outputs")
    .in(sqOutputName)
    .in(namespaceParameter)
    .in(jsonOrYamlBody[StandingQueryResultOutputUserDef](Some(StandingQueryResultOutputUserDef.PrintToStandardOut())))
    .out(statusCode(StatusCode.Created))
    .post
    .serverLogic { case (memberIdx, sqName, sqOutputName, namespace, outputDef) =>
      runServerLogicFromEither[(String, String, NamespaceId, StandingQueryResultOutputUserDef), Unit](
        CreateSQOutputApiCmd,
        memberIdx,
        (sqName, sqOutputName, namespaceFromParam(namespace), outputDef),
        t => appMethods.addSQOutput(t._1, t._2, t._3, t._4),
      )
    }

  private val exPattern = """MATCH (n) WHERE n % 100 == 0 RETURN n.num"""

  private val createSqExample: StandingQueryDefinition =
    StandingQueryDefinition(Cypher(exPattern, MultipleValues), Map.from(List("stdout" -> PrintToStandardOut())))

  private val createSQEndpoint
    : Full[Unit, Unit, (Option[Int], String, Option[String], StandingQueryDefinition), ErrorEnvelope[
      _ <: CustomError,
    ], ObjectEnvelope[Option[Unit]], Any, Future] = standingQueryEndpoint[Option[Unit]]
    .name("Create Standing Query")
    .description("""|Individual standing queries are issued into the graph one time;
                     |result outputs are produced as new data is written into Quine and matches are found.
                     |
                     |Compared to traditional queries, standing queries are less imperative
                     |and more declarative - it doesn't matter what order parts of the pattern match,
                     |only that the composite structure exists.
                     |
                     |Learn more about writing
                     |[standing queries](https://docs.quine.io/components/writing-standing-queries.html)
                     |in the docs.""".stripMargin)
    .in(sqName)
    .in(namespaceParameter)
    .in(jsonOrYamlBody[StandingQueryDefinition](Some(createSqExample)))
    .post
    .out(statusCode(StatusCode.Created))
    .serverLogic { case (memberIdx, sqName, namespace, definition) =>
      runServerLogicFromEither[(String, NamespaceId, StandingQueryDefinition), Option[Unit]](
        CreateSQApiCmd,
        memberIdx,
        (sqName, namespaceFromParam(namespace), definition),
        t => appMethods.createSQ(t._1, t._2, t._3),
      )
    }

  private val deleteSQEndpoint: Full[Unit, Unit, (Option[Int], String, Option[String]), ErrorEnvelope[
    _ <: CustomError,
  ], ObjectEnvelope[Option[RegisteredStandingQuery]], Any, Future] =
    standingQueryEndpoint[Option[RegisteredStandingQuery]]
      .name("Delete Standing Query")
      .description("Immediately halt and remove the named standing query from Quine.")
      .in(sqName)
      .in(namespaceParameter)
      .delete
      .serverLogic { case (memberIdx, standingQueryName, namespace) =>
        runServerLogic[(String, NamespaceId), Option[RegisteredStandingQuery]](
          DeleteSQOutputApiCmd,
          memberIdx,
          (standingQueryName, namespaceFromParam(namespace)),
          t => appMethods.deleteSQ(t._1, t._2),
        )
      }

  private val deleteSQOutputEndpoint: Full[Unit, Unit, (Option[Int], String, String, Option[String]), ErrorEnvelope[
    _ <: CustomError,
  ], ObjectEnvelope[Option[StandingQueryResultOutputUserDef]], Any, Future] =
    standingQueryEndpoint[Option[StandingQueryResultOutputUserDef]]
      .name("Delete Standing Query Output")
      .description("Remove an output from a standing query.")
      .in(sqName)
      .in("outputs")
      .in(sqOutputName)
      .in(namespaceParameter)
      .delete
      .serverLogic { case (memberIdx, sqName, sqOutputName, namespace) =>
        runServerLogic[(String, String, NamespaceId), Option[StandingQueryResultOutputUserDef]](
          DeleteSQOutputApiCmd,
          memberIdx,
          (sqName, sqOutputName, namespaceFromParam(namespace)),
          t => appMethods.deleteSQOutput(t._1, t._2, t._3),
        )
      }

  private val getSqEndpoint: Full[Unit, Unit, (Option[Int], String, Option[String]), ErrorEnvelope[
    _ <: CustomError,
  ], ObjectEnvelope[Option[RegisteredStandingQuery]], Any, Future] =
    standingQueryEndpoint[Option[RegisteredStandingQuery]]
      .name("Standing Query Status")
      .description("Return the status information for a configured standing query by name.")
      .in(sqName)
      .in(namespaceParameter)
      .get
      .serverLogic { case (memberIdx, sqName, namespace) =>
        runServerLogic[(String, NamespaceId), Option[RegisteredStandingQuery]](
          GetSQApiCmd,
          memberIdx,
          (sqName, namespaceFromParam(namespace)),
          t => appMethods.getSQ(t._1, t._2),
        )
      }

  val standingQueryEndpoints: List[ServerEndpoint[Any, Future]] = List(
    listStandingQueriesEndpoint,
    getSqEndpoint,
    createSQEndpoint,
    propagateStandingQueryEndpoint,
    addSQOutputEndpoint,
    deleteSQEndpoint,
    deleteSQOutputEndpoint,
  )

}
