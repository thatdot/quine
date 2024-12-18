package com.thatdot.quine.app.routes

import scala.concurrent.{ExecutionContext, Future}

import org.apache.pekko.http.scaladsl.model.sse.ServerSentEvent
import org.apache.pekko.http.scaladsl.model.ws
import org.apache.pekko.http.scaladsl.server.Directives._
import org.apache.pekko.http.scaladsl.server.{Route, ValidationRejection}
import org.apache.pekko.stream.scaladsl.{Flow, Sink}
import org.apache.pekko.stream.{Materializer, OverflowStrategy}
import org.apache.pekko.util.Timeout

import cats.data.NonEmptyList
import endpoints4s.{Invalid, Valid}

import com.thatdot.quine.app.ingest.util.KafkaSettingsValidator
import com.thatdot.quine.app.ingest.util.KafkaSettingsValidator.ErrorString
import com.thatdot.quine.exceptions.NamespaceNotFoundException
import com.thatdot.quine.graph.cypher.CypherException
import com.thatdot.quine.graph.{
  InvalidQueryPattern,
  NamespaceId,
  StandingQueryId,
  StandingQueryOpsGraph,
  StandingQueryResult,
  StandingQueryResultStructure,
}
import com.thatdot.quine.routes.StandingQueryResultOutputUserDef.WriteToKafka
import com.thatdot.quine.routes._
import com.thatdot.quine.util.Log._

trait StandingQueryStore {

  def addStandingQuery(queryName: String, inNamespace: NamespaceId, query: StandingQueryDefinition): Future[Boolean]

  def cancelStandingQuery(queryName: String, inNamespace: NamespaceId): Future[Option[RegisteredStandingQuery]]

  def addStandingQueryOutput(
    queryName: String,
    outputName: String,
    inNamespace: NamespaceId,
    sqResultOutput: StandingQueryResultOutputUserDef,
  ): Future[Option[Boolean]]

  def removeStandingQueryOutput(
    queryName: String,
    outputName: String,
    inNamespace: NamespaceId,
  ): Future[Option[StandingQueryResultOutputUserDef]]

  def getStandingQueries(inNamespace: NamespaceId): Future[List[RegisteredStandingQuery]]

  def getStandingQuery(queryName: String, inNamespace: NamespaceId): Future[Option[RegisteredStandingQuery]]

  def getStandingQueryId(queryName: String, inNamespace: NamespaceId): Option[StandingQueryId]
}

/** The Pekko HTTP implementation of [[StandingQueryRoutes]] */
trait StandingQueryRoutesImpl
    extends StandingQueryRoutes
    with endpoints4s.circe.JsonSchemas
    with com.thatdot.quine.app.routes.exts.PekkoQuineEndpoints
    with com.thatdot.quine.app.routes.exts.circe.JsonEntitiesFromSchemas {

  implicit def graph: StandingQueryOpsGraph

  implicit def timeout: Timeout

  implicit def materializer: Materializer

  implicit protected def logConfig: LogConfig

  def quineApp: StandingQueryStore

  private def validateOutputDef(outputDef: StandingQueryResultOutputUserDef): Option[NonEmptyList[ErrorString]] =
    outputDef match {
      case k: WriteToKafka => KafkaSettingsValidator.validateOutput(k.kafkaProperties)
      case _ => None
    }

  private val standingIssueRoute = standingIssue.implementedByAsync { case (name, namespaceParam, query) =>
    graph.requiredGraphIsReadyFuture {
      try quineApp
        .addStandingQuery(name, namespaceFromParam(namespaceParam), query)
        .map {
          case false => Left(endpoints4s.Invalid(s"There is already a standing query named '$name'"))
          case true => Right(Some(()))
        }(graph.nodeDispatcherEC)
        .recoverWith { case _: NamespaceNotFoundException =>
          Future.successful(Right(None))
        }(graph.nodeDispatcherEC)
      catch {
        case iqp: InvalidQueryPattern => Future.successful(Left(endpoints4s.Invalid(iqp.message)))
        case cypherException: CypherException => Future.successful(Left(endpoints4s.Invalid(cypherException.pretty)))
      }
    }
  }

  private val standingRemoveOutRoute = standingRemoveOut.implementedByAsync { case (name, outputName, namespaceParam) =>
    graph.requiredGraphIsReadyFuture {
      quineApp.removeStandingQueryOutput(name, outputName, namespaceFromParam(namespaceParam))
    }
  }

  private val standingCancelRoute = standingCancel.implementedByAsync { case (name: String, namespaceParam) =>
    graph.requiredGraphIsReadyFuture {
      quineApp.cancelStandingQuery(name, namespaceFromParam(namespaceParam))
    }
  }

  private val standingGetRoute = standingGet.implementedByAsync { case (queryName, namespaceParam) =>
    graph.requiredGraphIsReadyFuture {
      quineApp.getStandingQuery(queryName, namespaceFromParam(namespaceParam))
    }
  }

  private val standingAddOutRoute = standingAddOut.implementedByAsync {
    case (name, outputName, namespaceParam, sqResultOutput) =>
      graph.requiredGraphIsReadyFuture {
        validateOutputDef(sqResultOutput) match {
          case Some(errors) =>
            Future.successful(
              Some(Left(Invalid(s"Cannot create output `$outputName`: ${errors.toList.mkString(",")}"))),
            )
          case None =>
            quineApp
              .addStandingQueryOutput(name, outputName, namespaceFromParam(namespaceParam), sqResultOutput)
              .map {
                _.map {
                  case false =>
                    Left(endpoints4s.Invalid(s"There is already a standing query output named '$outputName'"))
                  case true => Right(())
                }
              }(graph.shardDispatcherEC)
        }
      }
  }

  private val standingGetWebsocketRoute =
    (standing / standingName /? namespace).directive {
      case Valid((name, namespaceParam)) =>
        quineApp
          .getStandingQueryId(name, namespaceFromParam(namespaceParam))
          .flatMap(sqid =>
            graph
              .standingQueries(namespaceFromParam(namespaceParam))
              // Silently ignores SQs in any absent namespace, returning `None`
              .flatMap((sq: StandingQueryOpsGraph#NamespaceStandingQueries) => sq.standingResultsHub(sqid)),
          ) match {
          case None => reject(ValidationRejection("No Standing Query with the provided name was found"))
          case Some(source) =>
            handleWebSocketMessages(
              Flow
                .fromSinkAndSource(
                  Sink.ignore,
                  source
                    .buffer(size = 128, overflowStrategy = OverflowStrategy.dropHead)
                    // todo: Verify this is the correct behavior and it shouldn't depend on some configuration option somewhere
                    .map((r: StandingQueryResult) =>
                      ws.TextMessage(r.toJson(StandingQueryResultStructure.WithMetaData()).noSpaces),
                    ),
                )
                .named(s"sq-results-websocket-for-$name"),
            )

        }
      case Invalid(nameValidationErrors) =>
        // ValidationRejection is a safe "semantics violated" rejection -- but this case should not be reachable anyway
        reject(nameValidationErrors.map(ValidationRejection(_)): _*)
    }

  private val standingGetResultsRoute: Route =
    (standing / standingName / "results" /? namespace).directive {
      case Valid((name, namespaceParam)) =>
        quineApp
          .getStandingQueryId(name, namespaceFromParam(namespaceParam))
          .flatMap(sqid => // Silently ignores any SQs in an absent namespace, returning `None`
            graph.standingQueries(namespaceFromParam(namespaceParam)).flatMap(_.standingResultsHub(sqid)),
          ) match {
          case None => reject(ValidationRejection("No Standing Query with the provided name was found"))
          case Some(source) =>
            Util.sseRoute(
              source
                .map(sqResult =>
                  ServerSentEvent(
                    // todo: Verify this is the correct behavior and it shouldn't depend on some configuration option somewhere
                    data = sqResult.toJson(StandingQueryResultStructure.WithMetaData()).noSpaces,
                    eventType = Some(if (sqResult.meta.isPositiveMatch) "result" else "cancellation"),
                    id = Some(sqResult.dataHashCode.toString),
                  ),
                ),
            )
        }
      case Invalid(nameValidationErrors) =>
        // ValidationRejection is a safe "semantics violated" rejection -- but this case should not be reachable anyway
        reject(nameValidationErrors.map(ValidationRejection(_)): _*)
    }

  private val standingListRoute = standingList.implementedByAsync { namespaceParam =>
    graph.requiredGraphIsReadyFuture {
      quineApp.getStandingQueries(namespaceFromParam(namespaceParam))
    }
  }

  private val standingPropagateRoute = standingPropagate.implementedByAsync { case (wakeUpNodes, par, namespaceParam) =>
    graph.requiredGraphIsReadyFuture {
      graph
        .standingQueries(namespaceFromParam(namespaceParam))
        .fold(Future.successful[Option[Unit]](None)) {
          _.propagateStandingQueries(Some(par).filter(_ => wakeUpNodes)).map(_ => Some(()))(ExecutionContext.parasitic)
        }
    }
  }

  final val standingQueryRoutes: Route = {
    standingIssueRoute ~
    standingAddOutRoute ~
    standingRemoveOutRoute ~
    standingCancelRoute ~
    standingGetWebsocketRoute ~
    standingGetResultsRoute ~
    standingGetRoute ~
    standingListRoute ~
    standingPropagateRoute
  }
}
