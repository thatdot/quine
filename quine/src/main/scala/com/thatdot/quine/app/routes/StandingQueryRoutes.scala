package com.thatdot.quine.app.routes

import scala.concurrent.Future

import akka.http.scaladsl.model.sse.ServerSentEvent
import akka.http.scaladsl.model.ws
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.{Route, ValidationRejection}
import akka.stream.scaladsl.{Flow, Sink}
import akka.stream.{Materializer, OverflowStrategy}
import akka.util.Timeout

import endpoints4s.{Invalid, Valid}

import com.thatdot.quine.graph.{InvalidQueryPattern, StandingQueryId, StandingQueryOpsGraph, StandingQueryResult}
import com.thatdot.quine.routes._

trait StandingQueryStore {

  def addStandingQuery(queryName: String, query: StandingQueryDefinition): Future[Boolean]

  def cancelStandingQuery(queryName: String): Future[Option[RegisteredStandingQuery]]

  def addStandingQueryOutput(
    queryName: String,
    outputName: String,
    sqResultOutput: StandingQueryResultOutputUserDef
  ): Future[Option[Boolean]]

  def removeStandingQueryOutput(
    queryName: String,
    outputName: String
  ): Future[Option[StandingQueryResultOutputUserDef]]

  def getStandingQueries(): Future[List[RegisteredStandingQuery]]

  def getStandingQuery(queryName: String): Future[Option[RegisteredStandingQuery]]

  def getStandingQueryId(queryName: String): Option[StandingQueryId]
}

/** The Akka HTTP implementation of [[StandingQueryRoutes]] */
trait StandingQueryRoutesImpl
    extends StandingQueryRoutes
    with endpoints4s.ujson.JsonSchemas
    with endpoints4s.akkahttp.server.Endpoints
    with endpoints4s.akkahttp.server.JsonEntitiesFromSchemas
    with exts.ServerQuineEndpoints {

  implicit def graph: StandingQueryOpsGraph
  implicit def timeout: Timeout
  implicit def materializer: Materializer

  def serviceState: StandingQueryStore

  private val standingIssueRoute = standingIssue.implementedByAsync { case (name, query) =>
    try serviceState
      .addStandingQuery(name, query)
      .map {
        case false => Left(endpoints4s.Invalid(s"There is already a standing query named '$name'"))
        case true => Right(())
      }(graph.shardDispatcherEC)
    catch {
      case InvalidQueryPattern(message) =>
        Future.successful(Left(endpoints4s.Invalid(message)))
    }
  }

  private val standingAddOutRoute = standingAddOut.implementedByAsync { case (name, outputName, sqResultOutput) =>
    serviceState
      .addStandingQueryOutput(name, outputName, sqResultOutput)
      .map {
        _.map {
          case false => Left(endpoints4s.Invalid(s"There is already a standing query output named '$outputName'"))
          case true => Right(())
        }
      }(graph.shardDispatcherEC)
  }

  private val standingRemoveOutRoute = standingRemoveOut.implementedByAsync { case (name, outputName) =>
    serviceState.removeStandingQueryOutput(name, outputName)
  }

  private val standingCancelRoute = standingCancel.implementedByAsync { (name: String) =>
    serviceState.cancelStandingQuery(name)
  }

  private val standingGetRoute = standingGet.implementedByAsync(serviceState.getStandingQuery(_))

  private val standingGetWebsocketRoute =
    (standing / standingName).directive {
      case Valid(name) =>
        serviceState.getStandingQueryId(name).flatMap(graph.wireTapStandingQuery(_)) match {
          case None => reject(ValidationRejection("No Standing Query with the provided name was found"))
          case Some(source) =>
            handleWebSocketMessages(
              Flow
                .fromSinkAndSource(
                  Sink.ignore,
                  source
                    .buffer(size = 128, overflowStrategy = OverflowStrategy.dropHead)
                    .map((r: StandingQueryResult) => ws.TextMessage(r.toJson.noSpaces))
                )
                .named(s"sq-results-websocket-for-$name")
            )

        }
      case Invalid(nameValidationErrors) =>
        // ValidationRejection is a safe "semantics violated" rejection -- but this case should not be reachable anyway
        reject(nameValidationErrors.map(ValidationRejection(_)): _*)
    }

  private val standingGetResultsRoute: Route =
    (standing / standingName / "results").directive {
      case Valid(name) =>
        serviceState.getStandingQueryId(name).flatMap(graph.wireTapStandingQuery(_)) match {
          case None => reject(ValidationRejection("No Standing Query with the provided name was found"))
          case Some(source) =>
            Util.sseRoute(
              source
                .map(sqResult =>
                  ServerSentEvent(
                    data = sqResult.toJson.noSpaces,
                    eventType = Some(if (sqResult.meta.isPositiveMatch) "result" else "cancellation"),
                    id = Some(sqResult.meta.resultId.uuid.toString)
                  )
                )
            )
        }
      case Invalid(nameValidationErrors) =>
        // ValidationRejection is a safe "semantics violated" rejection -- but this case should not be reachable anyway
        reject(nameValidationErrors.map(ValidationRejection(_)): _*)
    }

  private val standingListRoute = standingList.implementedByAsync { _ =>
    serviceState.getStandingQueries()
  }

  private val standingPropagateRoute = standingPropagate.implementedByAsync { case (wakeUpNodes, par) =>
    graph.propagateStandingQueries(Some(par).filter(_ => wakeUpNodes))
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
