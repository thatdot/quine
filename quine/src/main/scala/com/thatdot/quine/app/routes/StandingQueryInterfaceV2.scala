package com.thatdot.quine.app.routes

import scala.concurrent.Future

import com.thatdot.quine.app.model.outputs2.query.standing.{StandingQuery, StandingQueryResultWorkflow}
import com.thatdot.quine.graph.{NamespaceId, StandingQueryId}

trait StandingQueryInterfaceV2 {

  def addStandingQueryV2(
    queryName: String,
    inNamespace: NamespaceId,
    standingQueryDefinition: StandingQuery.StandingQueryDefinition,
  ): Future[StandingQueryInterfaceV2.Result]

  def cancelStandingQueryV2(
    queryName: String,
    inNamespace: NamespaceId,
  ): Future[Option[StandingQuery.RegisteredStandingQuery]]

  def addStandingQueryOutputV2(
    queryName: String,
    outputName: String,
    inNamespace: NamespaceId,
    standingQueryResultWorkflow: StandingQueryResultWorkflow,
  ): Future[StandingQueryInterfaceV2.Result]

  def removeStandingQueryOutputV2(
    queryName: String,
    outputName: String,
    inNamespace: NamespaceId,
  ): Future[Option[StandingQueryResultWorkflow]]

  def getStandingQueriesV2(inNamespace: NamespaceId): Future[List[StandingQuery.RegisteredStandingQuery]]

  def getStandingQueryV2(
    queryName: String,
    inNamespace: NamespaceId,
  ): Future[Option[StandingQuery.RegisteredStandingQuery]]

  def getStandingQueryIdV2(queryName: String, inNamespace: NamespaceId): Option[StandingQueryId]
}

object StandingQueryInterfaceV2 {
  sealed trait Result

  object Result {
    case object Success extends Result
    case class AlreadyExists(name: String) extends Result
    case class NotFound(queryName: String) extends Result
  }
}
