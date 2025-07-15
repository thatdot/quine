package com.thatdot.quine.app.routes

import scala.concurrent.Future

import com.thatdot.quine.app.v2api.definitions.query.{standing => V2ApiStanding}
import com.thatdot.quine.graph.{NamespaceId, StandingQueryId}

trait StandingQueryInterfaceV2 {

  def addStandingQueryV2(
    queryName: String,
    inNamespace: NamespaceId,
    standingQueryDefinition: V2ApiStanding.StandingQuery.StandingQueryDefinition,
  ): Future[StandingQueryInterfaceV2.Result]

  def cancelStandingQueryV2(
    queryName: String,
    inNamespace: NamespaceId,
  ): Future[Option[V2ApiStanding.StandingQuery.RegisteredStandingQuery]]

  def addStandingQueryOutputV2(
    queryName: String,
    outputName: String,
    inNamespace: NamespaceId,
    standingQueryResultWorkflow: V2ApiStanding.StandingQueryResultWorkflow,
  ): Future[StandingQueryInterfaceV2.Result]

  def removeStandingQueryOutputV2(
    queryName: String,
    outputName: String,
    inNamespace: NamespaceId,
  ): Future[Option[V2ApiStanding.StandingQueryResultWorkflow]]

  def getStandingQueriesV2(inNamespace: NamespaceId): Future[List[V2ApiStanding.StandingQuery.RegisteredStandingQuery]]

  def getStandingQueryV2(
    queryName: String,
    inNamespace: NamespaceId,
  ): Future[Option[V2ApiStanding.StandingQuery.RegisteredStandingQuery]]

  def getStandingQueryIdV2(queryName: String, inNamespace: NamespaceId): Option[StandingQueryId]
}

object StandingQueryInterfaceV2 {
  sealed trait Result

  object Result {
    case object Success extends Result
    case class AlreadyExists(name: String) extends Result
    case class NotFound(name: String) extends Result
  }
}
