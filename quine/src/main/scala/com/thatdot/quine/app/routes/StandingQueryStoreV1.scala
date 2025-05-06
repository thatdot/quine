package com.thatdot.quine.app.routes

import scala.concurrent.Future

import com.thatdot.quine.graph.{NamespaceId, StandingQueryId}
import com.thatdot.quine.routes.{RegisteredStandingQuery, StandingQueryDefinition, StandingQueryResultOutputUserDef}

trait StandingQueryStoreV1 {

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
