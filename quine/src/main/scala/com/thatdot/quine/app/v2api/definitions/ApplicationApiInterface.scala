package com.thatdot.quine.app.v2api.definitions

import java.util.Properties

import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters.CollectionHasAsScala

import com.thatdot.quine.app.BaseApp
import com.thatdot.quine.graph.{BaseGraph, LiteralOpsGraph, namespaceToString}

/** Access to application components for api endpoints */
trait ApplicationApiInterface {
  val graph: BaseGraph with LiteralOpsGraph
  val app: BaseApp
  def thisMemberIdx: Int

  //  endpoint business logic functionality.
  def getProperties: Future[Map[String, String]] = {
    val props: Properties = System.getProperties
    Future.successful(props.keySet.asScala.map(s => s.toString -> props.get(s).toString).toMap[String, String])
  }

  def getNamespaces: Future[List[String]] = Future.apply {
    graph.requiredGraphIsReady()
    app.getNamespaces.map(namespaceToString).toList
  }(ExecutionContext.parasitic)

  def createNamespace(namespace: String): Future[Boolean] =
    app.createNamespace(Some(Symbol(namespace)))

  def deleteNamespace(namespace: String): Future[Boolean] =
    app.deleteNamespace(Some(Symbol(namespace)))

}
