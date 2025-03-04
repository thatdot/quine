package com.thatdot.quine.graph.quinepattern

import org.apache.pekko.actor.{ActorRef, Props}

import com.thatdot.quine.graph.{BaseGraph, behavior}

/** Trait representing operations related to Quine pattern handling within a graph.
  * This trait extends the `BaseGraph` trait and provides functionality for managing
  * pattern registries and loaders specific to Quine patterns.
  *
  * Responsibilities include:
  * - Ensuring compatibility with the required node type behavior.
  * - Providing access to the registry actor for managing pattern registries.
  * - Providing access to the loader actor for handling loading operations related to Quine patterns.
  */
trait QuinePatternOpsGraph extends BaseGraph {

  private[this] def requireCompatibleNodeType(): Unit =
    requireBehavior[QuinePatternOpsGraph, behavior.QuinePatternQueryBehavior]

  private[this] val registryActor: ActorRef = system.actorOf(Props(classOf[QuinePatternRegistry], namespacePersistor))

  private[this] val loaderActor: ActorRef = system.actorOf(Props(classOf[QuinePatternLoader], this))

  def getRegistry: ActorRef = {
    requireCompatibleNodeType()
    registryActor
  }

  def getLoader: ActorRef = {
    requireCompatibleNodeType()
    loaderActor
  }

}
