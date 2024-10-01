package com.thatdot.quine.graph

import org.apache.pekko.actor.{ActorRef, Props}

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
