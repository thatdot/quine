package com.thatdot.quine.graph

import scala.concurrent.duration.DurationInt

import org.apache.pekko.actor.Actor

import com.github.blemale.scaffeine.Scaffeine

import com.thatdot.quine.graph.cypher.QuinePattern
import com.thatdot.quine.persistor.PrimePersistor

sealed trait QuinePatternRegistryMessage

object QuinePatternRegistryMessage {
  case class RegisterQuinePattern(
    namespaceId: NamespaceId,
    standingQueryId: StandingQueryId,
    quinePattern: QuinePattern,
    durable: Boolean
  ) extends QuinePatternRegistryMessage
}

class QuinePatternRegistry(persistor: PrimePersistor) extends Actor {
  var durablePlans: Map[StandingQueryId, QuinePattern] = Map.empty

  private val ephemeralPlans = Scaffeine()
    .expireAfterWrite(1.hour)
    .maximumSize(500)
    .build[StandingQueryId, QuinePattern]()

  override def receive: Receive = {
    case QuinePatternRegistryMessage.RegisterQuinePattern(namespace, sqId, qp, durable) =>
      if (durable) {
        durablePlans += (sqId -> qp)
        persistor(namespace)
          .getOrElse(
            throw new IllegalArgumentException(
              s"Could not persist standing query because namespace: $namespace does not exist."
            )
          )
          .persistQuinePattern(sqId, qp)
          .onComplete(_ => ())(this.context.system.dispatcher)
      } else {
        ephemeralPlans.put(sqId, qp)
      }
  }
}
