package com.thatdot.quine.graph

import scala.concurrent.duration.DurationInt

import org.apache.pekko.actor.Actor

import com.github.blemale.scaffeine.Scaffeine

import com.thatdot.quine.graph.cypher.QueryPlan
import com.thatdot.quine.persistor.PrimePersistor

sealed trait QuinePatternRegistryMessage

object QuinePatternRegistryMessage {
  case class RegisterQueryPlan(
    namespaceId: NamespaceId,
    standingQueryId: StandingQueryId,
    queryPlan: QueryPlan,
    durable: Boolean,
  ) extends QuinePatternRegistryMessage
}

class QuinePatternRegistry(persistor: PrimePersistor) extends Actor {
  var durablePlans: Map[StandingQueryId, QueryPlan] = Map.empty

  private val ephemeralPlans = Scaffeine()
    .expireAfterWrite(1.hour)
    .maximumSize(500)
    .build[StandingQueryId, QueryPlan]()

  override def receive: Receive = { case QuinePatternRegistryMessage.RegisterQueryPlan(namespace, sqId, qp, durable) =>
    if (durable) {
      durablePlans += (sqId -> qp)
      persistor(namespace)
        .getOrElse(
          throw new IllegalArgumentException(
            s"Could not persist standing query because namespace: $namespace does not exist.",
          ),
        )
        .persistQueryPlan(sqId, qp)
        .onComplete(_ => ())(this.context.system.dispatcher)
    } else {
      ephemeralPlans.put(sqId, qp)
    }
  }
}
