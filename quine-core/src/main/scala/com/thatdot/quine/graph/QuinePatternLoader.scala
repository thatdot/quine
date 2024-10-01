package com.thatdot.quine.graph

import java.time.LocalDateTime

import org.apache.pekko.actor.Actor

import com.thatdot.quine.graph.behavior.ExampleMessages.QuinePatternMessages
import com.thatdot.quine.graph.cypher.QuinePattern
import com.thatdot.quine.graph.messaging.SpaceTimeQuineId
import com.thatdot.quine.model.Milliseconds

sealed trait QueryTime

object QueryTime {
  case object Now extends QueryTime
  case class At(time: LocalDateTime) extends QueryTime
}

sealed trait ExecutionMode

object ExecutionMode {
  case object Lazy extends ExecutionMode
  case class Eager(at: QueryTime) extends ExecutionMode
}

/** This will be fleshed out in the future to describe anchoring information for the query plan
  */
sealed trait Anchors

sealed trait QuinePatternLoaderMessage

object QuinePatternLoaderMessage {
  case class LoadQuery(
    namespaceId: NamespaceId,
    id: StandingQueryId,
    queryPlan: QuinePattern,
    anchors: Anchors,
    mode: ExecutionMode,
  ) extends QuinePatternLoaderMessage
}

/** This actor is responsible for
  * <ul>
  *   <li>Caching query plans</li>
  *   <li>Distributing query plans to nodes</li>
  * </ul>
  *
  * The query plan cache supports query plan reuse
  * The nodes are given query plans based on anchoring information
  *
  * Nodes won't necessarily immediately execute on query plans received
  * The <b>ExecutionMode</b> parameter of the <b>LoadQuery</b> message determines
  * the behavior of the on-node interpreter.
  *
  * @param graph A reference to the graph, statically typed to handle QuinePattern
  */
class QuinePatternLoader(graph: QuinePatternOpsGraph) extends Actor {
  //TODO The compiler needs to provide better anchoring information so that the query loader
  //     can correctly interact with the select nodes that are required. Additionally, the
  //     notion of historical vs. current needs to be handled
  override def receive: Receive = { case QuinePatternLoaderMessage.LoadQuery(namespace, id, plan, _, mode) =>
    mode match {
      case ExecutionMode.Lazy =>
        graph.getRegistry ! QuinePatternRegistryMessage.RegisterQuinePattern(namespace, id, plan, true)
      case ExecutionMode.Eager(_) =>
        graph.getRegistry ! QuinePatternRegistryMessage.RegisterQuinePattern(namespace, id, plan, false)
    }
    val nodeIds = graph.enumerateAllNodeIds(namespace)
    nodeIds.runForeach { qid =>
      val stqid = SpaceTimeQuineId(qid, namespace, Some(Milliseconds(System.currentTimeMillis())))
      graph.relayTell(stqid, QuinePatternMessages.RegisterPattern(plan, id, null), this.self)
    }(graph.materializer)
    ()
  }
}
