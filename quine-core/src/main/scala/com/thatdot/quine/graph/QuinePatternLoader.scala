package com.thatdot.quine.graph

import java.time.LocalDateTime

import org.apache.pekko.NotUsed
import org.apache.pekko.actor.{Actor, ActorRef, Status}
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.{Flow, Keep, MergeHub, Sink, Source}

import com.thatdot.language.phases.DependencyGraph
import com.thatdot.quine.graph.behavior.ExampleMessages.QuinePatternMessages
import com.thatdot.quine.graph.cypher.{QueryContext, QuinePattern}
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

  case class LoadIngestQuery(
    namespaceId: NamespaceId,
    id: StandingQueryId,
    queryPlan: DependencyGraph,
    anchors: Anchors,
    mode: ExecutionMode,
    src: Source[QueryContext, NotUsed],
    ref: ActorRef,
  ) extends QuinePatternLoaderMessage

  case class MergeQueryStream(id: StandingQueryId, stream: Source[QueryContext, NotUsed])
      extends QuinePatternLoaderMessage
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

  implicit private val materializer: Materializer = Materializer.matFromSystem(this.context.system)

  private var queryHubs = Map.empty[StandingQueryId, Sink[QueryContext, NotUsed]]

  //TODO The compiler needs to provide better anchoring information so that the query loader
  //     can correctly interact with the select nodes that are required. Additionally, the
  //     notion of historical vs. current needs to be handled
  override def receive: Receive = {
    case QuinePatternLoaderMessage.LoadQuery(namespace, id, plan, _, mode) =>
      mode match {
        case ExecutionMode.Lazy =>
          graph.getRegistry ! QuinePatternRegistryMessage.RegisterQuinePattern(namespace, id, plan, true)
        case ExecutionMode.Eager(_) =>
          graph.getRegistry ! QuinePatternRegistryMessage.RegisterQuinePattern(namespace, id, plan, false)
      }
      val printOut = Sink.foreach(println)
      val sourceHub = MergeHub.source[QueryContext]
      val runningHub = sourceHub.toMat(printOut)(Keep.left).run()
      queryHubs += (id -> runningHub)
      val nodeIds = graph.enumerateAllNodeIds(namespace)
      nodeIds.runForeach { qid =>
        val stqid = SpaceTimeQuineId(qid, namespace, Some(Milliseconds(System.currentTimeMillis())))
        graph.relayTell(
          stqid,
          QuinePatternMessages.RegisterPattern(plan, id, Source.single(QueryContext.empty)),
          this.self,
        )
      }(graph.materializer)
      ()
    case QuinePatternLoaderMessage.MergeQueryStream(id, stream) =>
      queryHubs(id).runWith(stream)
      ()
    case QuinePatternLoaderMessage.LoadIngestQuery(namespace, _, plan, _, _, src, ref) =>
      val stream =
        com.thatdot.quine.graph.cypher.Compiler.compileDepGraphToStream(plan, namespace, src, graph, this.self)
      ref ! stream.via(Flow[QueryContext].map { qc =>
        ref ! Status.Success("Finished!")
        qc
      })
  }
}
