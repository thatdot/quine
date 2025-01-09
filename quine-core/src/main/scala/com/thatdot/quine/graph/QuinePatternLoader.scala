package com.thatdot.quine.graph

import java.time.LocalDateTime

import org.apache.pekko.NotUsed
import org.apache.pekko.actor.Status.Success
import org.apache.pekko.actor.{Actor, ActorRef}
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.{Keep, MergeHub, Sink, Source}

import com.thatdot.language.ast.Value
import com.thatdot.quine.graph.behavior.QuinePatternCommand
import com.thatdot.quine.graph.cypher.{
  ExpressionInterpreter,
  QueryContext,
  QueryPlan,
  QuinePatternUnimplementedException,
}
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
  case class LoadAck(queryId: StandingQueryId) extends QuinePatternLoaderMessage

  case class LoadQuery(
    namespaceId: NamespaceId,
    id: StandingQueryId,
    queryPlan: QueryPlan,
    anchors: Anchors,
    mode: ExecutionMode,
  ) extends QuinePatternLoaderMessage

  case class LoadIngestQuery(
    namespaceId: NamespaceId,
    id: StandingQueryId,
    queryPlan: QueryPlan,
    anchors: Anchors,
    mode: ExecutionMode,
    parameters: Map[Symbol, cypher.Value],
    ref: ActorRef,
  ) extends QuinePatternLoaderMessage

  case class LoadMore(
    namespaceId: NamespaceId,
    id: StandingQueryId,
    queryPlan: List[QueryPlan],
    parameters: Map[Symbol, cypher.Value],
    stream: Source[QueryContext, NotUsed],
  ) extends QuinePatternLoaderMessage

  case class MergeQueryStream(id: StandingQueryId, stream: Source[QueryContext, NotUsed])
      extends QuinePatternLoaderMessage

  case class MergeIngestStream(id: StandingQueryId, stream: Source[QueryContext, NotUsed])
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

  private var ingestHubs = Map.empty[StandingQueryId, ActorRef]

  //TODO The compiler needs to provide better anchoring information so that the query loader
  //     can correctly interact with the select nodes that are required. Additionally, the
  //     notion of historical vs. current needs to be handled
  override def receive: Receive = {
    case QuinePatternLoaderMessage.LoadAck(id) =>
      ingestHubs(id) ! Success(id)
      ingestHubs -= id
    case QuinePatternLoaderMessage.LoadQuery(namespace, id, plan, _, mode) =>
      mode match {
        case ExecutionMode.Lazy =>
          graph.getRegistry ! QuinePatternRegistryMessage.RegisterQueryPlan(namespace, id, plan, true)
        case ExecutionMode.Eager(_) =>
          graph.getRegistry ! QuinePatternRegistryMessage.RegisterQueryPlan(namespace, id, plan, false)
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
          QuinePatternCommand.RegisterPattern(plan, id, Source.single(QueryContext.empty)),
          this.self,
        )
      }(graph.materializer)
      ()
    case QuinePatternLoaderMessage.MergeQueryStream(id, stream) =>
      queryHubs(id).runWith(stream)
      ()
    case QuinePatternLoaderMessage.MergeIngestStream(id, stream) => ingestHubs(id) ! stream
    case QuinePatternLoaderMessage.LoadMore(namespace, sqid, plan, parameters, stream) =>
      val plan1 = plan.head
      plan1 match {
        case QueryPlan.SpecificId(idExp, nodeInstructions) =>
          ExpressionInterpreter.eval(idExp, graph.idProvider, QueryContext.empty, parameters) match {
            case Value.NodeId(id) =>
              val stqid = SpaceTimeQuineId(id, namespace, None)
              graph.relayTell(
                stqid,
                QuinePatternCommand.LoadPlan(nodeInstructions, namespace, plan.tail, sqid, parameters, stream),
              )
            case _ => ???
          }
        case QueryPlan.AllNodeScan(_) => ???
        case QueryPlan.Effect(com.thatdot.cypher.ast.Effect.Create(_, _)) =>
          throw new QuinePatternUnimplementedException(s"Query plan: $plan1 is not implemented")
        case _ => ???
      }
    case QuinePatternLoaderMessage.LoadIngestQuery(namespace, sqid, plan, _, _, parameters, ref) =>
      ingestHubs += (sqid -> ref)

      val productPlan = plan.asInstanceOf[QueryPlan.Product].of
      val plan1 = productPlan.head

      plan1 match {
        case QueryPlan.SpecificId(idExp, nodeInstructions) =>
          ExpressionInterpreter.eval(idExp, graph.idProvider, QueryContext.empty, parameters) match {
            case Value.NodeId(id) =>
              val stqid = SpaceTimeQuineId(id, namespace, None)
              graph.relayTell(
                stqid,
                QuinePatternCommand.LoadPlan(
                  nodeInstructions,
                  namespace,
                  productPlan.tail,
                  sqid,
                  parameters,
                  Source.single(QueryContext.empty),
                ),
              )
            case _ => ???
          }
        case QueryPlan.AllNodeScan(_) => ???
        case _ => throw new QuinePatternUnimplementedException(s"Query plan: $plan1 is not implemented")
      }

  }
}
