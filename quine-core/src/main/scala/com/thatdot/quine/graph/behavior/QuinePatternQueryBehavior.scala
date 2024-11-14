package com.thatdot.quine.graph.behavior

import scala.concurrent.Promise

import org.apache.pekko.NotUsed
import org.apache.pekko.actor.Actor
import org.apache.pekko.stream.scaladsl.{BroadcastHub, Flow, Keep, Source}

import com.thatdot.quine.graph.cypher.{QueryContext, QuinePattern}
import com.thatdot.quine.graph.messaging.{QuineIdOps, QuineMessage, QuineRefOps}
import com.thatdot.quine.graph.{
  BaseNodeActor,
  QuinePatternLoaderMessage,
  QuinePatternOpsGraph,
  RunningStandingQuery,
  StandingQueryId,
  StandingQueryOpsGraph,
  StandingQueryPattern,
}

object ExampleMessages {
  sealed trait QuinePatternMessages extends QuineMessage

  object QuinePatternMessages {
    case class RegisterPattern(
      quinePattern: QuinePattern,
      pid: StandingQueryId,
      queryStream: Source[QueryContext, NotUsed],
    ) extends QuinePatternMessages

    case class RunPattern(
      pid: StandingQueryId,
      quinePattern: QuinePattern,
      currentEnv: QueryContext,
      result: Promise[QueryContext],
    ) extends QuinePatternMessages
  }
}

trait QuinePatternQueryBehavior
    extends Actor
    with BaseNodeActor
    with QuineIdOps
    with QuineRefOps
    with StandingQueryBehavior {

  protected def graph: QuinePatternOpsGraph with StandingQueryOpsGraph

  private def patternToEffect(
    qp: QuinePattern,
    id: StandingQueryId,
    outputStream: Source[QueryContext, NotUsed],
  ): Source[QueryContext, NotUsed] =
    qp match {
      case QuinePattern.QuineUnit => outputStream
      case node: QuinePattern.Node =>
        outputStream.via(Flow[QueryContext].mapAsync(1) { qc =>
          val resultPromise = Promise[QueryContext]()

          self ! ExampleMessages.QuinePatternMessages.RunPattern(id, node, qc, resultPromise)

          resultPromise.future
        })
      case edge: QuinePattern.Edge =>
        val broadcastHub = BroadcastHub.sink[QueryContext]
        val newSrc = outputStream.toMat(broadcastHub)(Keep.right).run()
        this.edges.matching(edge.edgeLabel).toList.foreach { he =>
          he.other ! ExampleMessages.QuinePatternMessages.RegisterPattern(edge.remotePattern, id, newSrc)
        }
        newSrc
      case fold: QuinePattern.Fold =>
        val initSrc = patternToEffect(fold.init, id, outputStream)
        fold.over.foldLeft(initSrc) { (stream, pattern) =>
          patternToEffect(pattern, id, stream)
        }
    }

  def updateQuinePatternOnNode(
    qp: QuinePattern,
    id: StandingQueryId,
    outputStream: Source[QueryContext, NotUsed],
  ): Unit = graph.getLoader ! QuinePatternLoaderMessage.MergeQueryStream(id, patternToEffect(qp, id, outputStream))

  def updateQuinePatternsOnNode(): Unit = {
    val runningStandingQueries = // Silently empty if namespace is absent.
      graph.standingQueries(namespace).fold(Map.empty[StandingQueryId, RunningStandingQuery])(_.runningStandingQueries)

    for {
      (sqId, runningSQ) <- runningStandingQueries
      query <- runningSQ.query.queryPattern match {
        case qp: StandingQueryPattern.QuinePatternQueryPattern => Some(qp.quinePattern)
        case _ => None
      }
    } updateQuinePatternOnNode(query, sqId, Source.empty[QueryContext])
  }
}
