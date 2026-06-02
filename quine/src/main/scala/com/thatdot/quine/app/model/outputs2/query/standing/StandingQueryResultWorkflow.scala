package com.thatdot.quine.app.model.outputs2.query.standing

import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.Flow

import cats.data.NonEmptyList

import com.thatdot.common.logging.Log.LogConfig
import com.thatdot.data.{DataFoldableFrom, DataFolderTo}
import com.thatdot.outputs2.DataFoldableSink
import com.thatdot.quine.app.model.outputs2.query.CypherQuery
import com.thatdot.quine.graph.cypher.QueryContext
import com.thatdot.quine.graph.{CypherOpsGraph, NamespaceId, StandingQueryResult, cypher}
import com.thatdot.quine.model.QuineIdProvider

import TapBus.topicForSq

/** Identifies the SQ output pipeline tap, carrying the bus instance for publishing. */
case class TapContext(
  bus: TapBus,
  sqName: String,
  outputName: String,
  namespaceId: NamespaceId,
)

case class Workflow(
  filter: Option[Predicate],
  /*
  {"meta": {"isPositiveMatch": true}, "data": {"emailAddress": "i.am.a.user@gmail.com"}}
   => {"username": "i.am.a.user", "removeDownstream": !meta.isPositiveMatch}
     => Value: Map("username" -> String, "removeDownstream" -> Boolean)
   => [1, 2, 3, 4]
     => Value: [1, 2, 3, 4]
   */
  preEnrichmentTransformation: Option[StandingQueryResultTransformation],
  /*
   MATCH (u:User) WHERE id(u) = idFrom(that.username) RETURNING (<u.Products>, that.removeDownstream)
   */
  enrichmentQuery: Option[CypherQuery],
) {
  import StandingQueryResultWorkflow._
  import Workflow._

  def flow(outputName: String, namespaceId: NamespaceId)(implicit
    graph: CypherOpsGraph,
    logConfig: LogConfig,
    tapCtx: Option[TapContext] = None,
  ): BroadcastableFlow = {
    implicit val idProvider: QuineIdProvider = graph.idProvider
    import com.thatdot.quine.app.data.QuineDataFoldersTo.cypherValueFolder

    val sqOrigin: StandingQueryResultFlow = new StandingQueryResultFlow {
      override def foldableFrom: DataFoldableFrom[StandingQueryResult] = implicitly
    }
    val maybeThenFilter = filter.fold(identity[StandingQueryResultFlow] _) {
      predicate => (sqFlow: StandingQueryResultFlow) =>
        new StandingQueryResultFlow {
          override def foldableFrom: DataFoldableFrom[StandingQueryResult] = sqFlow.foldableFrom
          override def flow: Flow[StandingQueryResult, StandingQueryResult, NotUsed] =
            sqFlow.flow.filter(predicate.apply)
        }
    }
    val maybeThenPreEnrich = preEnrichmentTransformation.fold((x: StandingQueryResultFlow) => x: BroadcastableFlow) {
      // Right now, `preEnrichmentTransformation` only supports built-in offerings, but this will need to change when
      //  we want to support JS transformations here, too.
      transformation => (priorFlow: StandingQueryResultFlow) =>
        new BroadcastableFlow {
          override type Out = transformation.Out
          override def foldableFrom: DataFoldableFrom[Out] = transformation.dataFoldableFrom
          override def flow: Flow[StandingQueryResult, Out, NotUsed] = priorFlow.flow.map(transformation.apply)
        }
    }
    val maybePreEnrichTap: BroadcastableFlow => BroadcastableFlow = tapCtx.fold(identity[BroadcastableFlow] _) { ctx =>
      val topic = topicForSq(ctx.namespaceId, ctx.sqName, ctx.outputName, SqTapStage.PreEnrichment)
      priorFlow =>
        new BroadcastableFlow {
          override type Out = priorFlow.Out
          override def foldableFrom: DataFoldableFrom[Out] = priorFlow.foldableFrom
          override def flow: Flow[StandingQueryResult, Out, NotUsed] =
            // Map is technically blocking, but bus.publish is fire and forget,
            //   so in the case where there are no subscribers and the serialization is skipped,
            //   this is actually less expensive than forking the graph with a wireTap
            priorFlow.flow.map { out =>
              if (ctx.bus.hasSubscribers(topic)) ctx.bus.publish(topic, out)(priorFlow.foldableFrom)
              out
            }
        }
    }
    val maybeThenEnrich = enrichmentQuery.fold(identity[BroadcastableFlow] _) {
      enrichQuery => (priorFlow: BroadcastableFlow) =>
        new BroadcastableFlow {
          override type Out = cypher.QueryContext
          override def foldableFrom: DataFoldableFrom[Out] = implicitly
          override def flow: Flow[StandingQueryResult, Out, NotUsed] = {
            val dataFold = priorFlow.foldableFrom.to[cypher.Value]
            priorFlow.flow.map(dataFold).via(enrichQuery.flow(outputName, namespaceId))
          }
        }
    }

    val steps = maybeThenFilter
      .andThen(maybeThenPreEnrich)
      .andThen(maybePreEnrichTap)
      .andThen(maybeThenEnrich)

    steps(sqOrigin)
  }
}

object Workflow {
  trait BroadcastableFlow {
    type Out
    def foldableFrom: DataFoldableFrom[Out]
    def flow: Flow[StandingQueryResult, Out, NotUsed]
  }

  trait StandingQueryResultFlow extends BroadcastableFlow {
    type Out = StandingQueryResult
    def foldableFrom: DataFoldableFrom[StandingQueryResult]
    def flow: Flow[StandingQueryResult, StandingQueryResult, NotUsed] = Flow[StandingQueryResult]
  }
}

case class StandingQueryResultWorkflow(
  outputName: String,
  namespaceId: NamespaceId,
  workflow: Workflow,
  destinationStepsList: NonEmptyList[DataFoldableSink],
) {

  def flow(graph: CypherOpsGraph)(implicit
    logConfig: LogConfig,
    tapCtx: Option[TapContext] = None,
  ): Flow[StandingQueryResult, Unit, NotUsed] = {
    val rawTapFlow: Flow[StandingQueryResult, StandingQueryResult, NotUsed] = tapCtx match {
      case Some(ctx) =>
        val topic = topicForSq(ctx.namespaceId, ctx.sqName, "_raw_", SqTapStage.Raw)
        implicit val foldable = StandingQueryResultWorkflow.sqDataFoldableFrom(graph.idProvider)
        Flow[StandingQueryResult].map { x => if (ctx.bus.hasSubscribers(topic)) ctx.bus.publish(topic, x); x }
      case None => Flow[StandingQueryResult]
    }

    val preBroadcastFlow = workflow.flow(outputName, namespaceId)(graph, logConfig, tapCtx)

    val postEnrichFlow = tapCtx match {
      case Some(ctx) =>
        val topic = topicForSq(ctx.namespaceId, ctx.sqName, ctx.outputName, SqTapStage.PostEnrichment)
        preBroadcastFlow.flow.map { x =>
          if (ctx.bus.hasSubscribers(topic)) ctx.bus.publish(topic, x)(preBroadcastFlow.foldableFrom)
          x
        }
      case None => preBroadcastFlow.flow
    }

    val sinks = destinationStepsList
      .map(_.sink(outputName, namespaceId)(preBroadcastFlow.foldableFrom, logConfig))
      .toList
    rawTapFlow.via(postEnrichFlow).alsoToAll(sinks: _*).map(_ => ())
  }
}

object StandingQueryResultWorkflow {
  val title = "Standing Query Result Workflow"

  implicit def sqDataFoldableFrom(implicit quineIdProvider: QuineIdProvider): DataFoldableFrom[StandingQueryResult] = {
    import com.thatdot.quine.serialization.data.QuineSerializationFoldablesFrom.quineValueDataFoldableFrom

    new DataFoldableFrom[StandingQueryResult] {
      override def fold[B](value: StandingQueryResult, folder: DataFolderTo[B]): B = {
        val outerMap = folder.mapBuilder()

        val targetMetaBuilder = folder.mapBuilder()
        value.meta.toMap.foreach { case (k, v) =>
          targetMetaBuilder.add(k, quineValueDataFoldableFrom.fold(v, folder))
        }
        outerMap.add("meta", targetMetaBuilder.finish())

        val targetDataBuilder = folder.mapBuilder()
        value.data.foreach { case (k, v) =>
          targetDataBuilder.add(k, quineValueDataFoldableFrom.fold(v, folder))
        }
        outerMap.add("data", targetDataBuilder.finish())

        outerMap.finish()
      }
    }
  }

  implicit val queryContextFoldableFrom: DataFoldableFrom[QueryContext] = new DataFoldableFrom[QueryContext] {
    import com.thatdot.quine.app.data.QuineDataFoldablesFrom.cypherValueDataFoldable

    override def fold[B](value: QueryContext, folder: DataFolderTo[B]): B = {
      val builder = folder.mapBuilder()
      value.environment.foreach { case (k, v) =>
        builder.add(k.name, cypherValueDataFoldable.fold(v, folder))
      }
      builder.finish()
    }
  }
}
