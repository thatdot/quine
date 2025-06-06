package com.thatdot.quine.app.model.outputs2.query.standing

import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.Flow

import cats.data.NonEmptyList

import com.thatdot.common.logging.Log.LogConfig
import com.thatdot.quine.app.model.ingest2.core.{DataFoldableFrom, DataFolderTo}
import com.thatdot.quine.app.model.outputs2.definitions.DestinationSteps
import com.thatdot.quine.app.model.outputs2.query.standing.StandingQueryResultWorkflow._
import com.thatdot.quine.app.model.query.CypherQuery
import com.thatdot.quine.graph.cypher.QueryContext
import com.thatdot.quine.graph.{CypherOpsGraph, NamespaceId, StandingQueryResult, cypher}
import com.thatdot.quine.model.QuineIdProvider

case class Workflow(
  enrichmentQuery: Option[CypherQuery],
) {
  import Workflow._

  def flow(outputName: String, namespaceId: NamespaceId)(implicit graph: CypherOpsGraph): BroadcastableFlow = {
    implicit val idProvider: QuineIdProvider = graph.idProvider
    val sqOrigin: BroadcastableFlow = new OriginFlow {
      override def foldableFrom: DataFoldableFrom[StandingQueryResult] = implicitly
    }
    val maybeThenEnrich = enrichmentQuery.fold(identity[BroadcastableFlow] _) {
      enrichQuery => (originFlow: BroadcastableFlow) =>
        new BroadcastableFlow {
          override type Out = cypher.QueryContext
          override def foldableFrom: DataFoldableFrom[Out] = implicitly
          override def flow: Flow[StandingQueryResult, Out, NotUsed] = {
            val dataFold = originFlow.foldableFrom.to[cypher.Value]
            originFlow.flow.map(dataFold).via(enrichQuery.flow(outputName, namespaceId))
          }
        }
    }

    val workflowBroadcastableFlow: BroadcastableFlow =
      Seq(
        maybeThenEnrich,
      ).foldLeft(sqOrigin: BroadcastableFlow)((acc, curF) => curF(acc))

    workflowBroadcastableFlow
  }
}

object Workflow {
  trait BroadcastableFlow {
    type Out
    def foldableFrom: DataFoldableFrom[Out]
    def flow: Flow[StandingQueryResult, Out, NotUsed]
  }

  trait OriginFlow extends BroadcastableFlow {
    type Out = StandingQueryResult
    def foldableFrom: DataFoldableFrom[StandingQueryResult]
    def flow: Flow[StandingQueryResult, StandingQueryResult, NotUsed] = Flow[StandingQueryResult]
  }
}

case class StandingQueryResultWorkflow(
  outputName: String,
  namespaceId: NamespaceId,
  workflow: Workflow,
  destinationStepsList: NonEmptyList[DestinationSteps],
) {

  def flow(graph: CypherOpsGraph)(implicit logConfig: LogConfig): Flow[StandingQueryResult, Unit, NotUsed] = {
    val preBroadcastFlow = workflow.flow(outputName, namespaceId)(graph)
    val sinks = destinationStepsList
      .map(_.sink(outputName, namespaceId)(preBroadcastFlow.foldableFrom, logConfig))
      .toList
    preBroadcastFlow.flow.alsoToAll(sinks: _*).map(_ => ())
  }
}

object StandingQueryResultWorkflow {
  val title = "Standing Query Result Workflow"

  implicit def sqDataFoldableFrom(implicit quineIdProvider: QuineIdProvider): DataFoldableFrom[StandingQueryResult] =
    new DataFoldableFrom[StandingQueryResult] {
      override def fold[B](value: StandingQueryResult, folder: DataFolderTo[B]): B = {
        val outerMap = folder.mapBuilder()

        val targetMetaBuilder = folder.mapBuilder()
        value.meta.toMap.foreach { case (k, v) =>
          targetMetaBuilder.add(k, DataFoldableFrom.quineValueDataFoldableFrom.fold(v, folder))
        }
        outerMap.add("meta", targetMetaBuilder.finish())

        val targetDataBuilder = folder.mapBuilder()
        value.data.foreach { case (k, v) =>
          targetDataBuilder.add(k, DataFoldableFrom.quineValueDataFoldableFrom.fold(v, folder))
        }
        outerMap.add("data", targetDataBuilder.finish())

        outerMap.finish()
      }
    }

  implicit val queryContextFoldableFrom: DataFoldableFrom[QueryContext] = new DataFoldableFrom[QueryContext] {
    import DataFoldableFrom.cypherValueDataFoldable

    override def fold[B](value: QueryContext, folder: DataFolderTo[B]): B = {
      val builder = folder.mapBuilder()
      value.environment.foreach { case (k, v) =>
        builder.add(k.name, cypherValueDataFoldable.fold(v, folder))
      }
      builder.finish()
    }
  }
}
