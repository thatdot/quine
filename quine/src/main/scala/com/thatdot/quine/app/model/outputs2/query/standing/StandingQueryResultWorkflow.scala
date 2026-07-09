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
import com.thatdot.quine.util.{GaugeKey, PressureGauge}

import TapBus.topicForSq

/** Carries the [[TapBus]] and the identifying triple (namespace / SQ / output) used to
  * compute tap topic names for one output's pipeline.
  *
  * Threaded through workflow construction as an implicit because two layers of `flow`
  * builders need it ([[StandingQueryResultWorkflow.flow]] for the raw and post-enrichment
  * taps, [[Workflow.flow]] for the pre-enrichment tap), and we don't want callers passing
  * the bus through by hand at each layer.
  *
  * The tap stages are inserted unconditionally; [[TapBus.hasSubscribers]] gates the
  * runtime cost — a lock-free read that short-circuits both serialization and publish
  * when nothing is listening. This is cheaper than forking the stream with a `wireTap`.
  */
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

  /** Builds the flow segment running from `filter` through `enrichmentQuery`, splicing in the
    * pre-enrichment tap stage between the transformation and the Cypher enrichment.
    *
    * @param tapCtx contains a referencet to the tapbus and the identifying triple (namespace / SQ / output) used to compute tap topic names
    */
  def flow(outputName: String, namespaceId: NamespaceId)(implicit
    graph: CypherOpsGraph,
    logConfig: LogConfig,
    tapCtx: TapContext,
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
    val preEnrichTap: BroadcastableFlow => BroadcastableFlow = {
      val topic = topicForSq(tapCtx.namespaceId, tapCtx.sqName, tapCtx.outputName, SqTapStage.PreEnrichment)
      priorFlow =>
        new BroadcastableFlow {
          override type Out = priorFlow.Out
          override def foldableFrom: DataFoldableFrom[Out] = priorFlow.foldableFrom
          override def flow: Flow[StandingQueryResult, Out, NotUsed] =
            // Map is technically blocking, but bus.publish is fire and forget,
            //   so in the case where there are no subscribers and the serialization is skipped,
            //   this is actually less expensive than forking the graph with a wireTap
            priorFlow.flow.map { out =>
              if (tapCtx.bus.hasSubscribers(topic)) tapCtx.bus.publish(topic, out)(priorFlow.foldableFrom)
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
      .andThen(preEnrichTap)
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

  /** Builds the end-to-end output flow: raw tap → workflow (filter/transform/pre-tap/enrich) →
    * post-enrichment tap → fanout to all configured sinks.
    *
    * @param tapCtx contains a referencet to the tapbus and the identifying triple (namespace / SQ / output) used to compute tap topic names
    */
  def flow(graph: CypherOpsGraph)(implicit
    logConfig: LogConfig,
    tapCtx: TapContext,
  ): Flow[StandingQueryResult, Unit, NotUsed] = {
    val rawTapFlow: Flow[StandingQueryResult, StandingQueryResult, NotUsed] = {
      val topic = topicForSq(tapCtx.namespaceId, tapCtx.sqName, "_raw_", SqTapStage.Raw)
      implicit val foldable = StandingQueryResultWorkflow.sqDataFoldableFrom(graph.idProvider)
      Flow[StandingQueryResult].map { x => if (tapCtx.bus.hasSubscribers(topic)) tapCtx.bus.publish(topic, x); x }
    }

    val preBroadcastFlow = workflow.flow(outputName, namespaceId)(graph, logConfig, tapCtx)

    val postEnrichFlow = {
      val topic = topicForSq(tapCtx.namespaceId, tapCtx.sqName, tapCtx.outputName, SqTapStage.PostEnrichment)
      preBroadcastFlow.flow.map { x =>
        if (tapCtx.bus.hasSubscribers(topic)) tapCtx.bus.publish(topic, x)(preBroadcastFlow.foldableFrom)
        x
      }
    }

    // Wrap each destination sink with a PressureGauge to detect per-destination backpressure
    val sqName = tapCtx.sqName
    val outputKey = s"$sqName/$outputName"
    val registry = graph.pressureGaugeRegistry

    val instrumentedSinks = destinationStepsList.toList.zipWithIndex.map { case (destStep, idx) =>
      val rawSink = destStep.sink(outputName, namespaceId)(preBroadcastFlow.foldableFrom, logConfig)
      // Include the destination index so multiple destinations of the same type get distinct gauge
      // keys. Without it, same-type destinations collide on the key and only one appears in the diagram.
      val gaugeKey = GaugeKey("sq-output", namespaceId.name, outputKey, s"destination-$idx-${destStep.slug}")
      Flow[preBroadcastFlow.Out].via(PressureGauge(registry, gaugeKey)).to(rawSink)
    }

    // Throughput meter: counts results entering the destination fan-out (before alsoToAll).
    // Named ".post-enrichment" when enrichment is configured, ".throughput" otherwise.
    val hasEnrichment = workflow.enrichmentQuery.isDefined || workflow.preEnrichmentTransformation.isDefined
    val meterSuffix = if (hasEnrichment) "post-enrichment" else "throughput"
    val throughputMeter = graph.metrics.metricRegistry.meter(s"sq.output.${namespaceId.name}.$outputKey.$meterSuffix")

    // Pre-workflow backpressure gauge, registered only when an enrichment stage exists to observe.
    // Registering it here (rather than in each caller) keeps OSS and enterprise consistent and ensures
    // the "pre-workflow" gauge is present iff enrichment is configured — so the diagram reports real
    // enrichment backpressure instead of always reporting FLOWING.
    val entryFlow: Flow[StandingQueryResult, StandingQueryResult, NotUsed] =
      if (hasEnrichment) {
        val preGaugeKey = GaugeKey("sq-output", namespaceId.name, outputKey, "pre-workflow")
        Flow[StandingQueryResult].via(PressureGauge(registry, preGaugeKey))
      } else Flow[StandingQueryResult]

    entryFlow
      .via(rawTapFlow)
      .via(postEnrichFlow)
      .map { x => throughputMeter.mark(); x }
      .alsoToAll(instrumentedSinks: _*)
      .map(_ => ())
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
