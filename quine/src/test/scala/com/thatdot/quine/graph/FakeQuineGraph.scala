package com.thatdot.quine.graph

import scala.concurrent.Future

import org.apache.pekko.actor.{ActorRef, ActorSystem}
import org.apache.pekko.testkit.TestKit
import org.apache.pekko.util.Timeout

import com.codahale.metrics.NoopMetricRegistry

import com.thatdot.common.logging.Log.LogConfig
import com.thatdot.common.quineid.QuineId
import com.thatdot.quine.app.config.EdgeIteration
import com.thatdot.quine.graph.NodeActor.{Journal, MultipleValuesStandingQueries}
import com.thatdot.quine.graph.edges.SyncEdgeCollection
import com.thatdot.quine.graph.messaging.{AskableQuineMessage, QuineMessage, QuineRef, ResultHandler, ShardRef}
import com.thatdot.quine.graph.metrics.HostQuineMetrics
import com.thatdot.quine.model.QuineIdProvider
import com.thatdot.quine.persistor.{EventEffectOrder, InMemoryPersistor, PrimePersistor}
import com.thatdot.quine.util.QuineDispatchers

/** A fake Quine Graph [Service] for testing with minimal (-ish) integrations and weight. Currently, it's best for this
  * to have its own file because the abstract types used for the type definitions of `Node` and `Snapshot` are private
  * to the `graph` package, while tests that consume this may not all be in the `graph` package. Also, it's big.
  *
  * Principles for further development:
  * - Add new implementations over unimplemented items as necessary, until or unless conflicts occur with the needs of
  *   other consumers
  * - When/if conflicts arise or the mood strikes, at least parameterize the class construction
  * - When/if the class construction becomes burdensome, or the mood strikes, implement the builder pattern for this
  * - For all of the above; consider doing the same for, or somehow reasonably DRY-ing, the `FakeNoveltyGraph` class
  */
class FakeQuineGraph(
  override val system: ActorSystem = ActorSystem.create(),
  // Must be an argument, otherwise `metrics` call in BaseGraph construction has NPE
  override val metrics: HostQuineMetrics = HostQuineMetrics(
    enableDebugMetrics = true,
    metricRegistry = new NoopMetricRegistry(),
    omitDefaultNamespace = false,
  ),
) extends CypherOpsGraph {

  override def dispatchers: QuineDispatchers = new QuineDispatchers(system)

  override def idProvider: QuineIdProvider = QuineUUIDProvider

  override val namespacePersistor: PrimePersistor = InMemoryPersistor.namespacePersistor

  implicit override protected def logConfig: LogConfig = LogConfig.permissive

  override type Node = AbstractNodeActor
  override type Snapshot = NodeSnapshot // Not Abstract in order to simplify node support test implementation
  override type NodeConstructorRecord = Product
  override def nodeStaticSupport: StaticNodeSupport[Node, Snapshot, NodeConstructorRecord] =
    new StaticNodeSupport[Node, Snapshot, NodeConstructorRecord]() {
      override def createNodeArgs(
        snapshot: Option[Snapshot],
        initialJournal: Journal,
        multipleValuesStandingQueryStates: MultipleValuesStandingQueries,
      ): Product = StaticNodeActorSupport.createNodeArgs(snapshot, initialJournal, multipleValuesStandingQueryStates)
    }

  override val edgeCollectionFactory: QuineId => SyncEdgeCollection = EdgeIteration.Unordered.edgeCollectionFactory

  override def effectOrder: EventEffectOrder = EventEffectOrder.MemoryFirst
  override def declineSleepWhenWriteWithinMillis: Long = 0
  override def declineSleepWhenAccessWithinMillis: Long = 0
  override def maxCatchUpSleepMillis: Long = 0
  override def labelsProperty: Symbol = Symbol("__LABEL")
  override def isOnThisHost(quineRef: QuineRef): Boolean = true
  override def isSingleHost: Boolean = true
  override def shards: Iterable[ShardRef] = Iterable.empty
  override def relayTell(quineRef: QuineRef, message: QuineMessage, originalSender: ActorRef): Unit = ()
  override def relayAsk[Resp](
    quineRef: QuineRef,
    unattributedMessage: QuineRef => QuineMessage with AskableQuineMessage[Resp],
    originalSender: ActorRef,
  )(implicit timeout: Timeout, resultHandler: ResultHandler[Resp]): Future[Resp] =
    Future.failed(new NotImplementedError("Override `relayAsk` in instance of FakeQuineGraph"))
  override def isReady: Boolean = true
  override def shutdown(): Future[Unit] = Future.successful(TestKit.shutdownActorSystem(system))
  override def createNamespace(namespace: NamespaceId)(implicit timeout: Timeout): Future[Boolean] =
    Future.failed(new NotImplementedError("Override `createNamespace` in instance of FakeQuineGraph"))
  override def deleteNamespace(namespace: NamespaceId)(implicit timeout: Timeout): Future[Boolean] =
    Future.failed(new NotImplementedError("Override `deleteNamespace` in instance of FakeQuineGraph"))
  override def getNamespaces: collection.Set[NamespaceId] = Set.empty
  override def shardFromNode(node: QuineId): ShardRef = { // Copied from StaticShardGraph. Override if needed.
    val shardIdx = idProvider.nodeLocation(node).shardIdx
    shards.toSeq(Math.floorMod(shardIdx, shards.size))
  }

}
