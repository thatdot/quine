package com.thatdot.quine.graph

import scala.concurrent.duration.DurationInt
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters.SetHasAsScala
import scala.reflect.{ClassTag, classTag}

import org.apache.pekko.NotUsed
import org.apache.pekko.actor.{ActorRef, ActorSystem}
import org.apache.pekko.dispatch.MessageDispatcher
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.{Flow, Sink, Source}
import org.apache.pekko.util.Timeout

import com.thatdot.quine.graph.edges.SyncEdgeCollection
import com.thatdot.quine.graph.messaging.LiteralMessage.GetNodeHashCode
import com.thatdot.quine.graph.messaging.ShardMessage.RequestNodeSleep
import com.thatdot.quine.graph.messaging.{
  AskableQuineMessage,
  LocalShardRef,
  QuineMessage,
  QuineRef,
  ResultHandler,
  ShardMessage,
  ShardRef,
  SpaceTimeQuineId
}
import com.thatdot.quine.model.{Milliseconds, QuineId, QuineIdProvider}
import com.thatdot.quine.persistor.{EmptyPersistor, EventEffectOrder, PrimePersistor, WrappedPersistenceAgent}
import com.thatdot.quine.util.Log._
import com.thatdot.quine.util.{QuineDispatchers, SharedValve, ValveFlow}

trait BaseGraph extends StrictSafeLogging {

  def system: ActorSystem

  def dispatchers: QuineDispatchers

  def shardDispatcherEC: MessageDispatcher = dispatchers.shardDispatcherEC
  def nodeDispatcherEC: MessageDispatcher = dispatchers.nodeDispatcherEC
  def blockingDispatcherEC: MessageDispatcher = dispatchers.blockingDispatcherEC

  implicit val materializer: Materializer =
    Materializer.matFromSystem(system)

  def idProvider: QuineIdProvider

  val namespacePersistor: PrimePersistor

  implicit protected def logConfig: LogConfig

  val metrics: HostQuineMetrics

  type Node <: AbstractNodeActor
  type Snapshot <: AbstractNodeSnapshot
  type NodeConstructorRecord <: Product
  def nodeStaticSupport: StaticNodeSupport[Node, Snapshot, NodeConstructorRecord]

  /** Method for initializing edge collections */
  val edgeCollectionFactory: QuineId => SyncEdgeCollection

  // TODO: put this in some other class which is a field here
  val ingestValve: SharedValve = new SharedValve("ingest")
  metrics.registerGaugeValve(ingestValve)

  val masterStream: MasterStream = new MasterStream(materializer)(logConfig)

  def ingestThrottleFlow[A]: Flow[A, A, NotUsed] = Flow.fromGraph(new ValveFlow[A](ingestValve))

  /** Strategy for choosing whether to apply effects in memory before confirming write to persistence, or to write to
    * persistence first, and then apply in-memory effects after on-disk storage succeeds.
    */
  def effectOrder: EventEffectOrder

  /** Nodes will decline sleep if the last write to the node occurred less than
    * this many milliseconds ago (according to the actor's clock).
    *
    * Setting this to `0` means sleep will never be declined due to a recent
    * write (effectively disabling the setting).
    */
  def declineSleepWhenWriteWithinMillis: Long

  /** Nodes will decline sleep if the last read to the node occurred less than
    * this many milliseconds ago (according to the actor's clock).
    *
    * Setting this to `0` means sleep will never be declined due to a recent
    * write (effectively disabling the setting).
    */
  def declineSleepWhenAccessWithinMillis: Long

  /** Nodes will wait up to this amount of milliseconds before processing messages
    * when at-time is in the future. This can occur when there is difference in
    * the system clock across nodes in the cluster.
    */
  def maxCatchUpSleepMillis: Long

  /** Property on a node that gets used to store the list of strings that make
    * up the node's labels.
    *
    * TODO: document what happens when trying to access such a label directly
    */
  def labelsProperty: Symbol

  /** Determine where a certain destination is on the local JVM or not
    *
    * @param quineRef sendable destination
    * @return whether the destination is on this host
    */
  def isOnThisHost(quineRef: QuineRef): Boolean

  /** Is the logical graph entirely contained in this host?
    */
  def isSingleHost: Boolean

  /** Shards in the graph
    */
  def shards: Iterable[ShardRef]

  /** Route a [[QuineMessage]] to some location in the Quine graph.
    *
    * This abstracts over the details of the protocols involved in reaching
    * different entities in Quine and the protocols involved in cross-JVM
    * message delivery guarantees.
    *
    * @param quineRef destination of the message
    * @param message the message to deliver
    * @param originalSender from who was the message originally (for debug only)
    */
  def relayTell(
    quineRef: QuineRef,
    message: QuineMessage,
    originalSender: ActorRef = ActorRef.noSender
  ): Unit

  /** Route a message to some location in the Quine graph and return the answer
    *
    * This abstracts over the details of the protocols involved in reaching
    * different entities in Quine and the protocols involved in cross-JVM
    * message delivery guarantees.
    *
    * TODO: instead of timeout, require the asked message extend `Expires`?
    *
    * @param quineRef destination of the message
    * @param unattributedMessage the message to delivery
    * @param originalSender from who was the message originally (for debug only)
    * @return the reply we get back
    */
  def relayAsk[Resp](
    quineRef: QuineRef,
    unattributedMessage: QuineRef => QuineMessage with AskableQuineMessage[Resp],
    originalSender: ActorRef = ActorRef.noSender
  )(implicit
    timeout: Timeout,
    resultHandler: ResultHandler[Resp]
  ): Future[Resp]

  /** @return whether the graph is in an operational state and ready to receive input like ingest, API calls, queries */
  def isReady: Boolean

  /** Require the graph is ready and throw an exception if it isn't */
  @throws[GraphNotReadyException]("if the graph is not ready")
  def requiredGraphIsReady(): Unit =
    if (!isReady) {
      throw new GraphNotReadyException()
    }

  /** Run code in the provided Future if the graphis ready, or short-circuit and return a failed Future immediately. */
  def requiredGraphIsReadyFuture[A](f: => Future[A]): Future[A] =
    if (isReady) f
    else Future.failed(new GraphNotReadyException())

  /** Controlled shutdown of the graph
    *
    * @return future that completes when the graph is shut down
    */
  def shutdown(): Future[Unit]

  /** Make a new namespace. The outer future indicates success or failure. The inner Boolean indicates whether a
    * change was made.
    */
  def createNamespace(namespace: NamespaceId)(implicit timeout: Timeout): Future[Boolean]

  /** Remove an existing namespace. The outer future indicates success or failure. The inner Boolean indicates whether
    * a change was made.
    */
  def deleteNamespace(namespace: NamespaceId)(implicit timeout: Timeout): Future[Boolean]

  /** Get a set of existing namespaces. This is served by a local cache and meant to be fast and inexpensive.
    * `getNamespaces.contains(myNamespace)` can be called before every operation that uses a non-default namespace to
    * ensure the namespace exists, or otherwise fail fast before other actions.
    */
  def getNamespaces: collection.Set[NamespaceId]

  private[graph] val namespaceCache: scala.collection.mutable.Set[NamespaceId] =
    new java.util.concurrent.ConcurrentHashMap[NamespaceId, java.lang.Boolean]().keySet(true).asScala
  namespaceCache.add(defaultNamespaceId)

  /** Assert that the graph must have a node type with the specified behavior
    * as a supertype.
    *
    * This is only to guard against creation of graphs which claim to have
    * certain classes of functionality, but where the node type does not
    * handle the messages that would be needed for that functionality.
    *
    * @param context where is the requirement coming from?
    * @param clazz class of the behaviour
    *
    * TODO it should be possible to replace all runtime instances of this function with compile-time checks using
    *      something like an Aux pattern on BaseGraph
    */
  @throws[IllegalArgumentException]("node type does not implement the specified behaviour")
  def requireBehavior[C: ClassTag, T: ClassTag]: Unit =
    if (!classTag[T].runtimeClass.isAssignableFrom(nodeStaticSupport.nodeClass.runtimeClass)) {
      throw new IllegalArgumentException(
        s"${classTag[C].runtimeClass.getSimpleName} requires the type of nodes extend ${classTag[T].runtimeClass.getSimpleName}"
      )
    }

  /** Uses the appropriate persistor method (journals or snapshot) to enumerate all node IDs.
    * Augments the list with in-memory nodes that may not yet have reached the persistor yet.
    */
  def enumerateAllNodeIds(namespace: NamespaceId): Source[QuineId, NotUsed] =
    if (
      !namespacePersistor.persistenceConfig.journalEnabled ||
      WrappedPersistenceAgent.unwrap(namespacePersistor.getDefault).isInstanceOf[EmptyPersistor]
    ) {
      // TODO: don't hardcode
      implicit val timeout = Timeout(5.seconds)

      // Collect nodes that may be only in memory
      val inMemoryNodesFut: Future[Set[QuineId]] = Future
        .traverse(shards) { shardRef =>
          val awakeNodes =
            relayAsk(shardRef.quineRef, ShardMessage.SampleAwakeNodes(namespace, limit = None, atTime = None, _))
          Source
            .futureSource(awakeNodes)
            .map(_.quineId)
            .named(s"all-recent-node-scan-shard-${shardRef.shardId}")
            .runWith(Sink.collection[QuineId, Set[QuineId]])
        }(implicitly, shardDispatcherEC)
        .map(_.reduce(_ union _))(shardDispatcherEC)

      // Return those nodes, plus the ones the persistor produces
      val combinedSource = Source.futureSource {
        inMemoryNodesFut.map { (inMemoryNodes: Set[QuineId]) =>
          val persistorNodes = namespacePersistor(namespace).fold(Source.empty[QuineId])(
            _.enumerateSnapshotNodeIds().filterNot(inMemoryNodes.contains)
          )
          Source(inMemoryNodes) ++ persistorNodes
        }(shardDispatcherEC)
      }
      combinedSource.mapMaterializedValue(_ => NotUsed).named("all-node-scan-snapshot-based")
    } else {
      namespacePersistor(namespace)
        .fold(Source.empty[QuineId])(_.enumerateJournalNodeIds())
        .named("all-node-scan-journal-based")
    }

  /** Determines if the node by its [[QuineId]] belongs to this [[BaseGraph]].
    *
    * @note except in a clustered setting, all nodes are local
    */
  def isLocalGraphNode(qid: QuineId): Boolean = true

  /** Convenience method for getting some [[com.thatdot.quine.model.QuineId]]'s corresponding
    * to nodes that were recently touched (since they are not yet sleeping). (best effort)
    *
    * TODO: should/can we enforce that this is a subset of `enumerateAllNodeIds`?
    *
    * @param limit return no more than this number of nodes (may return less)
    * @param atTime the historical moment to query, or None for the moving present
    */
  def recentNodes(
    limit: Int,
    namespace: NamespaceId,
    atTime: Option[Milliseconds] = None
  )(implicit timeout: Timeout): Future[Set[QuineId]] = {
    val shardAskSizes: List[Int] = {
      val n = shards.size
      if (n == 0) {
        List.empty
      } else {
        val quot = limit / n
        val rem = limit % n
        List.fill(n - rem)(quot) ++ List.fill(rem)(quot + 1)
      }
    }

    Future
      .traverse(shards zip shardAskSizes) { case (shard, lim) =>
        Source
          .futureSource(relayAsk(shard.quineRef, ShardMessage.SampleAwakeNodes(namespace, Some(lim), atTime, _)))
          .map(_.quineId)
          .named(s"recent-node-sampler-shard-${shard.shardId}")
          .runWith(Sink.collection[QuineId, Set[QuineId]])
      }(implicitly, shardDispatcherEC)
      .map(_.foldLeft(Set.empty[QuineId])(_ union _))(shardDispatcherEC)
  }

  /** Get the in-memory limits for all the shards of this graph, possibly
    * updating some of those limits along the way
    *
    * @param updates Map from shard index to new requested in memory node limit. Although it is
    *                not necessary for every shard to be in this map, every shard in this map
    *                must be in the graph (an invalid index will return a failed future).
    * @return mapping from every shard in the graph to their (possibly updated) in-memory limits
    */
  def shardInMemoryLimits(
    updates: Map[Int, InMemoryNodeLimit]
  )(implicit
    timeout: Timeout
  ): Future[Map[Int, Option[InMemoryNodeLimit]]] = {

    // Build up a list of messages to send (but wait to send them, in case we find `updates` was invalid)
    var remainingAdjustments = updates
    val messages: List[(Int, () => Future[ShardMessage.CurrentInMemoryLimits])] =
      shards.toList.map { (shardRef: ShardRef) =>
        val shardId = shardRef.shardId
        val sendMessageToShard = () =>
          updates.get(shardId) match {
            case None => relayAsk(shardRef.quineRef, ShardMessage.GetInMemoryLimits)
            case Some(newLimit) => relayAsk(shardRef.quineRef, ShardMessage.UpdateInMemoryLimits(newLimit, _))
          }
        remainingAdjustments -= shardId
        shardId -> sendMessageToShard
      }

    if (remainingAdjustments.nonEmpty) {
      val msg = s"The following shards do not exist: ${remainingAdjustments.keys.mkString(", ")}"
      Future.failed(new IllegalArgumentException(msg))
    } else {
      Future
        .traverse(messages) { case (shardId, sendMessageToShard) =>
          sendMessageToShard().map { case ShardMessage.CurrentInMemoryLimits(limits) => shardId -> limits }(
            shardDispatcherEC
          )
        }(implicitly, shardDispatcherEC)
        .map(_.toMap)(shardDispatcherEC)
    }
  }

  /** Request that a node go to sleep by sending a message to the node's shard.
    */
  def requestNodeSleep(namespace: NamespaceId, quineId: QuineId)(implicit timeout: Timeout): Future[Unit] = {
    val shard = shardFromNode(quineId)
    relayAsk(shard.quineRef, RequestNodeSleep(SpaceTimeQuineId(quineId, namespace, None), _))
      .map(_ => ())(shardDispatcherEC)
  }

  /** Lookup the shard for a node ID.
    */
  def shardFromNode(node: QuineId): ShardRef

  /** Asynchronously compute a hash of the state of all nodes in the graph
    * at the optionally specified time. Caller should ensure the graph is
    * sufficiently stable and consistent before calling this function.
    */
  def getGraphHashCode(namespace: NamespaceId, atTime: Option[Milliseconds]): Future[Long] =
    enumerateAllNodeIds(namespace)
      .mapAsyncUnordered(parallelism = 16) { qid =>
        val timeout = 1.second
        val resultHandler = implicitly[ResultHandler[GraphNodeHashCode]]
        val ec = ExecutionContext.parasitic
        relayAsk(SpaceTimeQuineId(qid, namespace, atTime), GetNodeHashCode)(timeout, resultHandler).map(_.value)(ec)
      }
      .runFold(zero = 0L)((e, f) => e + f)

  def tellAllShards(message: QuineMessage): Unit =
    shards.foreach(shard => relayTell(shard.quineRef, message))

  def askAllShards[Resp](
    message: QuineRef => QuineMessage with AskableQuineMessage[Resp]
  )(implicit
    timeout: Timeout,
    resultHandler: ResultHandler[Resp]
  ): Future[Vector[Resp]] = Future.traverse(shards.toVector) { shard =>
    relayAsk(shard.quineRef, message)
  }(implicitly, shardDispatcherEC)

  def askLocalShards[Resp](
    message: QuineRef => QuineMessage with AskableQuineMessage[Resp]
  )(implicit
    timeout: Timeout,
    resultHandler: ResultHandler[Resp]
  ): Future[Vector[Resp]] = Future
    .sequence(shards.collect { case shard: LocalShardRef =>
      relayAsk(shard.quineRef, message)
    }.toVector)(implicitly, shardDispatcherEC)
}
