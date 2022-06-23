package com.thatdot.quine.graph

import java.util.function.Supplier

import scala.concurrent.duration.DurationInt
import scala.concurrent.{ExecutionContext, Future}

import akka.NotUsed
import akka.actor.{ActorRef, ActorSystem}
import akka.stream.Materializer
import akka.stream.scaladsl.{Flow, Sink, Source}
import akka.util.Timeout

import com.typesafe.scalalogging.StrictLogging

import com.thatdot.quine.graph.edgecollection.EdgeCollection
import com.thatdot.quine.graph.messaging.ShardMessage.RequestNodeSleep
import com.thatdot.quine.graph.messaging.{
  AskableQuineMessage,
  QuineIdAtTime,
  QuineMessage,
  QuineRef,
  ResultHandler,
  ShardMessage,
  ShardRef
}
import com.thatdot.quine.model.{Milliseconds, QuineId, QuineIdProvider}
import com.thatdot.quine.persistor.{EmptyPersistor, EventEffectOrder, PersistenceAgent}
import com.thatdot.quine.util.{SharedValve, ValveFlow}

trait BaseGraph extends StrictLogging {

  def system: ActorSystem

  implicit val shardDispatcherEC: ExecutionContext =
    system.dispatchers.lookup("akka.quine.graph-shard-dispatcher")

  implicit val materializer: Materializer =
    Materializer.matFromSystem(system)

  def idProvider: QuineIdProvider

  def persistor: PersistenceAgent

  val metrics: HostQuineMetrics

  /** Class of nodes in the graph
    *
    * This must have a constructor that has the following arguments (in order):
    *
    *   - `QuineIdAtTime`: node and time being tracked
    *   - `_ <: BaseGraph`: handle to the graph
    *   - `CostToSleep`: shard/node shared notion of how costly the node is to sleep
    *   - `AtomicReference[WakefulState]`: shard/node shared notion of node state
    *   - `StampedLock`: lock used to safely send messages to the actor
    *   - `Option[Array[Byte]]`: optional snapshot from which to restore
    *
    * The handle to the graph is usually a more specific type than `BaseGraph`,
    * constrained by behaviors that are mixed into the node class.
    */
  def nodeClass: Class[_]

  /** Method for initializing edge collections */
  val edgeCollectionFactory: Supplier[EdgeCollection]

  // TODO: put this in some other class which is a field here
  val ingestValve: SharedValve = new SharedValve("ingest")
  metrics.registerGaugeValve(ingestValve)

  val masterStream: MasterStream = new MasterStream(materializer)

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

  /** @return whether the graph is in an operational state */
  def isReady: Boolean

  /** Require the graph is ready and throw an exception if it isn't */
  @throws[GraphNotReadyException]("if the graph is not ready")
  def requiredGraphIsReady(): Unit =
    if (!isReady) {
      throw new GraphNotReadyException()
    }

  /** Controlled shutdown of the graph
    *
    * @return future that completes when the graph is shut down
    */
  def shutdown(): Future[Unit]

  /** Assert that the graph must have a node type with the specified behavior
    * as a supertype.
    *
    * This is only to guard against creation of graphs which claim to have
    * certain classes of functionality, but where the node type does not
    * handle the messages that would be needed for that functionality.
    *
    * @param context where is the requirement coming from?
    * @param clazz class of the behaviour
    */
  @throws[IllegalArgumentException]("node type does not implement the specified behaviour")
  def requireBehavior[T](context: String, clazz: Class[T]): Unit =
    if (!clazz.isAssignableFrom(nodeClass)) {
      throw new IllegalArgumentException(s"$context requires the type of nodes extend ${clazz.getSimpleName}")
    }

  /** Uses the appropriate persistor method (journals or snapshot) to enumerate all node IDs.
    * Augments the list with in-memory nodes that may not yet have reached the persistor yet.
    */
  def enumerateAllNodeIds(): Source[QuineId, NotUsed] = {
    requiredGraphIsReady()
    if (!persistor.persistenceConfig.journalEnabled || persistor.isInstanceOf[EmptyPersistor]) {
      // TODO: don't hardcode
      implicit val timeout = Timeout(5.seconds)

      // Collect nodes that may be only in memory
      val inMemoryNodesFut: Future[Set[QuineId]] = Future
        .traverse(shards) { shardRef =>
          val awakeNodes = relayAsk(shardRef.quineRef, ShardMessage.SampleAwakeNodes(limit = None, atTime = None, _))
          Source
            .futureSource(awakeNodes)
            .map(_.quineId)
            .runWith(Sink.collection[QuineId, Set[QuineId]])
        }
        .map(_.foldLeft(Set.empty[QuineId])(_ union _))

      // Return those nodes, plus the ones the persistor produces
      val combinedSource = Source.futureSource {
        inMemoryNodesFut.map { (inMemoryNodes: Set[QuineId]) =>
          val persistorNodes =
            persistor.enumerateSnapshotNodeIds().filterNot(inMemoryNodes.contains)
          Source(inMemoryNodes) ++ persistorNodes
        }
      }

      combinedSource.mapMaterializedValue(_ => NotUsed)
    } else {
      persistor.enumerateJournalNodeIds()
    }
  }

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
    atTime: Option[Milliseconds] = None
  )(implicit timeout: Timeout): Future[Set[QuineId]] = {
    requiredGraphIsReady()
    val shardAskSizes: List[Int] = {
      val n = shards.size
      val quot = limit / n
      val rem = limit % n
      List.fill(n - rem)(quot) ++ List.fill(rem)(quot + 1)
    }

    Future
      .traverse(shards zip shardAskSizes) { case (shard, lim) =>
        Source
          .futureSource(relayAsk(shard.quineRef, ShardMessage.SampleAwakeNodes(Some(lim), atTime, _)))
          .map(_.quineId)
          .runWith(Sink.collection[QuineId, Set[QuineId]])
      }
      .map(_.foldLeft(Set.empty[QuineId])(_ union _))
  }

  /** Snapshot nodes that are currently awake
    */
  def snapshotInMemoryNodes()(implicit timeout: Timeout): Future[Unit] = {
    requiredGraphIsReady()
    Future
      .traverse(shards) { (shard: ShardRef) =>
        relayAsk(shard.quineRef, ShardMessage.SnapshotInMemoryNodes)
      }
      .map(_.foreach {
        case ShardMessage.SnapshotFailed(shardId, msg) =>
          logger.error(s"Shard: $shardId failed to snapshot nodes: $msg")

        case ShardMessage.SnapshotSucceeded(idFailures) =>
          for ((id, msg) <- idFailures)
            logger.error(s"Node ${id.debug(idProvider)} failed to snapshot: $msg")
      })
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
            case None => relayAsk(shardRef.quineRef, ShardMessage.GetInMemoryLimits(_))
            case Some(newLimit) => relayAsk(shardRef.quineRef, ShardMessage.UpdateInMemoryLimits(newLimit, _))
          }
        remainingAdjustments -= shardId
        shardId -> sendMessageToShard
      }

    if (!remainingAdjustments.isEmpty) {
      val msg = s"The following shards do not exist: ${remainingAdjustments.keys.mkString(", ")}"
      Future.failed(new IllegalArgumentException(msg))
    } else {
      Future
        .traverse(messages) { case (shardId, sendMessageToShard) =>
          sendMessageToShard().map { case ShardMessage.CurrentInMemoryLimits(limits) => shardId -> limits }
        }
        .map(_.toMap)
    }
  }

  /** Request that a node go to sleep by sending a message to the node's shard.
    */
  def requestNodeSleep(quineId: QuineId)(implicit timeout: Timeout): Future[Unit] = {
    requiredGraphIsReady()
    val shard = shardFromNode(quineId)
    relayAsk(shard.quineRef, RequestNodeSleep(QuineIdAtTime(quineId, None), _))
      .map(_ => ())
  }

  /** Lookup the shard for a node ID.
    */
  def shardFromNode(node: QuineId): ShardRef
}
