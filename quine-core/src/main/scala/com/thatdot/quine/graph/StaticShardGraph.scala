package com.thatdot.quine.graph

import java.util.concurrent.ConcurrentHashMap

import scala.collection.compat.immutable._
import scala.collection.{concurrent, mutable}
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.jdk.CollectionConverters.ConcurrentMapHasAsScala

import org.apache.pekko.actor.{ActorRef, Props}
import org.apache.pekko.dispatch.Envelope
import org.apache.pekko.util.Timeout

import com.thatdot.quine.graph.GraphShardActor.NodeState
import com.thatdot.quine.graph.messaging.ShardMessage._
import com.thatdot.quine.graph.messaging.{
  AskableQuineMessage,
  LocalShardRef,
  NodeActorMailboxExtension,
  QuineMessage,
  QuineRef,
  ResultHandler,
  ShardRef,
  SpaceTimeQuineId,
  WrappedActorRef
}
import com.thatdot.quine.model.QuineId
import com.thatdot.quine.util.{QuineDispatchers, Retry}

/** Graph implementation that assumes a basic static topology of shards. */
trait StaticShardGraph extends BaseGraph {

  /** Number of shards in the graph
    *
    * Since shards are responsible for waking up and sleeping nodes, the number
    * of shards should be based on the expected rate of nodes being woken/slept.
    */
  def shardCount: Int

  /** Initial in-memory limits (in terms of nodes) of a shard
    *
    * This dictates the starting "capacity" of the shard as well as how much
    * buffer the shard will accept for waking up nodes beyond its desired
    * capacity.
    */
  def initialShardInMemoryLimit: Option[InMemoryNodeLimit]

  // refine [[shards]] to a Seq
  def shards: Seq[LocalShardRef]

  /** Creates an actor for each of the configured static shards, returning the array of shards.
    * This is a function rather than inlined in the `val shards = ...` to resolve an initialization order issue
    */
  protected[this] def initializeShards(): ArraySeq[LocalShardRef] =
    ArraySeq.unsafeWrapArray(Array.tabulate(shardCount) { (shardId: Int) =>
      logger.info(s"Adding a new local shard at idx: $shardId")

      val nodeMap: mutable.Map[NamespaceId, concurrent.Map[SpaceTimeQuineId, GraphShardActor.NodeState]] =
        mutable.Map(defaultNamespaceId -> new ConcurrentHashMap[SpaceTimeQuineId, NodeState]().asScala)

      val localRef: ActorRef = system.actorOf(
        Props(new GraphShardActor(this, shardId, nodeMap, initialShardInMemoryLimit))
          .withMailbox("pekko.quine.shard-mailbox")
          .withDispatcher(QuineDispatchers.shardDispatcherName),
        name = GraphShardActor.name(shardId)
      )

      new LocalShardRef(localRef, shardId, nodeMap)
    })

  def relayTell(
    quineRef: QuineRef,
    message: QuineMessage,
    originalSender: ActorRef = ActorRef.noSender
  ): Unit = {
    metrics.relayTellMetrics.markLocal()
    quineRef match {
      case qidAtTime: SpaceTimeQuineId =>
        val shardIdx = idProvider.nodeLocation(qidAtTime.id).shardIdx
        val shard: LocalShardRef = shards(Math.floorMod(shardIdx, shards.length))

        // Try sending the message straight to the node
        val sentDirectTell = shard.withLiveActorRef(qidAtTime, _.tell(message, originalSender))

        // If that fails, manually enqueue the message and request the shard wake the node up
        if (!sentDirectTell) {
          val envelope = Envelope(message, originalSender, system)
          NodeActorMailboxExtension(system).enqueueIntoMessageQueueAndWakeup(qidAtTime, shard.localRef, envelope)
        }

      case wrappedRef: WrappedActorRef =>
        wrappedRef.ref.tell(message, originalSender)
    }
  }

  def relayAsk[Resp](
    quineRef: QuineRef,
    unattributedMessage: QuineRef => QuineMessage with AskableQuineMessage[Resp],
    originalSender: ActorRef = ActorRef.noSender
  )(implicit
    timeout: Timeout,
    resultHandler: ResultHandler[Resp]
  ): Future[Resp] = {
    require(timeout.duration.length >= 0)
    val promise = Promise[Resp]()
    quineRef match {
      case qidAtTime: SpaceTimeQuineId =>
        val shardIdx = idProvider.nodeLocation(qidAtTime.id).shardIdx
        val shard: LocalShardRef = shards(Math.floorMod(shardIdx, shards.length))

        val askActorRef = system.actorOf(
          Props(
            new messaging.ExactlyOnceAskNodeActor(
              unattributedMessage,
              qidAtTime,
              remoteShardTarget = None,
              idProvider,
              originalSender,
              promise,
              timeout.duration,
              resultHandler,
              metrics.relayAskMetrics
            )
          ).withDispatcher(QuineDispatchers.nodeDispatcherName)
        )
        val askQuineRef = WrappedActorRef(askActorRef)
        val message = unattributedMessage(askQuineRef)

        // Try sending the message straight to the node
        val sentDirectTell = shard.withLiveActorRef(qidAtTime, _.tell(message, originalSender))

        // If that fails, manually enqueue the message and request the shard wake the node up
        if (!sentDirectTell) {
          val envelope = Envelope(message, originalSender, system)
          NodeActorMailboxExtension(system).enqueueIntoMessageQueueAndWakeup(qidAtTime, shard.localRef, envelope)
        }

      case wrappedRef: WrappedActorRef =>
        // Destination for response
        val askActorRef = system.actorOf(
          Props(
            new messaging.ExactlyOnceAskActor[Resp](
              unattributedMessage,
              wrappedRef.ref,
              false,
              originalSender,
              promise,
              timeout.duration,
              resultHandler,
              metrics.relayAskMetrics
            )
          ).withDispatcher(QuineDispatchers.nodeDispatcherName)
        )

        // Send the message directly
        val message = unattributedMessage(WrappedActorRef(askActorRef))
        wrappedRef.ref.tell(message, originalSender)
    }

    metrics.relayAskMetrics.markLocal()
    promise.future
  }

  def shutdown(): Future[Unit] = {
    val maxPollAttempts = 100
    val delayBetweenPollAttempts = 250.millis

    implicit val ec: ExecutionContext = nodeDispatcherEC

    // Send all shards a signal to shutdown nodes and get back a progress update
    def pollShutdownProgress(): Future[Int] = Future
      .traverse(shards) { (shard: LocalShardRef) =>
        relayAsk(shard.quineRef, InitiateShardShutdown(_))(5.seconds, implicitly)
      }
      .map(_.view.map(_.remainingNodeActorCount).sum)

    Retry
      .until[Int](
        pollShutdownProgress(),
        _ == 0,
        maxPollAttempts,
        delayBetweenPollAttempts,
        system.scheduler
      )(ec)
      .flatMap(_ => namespacePersistor.syncVersion())
      .flatMap(_ => namespacePersistor.shutdown())
  }

  def isOnThisHost(quineRef: QuineRef): Boolean = true

  def isSingleHost = true

  def shardFromNode(qid: QuineId): ShardRef = {
    val shardIdx = idProvider.nodeLocation(qid).shardIdx
    shards(Math.floorMod(shardIdx, shards.length))
  }
}
