package com.thatdot.quine.graph

import java.util.concurrent.{Callable, ConcurrentHashMap}

import scala.collection.compat.immutable._
import scala.collection.concurrent
import scala.concurrent._
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

import akka.actor._
import akka.dispatch.Envelope
import akka.pattern._
import akka.util.Timeout

import com.thatdot.quine.graph.messaging.ShardMessage._
import com.thatdot.quine.graph.messaging.{
  AskableQuineMessage,
  LocalShardRef,
  NodeActorMailboxExtension,
  QuineIdAtTime,
  QuineMessage,
  QuineRef,
  ResultHandler,
  WrappedActorRef
}

/** Graph implementation that assumes a basic static topology of shards. */
trait StaticShardGraph extends BaseGraph {

  /** Number of shards in the graph
    *
    * Since shards are reponsible for waking up and sleeping nodes, the number
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

  val shards: ArraySeq[LocalShardRef] =
    ArraySeq.unsafeWrapArray(Array.tabulate(shardCount) { (shardId: Int) =>
      logger.info(s"Adding a new local shard at idx: $shardId")

      val shardMap: concurrent.Map[QuineIdAtTime, GraphShardActor.NodeState] =
        new ConcurrentHashMap[QuineIdAtTime, GraphShardActor.NodeState]().asScala

      val localRef: ActorRef = system.actorOf(
        Props(new GraphShardActor(this, shardId, shardMap, initialShardInMemoryLimit))
          .withMailbox("akka.quine.shard-mailbox")
          .withDispatcher("akka.quine.graph-shard-dispatcher"),
        name = GraphShardActor.name(shardId)
      )

      new LocalShardRef(localRef, shardId, shardMap)
    })

  def relayTell(
    quineRef: QuineRef,
    message: QuineMessage,
    originalSender: ActorRef = ActorRef.noSender
  ): Unit =
    quineRef match {
      case qidAtTime: QuineIdAtTime =>
        val shardIdx = idProvider.nodeLocation(qidAtTime.id).shardIdx
        val shard: LocalShardRef = shards(Math.floorMod(shardIdx, shards.length))

        // Try sending the message straight to the node
        val sentDirectTell = shard.withLiveActorRef(qidAtTime, _.tell(message, originalSender))

        // If that fails, manually enqueue the message and request the shard wake the node up
        if (!sentDirectTell) {
          val envelope = Envelope(message, originalSender, system)
          NodeActorMailboxExtension(system).enqueueIntoMessageQueue(qidAtTime, shard.localRef, envelope)
        }

      case wrappedRef: WrappedActorRef =>
        wrappedRef.ref.tell(message, originalSender)
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
      case qidAtTime: QuineIdAtTime =>
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
              resultHandler
            )
          ).withDispatcher("akka.quine.node-dispatcher")
        )
        val askQuineRef = WrappedActorRef(askActorRef)
        val message = unattributedMessage(askQuineRef)

        // Try sending the message straight to the node
        val sentDirectTell = shard.withLiveActorRef(qidAtTime, _.tell(message, originalSender))

        // If that fails, manually enqueue the message and request the shard wake the node up
        if (!sentDirectTell) {
          val envelope = Envelope(message, originalSender, system)
          NodeActorMailboxExtension(system).enqueueIntoMessageQueue(qidAtTime, shard.localRef, envelope)
        }
        promise.future

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
              resultHandler
            )
          ).withDispatcher("akka.quine.node-dispatcher")
        )

        // Send the message directly
        val message = unattributedMessage(WrappedActorRef(askActorRef))
        wrappedRef.ref.tell(message, originalSender)
    }
    promise.future
  }

  def shutdown(): Future[Unit] = {
    val MaxPollAttemps = 100
    val DelayBetweenPollAttempts = 250.millis

    // Send all shards a signal to shutdown nodes and get back a progress update
    val pollShutdownProgress: Callable[Future[Unit]] = () => {
      Future
        .traverse(shards) { (shard: LocalShardRef) =>
          relayAsk(shard.quineRef, InitiateShardShutdown(_))(5.seconds, implicitly)
        }
        .map(_.view.map(_.remainingNodeActorCount).sum)
        .filter(_ == 0)
        .map(_ => ())
    }
    for {
      _ <- Patterns.retry(
        pollShutdownProgress,
        MaxPollAttemps,
        DelayBetweenPollAttempts,
        system.scheduler,
        system.dispatcher
      )
      _ <- persistor.syncVersion()
      _ <- persistor.shutdown()
      _ <- system.terminate()
    } yield ()
  }

  def isOnThisHost(quineRef: QuineRef): Boolean = true

  def isSingleHost = true
}
