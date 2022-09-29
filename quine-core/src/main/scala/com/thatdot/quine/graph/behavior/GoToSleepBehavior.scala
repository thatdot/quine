package com.thatdot.quine.graph.behavior

import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.locks.StampedLock

import scala.concurrent.duration.DurationInt
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success}

import akka.actor.{ActorRef, Scheduler}

import com.codahale.metrics.Timer

import com.thatdot.quine.graph._
import com.thatdot.quine.graph.messaging.QuineIdAtTime
import com.thatdot.quine.persistor.codecs.StandingQueryStateCodec
import com.thatdot.quine.persistor.{PersistenceAgent, PersistenceConfig}

trait GoToSleepBehavior extends BaseNodeActorView with ActorClock {

  protected def persistenceConfig: PersistenceConfig

  protected def persistor: PersistenceAgent

  protected def graph: BaseGraph

  protected def toSnapshotBytes(time: EventTime): Array[Byte]

  protected def actorRefLock: StampedLock

  protected def wakefulState: AtomicReference[WakefulState]

  protected def pendingStandingQueryWrites: collection.Set[(StandingQueryId, StandingQueryPartId)]

  protected def standingQueries: collection.Map[
    (StandingQueryId, StandingQueryPartId),
    (StandingQuerySubscribers, cypher.StandingQueryState)
  ]

  protected def lastWriteMillis: Long

  // TODO: retry in persistors
  private def retryPersistence[T](timer: Timer, op: => Future[T], ec: ExecutionContext)(implicit
    scheduler: Scheduler
  ): Future[T] =
    akka.pattern.retry(
      () => timer.time(op),
      attempts = 5,
      minBackoff = 100.millis,
      maxBackoff = 5.seconds,
      randomFactor = 0.5
    )(ec, scheduler)

  /* NB: all of the messages being sent/received in `goToSleep` are to/from the
   *     shard actor. Consequently, it is safe (and more efficient) to use
   *     plain `ActorRef`'s - we don't need to worry about exactly once
   *     delivery since a node and its shard are always on the same machine.
   */
  final protected def goToSleepBehavior(controlMessage: NodeControlMessage): Unit = controlMessage match {

    /* This message is just sent so that the dispatcher knows there are messages
     * to process (we need to "trick" the dispatcher into thinking this because
     * those messages were enqueued directly into the message queue)
     */
    case ProcessMessages => ()

    case SaveSnapshot =>
      val saveFuture = latestUpdateAfterSnapshot match {
        case Some(latestUpdateTime) if persistenceConfig.snapshotEnabled =>
          val snapshotTime = if (!persistenceConfig.snapshotSingleton) latestUpdateTime else EventTime.MaxValue
          val snapshot: Array[Byte] = toSnapshotBytes(latestUpdateTime)
          metrics.snapshotSize.update(snapshot.length)
          retryPersistence(
            metrics.persistorPersistSnapshotTimer,
            persistor.persistSnapshot(qid, snapshotTime, snapshot),
            context.dispatcher
          )(context.system.scheduler)

        case _ => Future.unit
      }
      sender() ! saveFuture

    case GoToSleep =>
      val shardActor: ActorRef = sender()
      // promise tracking updates to shard in-memory map of nodes (completed by the shard)
      val shardPromise = Promise[Unit]()

      def reportSleepSuccess(qidAtTime: QuineIdAtTime): Unit =
        shardActor ! SleepOutcome.SleepSuccess(qidAtTime, shardPromise)

      // Transition out of a `ConsideringSleep` state (if it is still state)

      val newState: WakefulState = wakefulState.updateAndGet {
        case WakefulState.ConsideringSleep(deadline) =>
          val millisNow = latestEventTime().millis
          val tooRecentAccess = graph.declineSleepWhenAccessWithinMillis > 0 &&
            graph.declineSleepWhenAccessWithinMillis > millisNow - previousMillisTime()
          val tooRecentWrite = graph.declineSleepWhenWriteWithinMillis > 0 &&
            graph.declineSleepWhenWriteWithinMillis > millisNow - lastWriteMillis
          if (deadline.hasTimeLeft() && !tooRecentAccess && !tooRecentWrite) {
            WakefulState.GoingToSleep(shardPromise)
          } else {
            WakefulState.Awake
          }
        case other => other
      }

      newState match {
        // Node may just have refused sleep, so the shard must add it back to `inMemoryActorList`
        case WakefulState.Awake =>
          shardActor ! StillAwake(qidAtTime)

        // We must've just set this
        case _: WakefulState.GoingToSleep =>
          // Log something if this (bad) case occurs
          if (latestUpdateAfterSnapshot.isDefined && atTime.nonEmpty) {
            log.error(s"Update occurred on a historical node $atTime (but it won't be persisted)")
          }

          latestUpdateAfterSnapshot match {
            case Some(latestUpdateTime) if persistenceConfig.snapshotOnSleep && atTime.isEmpty =>
              val snapshot: Array[Byte] = toSnapshotBytes(latestUpdateTime)
              metrics.snapshotSize.update(snapshot.length)

              implicit val scheduler: Scheduler = context.system.scheduler

              // Save all persistor data
              val snapshotSaved = retryPersistence(
                metrics.persistorPersistSnapshotTimer,
                persistor.persistSnapshot(
                  qid,
                  if (persistenceConfig.snapshotSingleton) EventTime.MaxValue
                  else latestUpdateTime,
                  snapshot
                ),
                context.dispatcher
              )
              val standingQueryStatesSaved = Future.traverse(pendingStandingQueryWrites) {
                case key @ (globalId, localId) =>
                  val serialized = standingQueries.get(key).map(StandingQueryStateCodec.format.write)
                  serialized.foreach(arr => metrics.standingQueryStateSize(globalId).update(arr.length))
                  retryPersistence(
                    metrics.persistorSetStandingQueryStateTimer,
                    persistor.setStandingQueryState(globalId, qid, localId, serialized),
                    context.dispatcher
                  )
              }(implicitly, context.dispatcher)

              val persistenceFuture = snapshotSaved zip standingQueryStatesSaved

              // Schedule an update to the shard
              persistenceFuture.onComplete {
                case Success(_) => reportSleepSuccess(qidAtTime)
                case Failure(err) =>
                  shardActor ! SleepOutcome.SleepFailed(
                    qidAtTime,
                    snapshot,
                    edges.size,
                    properties.transform((_, v) => v.serialized.length), // this eagerly serializes; can be expensive
                    err,
                    shardPromise
                  )
              }(context.dispatcher)

            case _ =>
              reportSleepSuccess(qidAtTime)
          }

          /* Block waiting for the write lock to the ActorRef
           *
           * This is important: we need to acquire the write lock and then never
           * release it, so that no one can ever acquire the lock. Why? Because
           * the actor ref is about to be permanently invalid.
           */
          // TODO: consider `tryWriteLock` and a transition back to `Awake`?
          actorRefLock.writeLock()
          context.stop(self)

        // The state hasn't changed
        case _ =>
      }
  }
}
