package com.thatdot.quine.graph.behavior

import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.locks.StampedLock

import scala.concurrent.duration.DurationInt
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success}

import org.apache.pekko.actor.{ActorRef, Scheduler}

import com.codahale.metrics.Timer
import org.apache.pekko

import com.thatdot.quine.graph._
import com.thatdot.quine.graph.cypher.MultipleValuesStandingQueryState
import com.thatdot.quine.graph.edges.EdgeProcessor
import com.thatdot.quine.graph.messaging.SpaceTimeQuineId
import com.thatdot.quine.graph.metrics.implicits.TimeFuture
import com.thatdot.quine.persistor.codecs.MultipleValuesStandingQueryStateCodec
import com.thatdot.quine.persistor.{NamespacedPersistenceAgent, PersistenceConfig}
import com.thatdot.quine.util.Log._
import com.thatdot.quine.util.Log.implicits._

trait GoToSleepBehavior extends BaseNodeActorView with ActorClock {

  protected def edges: EdgeProcessor

  protected def persistenceConfig: PersistenceConfig

  protected def persistor: NamespacedPersistenceAgent

  protected def graph: BaseGraph

  protected def toSnapshotBytes(time: EventTime): Array[Byte]

  protected def actorRefLock: StampedLock

  protected def wakefulState: AtomicReference[WakefulState]

  protected def pendingMultipleValuesWrites: collection.Set[(StandingQueryId, MultipleValuesStandingQueryPartId)]

  protected def multipleValuesStandingQueries: collection.Map[
    (StandingQueryId, MultipleValuesStandingQueryPartId),
    (MultipleValuesStandingQueryPartSubscription, MultipleValuesStandingQueryState),
  ]

  protected def lastWriteMillis: Long

  // TODO: retry in persistors
  private def retryPersistence[T](timer: Timer, op: => Future[T], ec: ExecutionContext)(implicit
    scheduler: Scheduler,
  ): Future[T] =
    pekko.pattern.retry(
      () => timer.time(op),
      attempts = 5,
      minBackoff = 100.millis,
      maxBackoff = 5.seconds,
      randomFactor = 0.5,
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

    case GoToSleep =>
      val shardActor: ActorRef = sender()
      // promise tracking updates to shard in-memory map of nodes (completed by the shard)
      val shardPromise = Promise[Unit]()

      def reportSleepSuccess(qidAtTime: SpaceTimeQuineId, timer: Timer.Context): Unit = {
        metrics.nodePropertyCounter(namespace).bucketContaining(properties.size).dec()
        edges.onSleep()
        shardActor ! SleepOutcome.SleepSuccess(qidAtTime, shardPromise, timer)
      }

      // Transition out of a `ConsideringSleep` state (if it is still state)
      // Invariant: `newState` is NOT `WakefulState.ConsideringSleep`
      val newState = wakefulState.updateAndGet {
        case WakefulState.ConsideringSleep(deadline, sleepTimer, wakeTimer) =>
          val millisNow = System.currentTimeMillis()
          val tooRecentAccess = graph.declineSleepWhenAccessWithinMillis > 0 &&
            graph.declineSleepWhenAccessWithinMillis > millisNow - previousMessageMillis()
          val tooRecentWrite = graph.declineSleepWhenWriteWithinMillis > 0 &&
            graph.declineSleepWhenWriteWithinMillis > millisNow - lastWriteMillis
          if (deadline.hasTimeLeft() && !tooRecentAccess && !tooRecentWrite) {
            WakefulState.GoingToSleep(shardPromise, sleepTimer)
          } else {
            WakefulState.Awake(wakeTimer)
          }
        case goingToSleep: WakefulState.GoingToSleep =>
          goingToSleep

        case awake: WakefulState.Awake => awake
      }

      newState match {
        // Node may just have refused sleep, so the shard must add it back to `inMemoryActorList`
        case _: WakefulState.Awake =>
          shardActor ! StillAwake(qidAtTime)

        // We must've just set this
        case WakefulState.GoingToSleep(shardPromise @ _, sleepTimer) =>
          // Log something if this (bad) case occurs
          if (latestUpdateAfterSnapshot.isDefined && atTime.nonEmpty) {
            log.error(
              safe"Update occurred on a historical node with timestamp: ${Safe(atTime)} (but it won't be persisted)",
            )
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
                  snapshot,
                ),
                context.dispatcher,
              )
              val multipleValuesStatesSaved = Future.traverse(pendingMultipleValuesWrites) {
                case key @ (globalId, localId) =>
                  val serialized =
                    multipleValuesStandingQueries.get(key).map(MultipleValuesStandingQueryStateCodec.format.write)
                  serialized.foreach(arr => metrics.standingQueryStateSize(namespace, globalId).update(arr.length))
                  retryPersistence(
                    metrics.persistorSetStandingQueryStateTimer,
                    persistor.setMultipleValuesStandingQueryState(globalId, qid, localId, serialized),
                    context.dispatcher,
                  )
              }(implicitly, context.dispatcher)

              val persistenceFuture = snapshotSaved zip multipleValuesStatesSaved

              // Schedule an update to the shard
              persistenceFuture.onComplete {
                case Success(_) => reportSleepSuccess(qidAtTime, sleepTimer)
                case Failure(err) =>
                  shardActor ! SleepOutcome.SleepFailed(
                    qidAtTime,
                    snapshot,
                    edges.size,
                    properties.transform((_, v) => v.serialized.length), // this eagerly serializes; can be expensive
                    err,
                    shardPromise,
                  )
              }(context.dispatcher)

            case _ =>
              reportSleepSuccess(qidAtTime, sleepTimer)
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
        case _: WakefulState.ConsideringSleep =>
          // If this is hit, this is a bug because the invariant above (on the definition of `newState`) was violated
          log.warn(
            log"Node $qid is still considering sleep after it should have decided whether to sleep.",
          )
      }
  }
}
