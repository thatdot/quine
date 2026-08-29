package com.thatdot.quine.graph

import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}
import scala.reflect.ClassTag
import scala.util.{Failure, Success, Try}

import com.thatdot.common.logging.Log.{LazySafeLogging, LogConfig, Safe, SafeLoggableInterpolator}
import com.thatdot.common.quineid.QuineId
import com.thatdot.quine.graph.NodeActor.{Journal, MultipleValuesStandingQueries}
import com.thatdot.quine.graph.StaticNodeSupport.{deserializeSnapshotBytes, getMultipleValuesStandingQueryStates}
import com.thatdot.quine.graph.behavior.MultipleValuesStandingQueryPartSubscription
import com.thatdot.quine.graph.cypher.{
  Columns,
  EdgeSubscriptionReciprocalState,
  MultipleValuesStandingQuery,
  MultipleValuesStandingQueryLookupInfo,
}
import com.thatdot.quine.graph.messaging.SpaceTimeQuineId
import com.thatdot.quine.graph.messaging.StandingQueryMessage.MultipleValuesStandingQuerySubscriber
import com.thatdot.quine.graph.metrics.implicits.TimeFuture
import com.thatdot.quine.model.QuineIdProvider
import com.thatdot.quine.persistor.NamespacedPersistenceAgent
import com.thatdot.quine.persistor.codecs.{AbstractSnapshotCodec, MultipleValuesStandingQueryStateCodec}
import com.thatdot.quine.util.Log.implicits._

abstract class StaticNodeSupport[
  Node <: AbstractNodeActor,
  Snapshot <: AbstractNodeSnapshot,
  ConstructorRecord <: Product,
](implicit
  val nodeClass: ClassTag[Node],
  val snapshotCodec: AbstractSnapshotCodec[Snapshot],
) {

  /** nodeClass is the class of nodes in the graph
    *
    * INV: this class's constructor arguments must end with the same types in the same order as [[ConstructorRecord]],
    * See finalNodeArgs in [[GraphShardActor]]'s handling of [[NodeStateRehydrated]]
    */
  def createNodeArgs(
    snapshot: Option[Snapshot],
    initialJournal: Journal = Iterable.empty,
    multipleValuesStandingQueryStates: MultipleValuesStandingQueries = mutable.Map.empty,
  ): ConstructorRecord

  /** Forget whatever query parts that are no longer running left outside their own blobs on this node.
    *
    * The blobs themselves are deleted where these are found, because finding them is the hard part: what is under a
    * part's key is cheap to remove and impossible to discover without scanning everything. So anything a part keeps
    * elsewhere has to go at the same moment, from the same list, or it never goes at all.
    *
    * A state that keeps everything in its own blob has nothing held elsewhere, so the default is to do nothing. Not
    * waited on, for the same reason the blob deletion is not: waking must not depend on a deletion that changes no
    * answer.
    */
  protected def dropRecordsHeldElsewhere(
    qid: QuineId,
    namespace: NamespaceId,
    graph: BaseGraph,
    removedParts: Iterable[(StandingQueryId, MultipleValuesStandingQueryPartId)],
  )(implicit logConfig: LogConfig): Unit = ()

  def readConstructorRecord(
    quineIdAtTime: SpaceTimeQuineId,
    recoverySnapshotBytes: Option[Array[Byte]],
    graph: BaseGraph,
  )(implicit logConfig: LogConfig): Future[ConstructorRecord] =
    recoverySnapshotBytes match {
      case Some(recoverySnapshotBytes) =>
        val snapshot =
          deserializeSnapshotBytes(recoverySnapshotBytes, quineIdAtTime)(
            graph.idProvider,
            snapshotCodec,
          )
        val multipleValuesStandingQueryStatesFut: Future[MultipleValuesStandingQueries] =
          getMultipleValuesStandingQueryStates(quineIdAtTime, graph, this)
        multipleValuesStandingQueryStatesFut.map(multipleValuesStandingQueryStates =>
          // this snapshot was created as the node slept, so there are no journal events after the snapshot
          createNodeArgs(
            Some(snapshot),
            initialJournal = Iterable.empty,
            multipleValuesStandingQueryStates = multipleValuesStandingQueryStates,
          ),
        )(graph.nodeDispatcherEC)

      case None => restoreFromSnapshotAndJournal(quineIdAtTime, graph)
    }

  /** Load the state of specified the node at the specified time. The resultant NodeActorConstructorArgs should allow
    * the node to restore itself to its state prior to sleeping (up to removed Standing Queries) without any additional
    * persistor calls.
    *
    * @param untilOpt load changes made up to and including this time
    */
  def restoreFromSnapshotAndJournal(
    quineIdAtTime: SpaceTimeQuineId,
    graph: BaseGraph,
  )(implicit logConfig: LogConfig): Future[ConstructorRecord] = graph
    .namespacePersistor(quineIdAtTime.namespace)
    .fold {
      Future.successful(((None: Option[Snapshot], Nil: Journal), mutable.Map.empty: MultipleValuesStandingQueries))
    } { persistor =>
      val SpaceTimeQuineId(qid, _, atTime) = quineIdAtTime
      val persistenceConfig = persistor.persistenceConfig

      def getSnapshot(): Future[Option[Snapshot]] =
        if (!persistenceConfig.snapshotEnabled) Future.successful(None)
        else {
          val upToTime = atTime match {
            case Some(historicalTime) if !persistenceConfig.snapshotSingleton =>
              EventTime.fromMillis(historicalTime)
            case _ =>
              EventTime.MaxValue
          }
          graph.metrics.persistorGetLatestSnapshotTimer
            .time {
              persistor.getLatestSnapshot(qid, upToTime)
            }
            .map { maybeBytes =>
              maybeBytes.map(
                deserializeSnapshotBytes(_, quineIdAtTime)(graph.idProvider, snapshotCodec),
              )
            }(graph.nodeDispatcherEC)
        }

      def getJournalAfter(after: Option[EventTime], includeDomainIndexEvents: Boolean): Future[Iterable[NodeEvent]] = {
        val startingAt = after.fold(EventTime.MinValue)(_.tickEventSequence(None))
        val endingAt = atTime match {
          case Some(until) => EventTime.fromMillis(until).largestEventTimeInThisMillisecond
          case None => EventTime.MaxValue
        }
        graph.metrics.persistorGetJournalTimer.time {
          persistor.getJournal(qid, startingAt, endingAt, includeDomainIndexEvents)
        }
      }

      // Get the snapshot and journal events
      val snapshotAndJournal =
        getSnapshot()
          .flatMap { latestSnapshotOpt =>
            val journalAfterSnapshot: Future[Journal] = if (persistenceConfig.journalEnabled) {
              getJournalAfter(latestSnapshotOpt.map(_.time), includeDomainIndexEvents = atTime.isEmpty)
              // QU-429 to avoid extra retries, consider unifying the Failure types of `persistor.getJournal`, and adding a
              // recoverWith here to map any that represent irrecoverable failures to a [[NodeWakeupFailedException]]
            } else
              Future.successful(Vector.empty)

            journalAfterSnapshot.map(journalAfterSnapshot => (latestSnapshotOpt, journalAfterSnapshot))(
              ExecutionContext.parasitic,
            )
          }(graph.nodeDispatcherEC)

      // Get the materialized standing query states for MultipleValues.
      val multipleValuesStandingQueryStates: Future[MultipleValuesStandingQueries] =
        getMultipleValuesStandingQueryStates(quineIdAtTime, graph, this)

      // Will defer all other message processing until the Future is complete.
      // It is OK to ignore the returned future from `pauseMessageProcessingUntil` because nothing else happens during
      // initialization of this actor. Additional message processing is deferred by `pauseMessageProcessingUntil`'s
      // message stashing.
      snapshotAndJournal
        .zip(multipleValuesStandingQueryStates)
    }
    .map { case ((snapshotOpt, journal), multipleValuesStates) =>
      createNodeArgs(snapshotOpt, journal, multipleValuesStates)
    }(graph.nodeDispatcherEC)
}

object StaticNodeSupport extends LazySafeLogging {
  @throws[NodeWakeupFailedException]("When snapshot could not be deserialized")
  private def deserializeSnapshotBytes[Snapshot <: AbstractNodeSnapshot](
    snapshotBytes: Array[Byte],
    qidForDebugging: SpaceTimeQuineId,
  )(implicit
    idProvider: QuineIdProvider,
    snapshotCodec: AbstractSnapshotCodec[Snapshot],
  ): Snapshot =
    snapshotCodec.format
      .read(snapshotBytes)
      .fold(
        err =>
          throw new NodeWakeupFailedException(
            s"Snapshot could not be loaded for: ${qidForDebugging.pretty}",
            err,
          ),
        identity,
      )

  private def getMultipleValuesStandingQueryStates(
    qidAtTime: SpaceTimeQuineId,
    graph: BaseGraph,
    support: StaticNodeSupport[_, _, _],
  )(implicit logConfig: LogConfig): Future[MultipleValuesStandingQueries] = (graph -> qidAtTime) match {
    case (sqGraph: StandingQueryOpsGraph, SpaceTimeQuineId(qid, namespace, None)) =>
      sqGraph
        .namespacePersistor(namespace)
        .fold {
          Future.successful(mutable.Map.empty: MultipleValuesStandingQueries)
        } { persistor =>
          sqGraph
            .standingQueries(namespace)
            .fold(Future.successful(mutable.Map.empty: MultipleValuesStandingQueries)) { sqns =>
              val idProv: QuineIdProvider = sqGraph.idProvider
              val lookupInfo = new MultipleValuesStandingQueryLookupInfo {
                def lookupQuery(queryPartId: MultipleValuesStandingQueryPartId): MultipleValuesStandingQuery =
                  sqns.getStandingQueryPart(queryPartId)
                val executingNodeId: QuineId = qid
                val idProvider: QuineIdProvider = idProv
              }
              sqGraph.metrics.persistorGetMultipleValuesStandingQueryStatesTimer
                .time {
                  persistor.getMultipleValuesStandingQueryStates(qid)
                }
                .flatMap { multipleValuesStandingQueryStates =>
                  // partition the retrieved MVSQ states into those that are still running and those that are not
                  val (keepThese, removeThese) = multipleValuesStandingQueryStates.partition {
                    case ((sqId, partId @ _), _) => sqns.runningStandingQuery(sqId).isDefined
                  }
                  // `removeThese` represents standing queries that have been cancelled since the previous time the
                  // node was awoken. Because these are no longer running, their persisted information is no longer
                  // relevant, and they will not be found if the we try to `rehydrate` them during construction.
                  // Nothing else will ever look at them again, so this is where they go: a state left behind by a
                  // cancelled query would otherwise be read at every wake, forever, only to be discarded again.
                  //
                  // Deliberately not waited on. Waking must not depend on the disk having caught up with a deletion
                  // that changes no answer, and a deletion missed because the node stopped is found again next wake,
                  // since what identifies these is that the query is gone rather than anything recorded here.
                  //
                  // Deleting them takes more than not finding them: it takes having looked somewhere complete. This
                  // graph fills its registry as it starts up, one query at a time, and until that has finished
                  // "cancelled" and "not read yet" are the same observation from here, so a wake landing in that
                  // window would read every state the node has as cancelled, and delete all of it. Nothing on the
                  // wake path refuses such a wake, and the registry has an entry for the default namespace from the
                  // moment the graph exists, so its emptiness looks like an answer. Asking whether the queries have
                  // been restored is what separates the two. Until they have, these states are dropped from the node
                  // and left on disk, which is what happened before they were deleted at all: the node runs this
                  // wake without them, and a wake that can tell them apart judges them again.
                  if (removeThese.nonEmpty) {
                    logger.debug(
                      safe"""During node constructor assembly, found ${Safe(removeThese.size)} no-longer-relevant
                            |MVSQ states for node: ${Safe(qidAtTime.pretty(idProv))}""".cleanLines,
                    )
                    if (sqGraph.standingQueriesRestored) {
                      support.dropRecordsHeldElsewhere(qid, namespace, graph, removeThese.keys)
                      removeThese.keys.foreach { case (sqId, partId) =>
                        persistor
                          .setMultipleValuesStandingQueryState(sqId, qid, partId, None)
                          .onComplete {
                            case Failure(err) =>
                              logger.info(
                                log"""Could not remove the state of cancelled standing query ${Safe(sqId)} from node:
                                     |${Safe(qidAtTime.pretty(idProv))}. It will be found again the next time this
                                     |node wakes.""".cleanLines withException err,
                              )
                            case Success(_) => ()
                          }(ExecutionContext.parasitic)
                      }
                    } else {
                      logger.info(
                        safe"""Node: ${Safe(qidAtTime.pretty(idProv))} woke holding ${Safe(removeThese.size)} MVSQ
                              |states whose standing queries this graph has not finished restoring, so it cannot yet
                              |tell a cancelled query from one it has not read. They are left where they are, to be
                              |judged at a later wake.""".cleanLines,
                      )
                    }
                  }

                  // with the still-relevant SQ states, continue to assemble the node's constructor arguments
                  val decoded: MultipleValuesStandingQueries =
                    mutable.Map.from(keepThese.map { case (sqIdAndPartId, bytes) =>
                      val sqState = MultipleValuesStandingQueryStateCodec.format
                        .read(bytes)
                        .fold(
                          err =>
                            throw new NodeWakeupFailedException(
                              s"NodeActor state (Standing Query States) for node: ${qidAtTime.pretty(idProv)} could not be loaded",
                              err,
                            ),
                          identity,
                        )
                      sqIdAndPartId -> sqState
                    })

                  // Fold reciprocal states written under per-half-edge ids into the shared states that now serve
                  // them. The map is transformed in place, so the node is live-correct from this moment.
                  val fold = foldLegacyReciprocals(decoded, partId => Try(sqns.getStandingQueryPart(partId)).toOption)
                  val folded: Future[Unit] =
                    if (fold.isEmpty) Future.unit
                    else {
                      logger.info(
                        safe"""Folded ${Safe(fold.foldedRowCount)} reciprocal standing query states written under
                            |per-edge ids into their shared states on node: ${Safe(qidAtTime.pretty(idProv))}
                            |""".cleanLines,
                      )
                      val persistence = persistLegacyReciprocalFold(fold, decoded, persistor, qid)
                      persistence.deleted.failed.foreach { err =>
                        logger.info(
                          log"""Could not delete the reciprocal standing query states of node:
                             |${Safe(qidAtTime.pretty(idProv))} that were folded into their shared states. Nothing
                             |reaches those rows in the meantime: the fold takes each of them out of what this node
                             |runs whether its row goes or not, so an undeleted one registers for no events and
                             |relays nothing. They are folded again the next time this node wakes.""".cleanLines
                          withException err,
                        )
                      }(ExecutionContext.parasitic)
                      // Waking waits for the writes. Until they land, the folded subscribers exist only in this
                      // node's memory and in rows that are about to be deleted; letting the node run first would put
                      // its own writes for these very keys in flight alongside the fold's, and one of them would win.
                      // This happens at most once per node, ever, and only for a node holding pre-collapse rows.
                      persistence.written.recover { case err =>
                        logger.info(
                          log"""Could not persist the folded reciprocal standing query states of node:
                             |${Safe(qidAtTime.pretty(idProv))}. The fold holds in memory and will run
                             |again the next time this node wakes.""".cleanLines withException err,
                        )
                      }(ExecutionContext.parasitic)
                    }

                  decoded.foreach { case (_, (_, state)) => state.rehydrate(lookupInfo) }
                  folded.map(_ => decoded)(ExecutionContext.parasitic)
                }(sqGraph.nodeDispatcherEC)
            }
        }
    case (_: StandingQueryOpsGraph, SpaceTimeQuineId(_, _, Some(_))) =>
      // this is the right kind of graph, but by definition, historical nodes (ie, atTime != None)
      // have no multipleValues states
      Future.successful(mutable.Map.empty)
    case (nonStandingQueryGraph @ _, _) =>
      // wrong kind of graph: only [[StandingQueryOpsGraph]]s can manage MultipleValues Standing Queries
      Future.successful(mutable.Map.empty)

  }

  /** What folding legacy reciprocal rows into shared states decided.
    *
    * The states map is already transformed by the time one of these exists; what remains is making that durable.
    * `updatedStates` must be written before `deletableRows` are deleted: at every point of that order the disk holds
    * either the legacy rows (still relaying, refolded at the next wake) or both generations (the legacy orphans
    * relay duplicate levels, which the subscribing side drops), never neither. Deleting first could lose
    * subscribers that exist nowhere else.
    *
    * @param updatedStates keys in the states map whose blobs must be (re)written, canonical reciprocals first
    * @param deletableRows keys of the per-half-edge rows the fold consumed, to delete after the writes land
    */
  final private[graph] case class LegacyReciprocalFold(
    updatedStates: Seq[(StandingQueryId, MultipleValuesStandingQueryPartId)],
    deletableRows: Seq[(StandingQueryId, MultipleValuesStandingQueryPartId)],
  ) {
    def nonEmpty: Boolean = updatedStates.nonEmpty || deletableRows.nonEmpty
    def isEmpty: Boolean = !nonEmpty
    def foldedRowCount: Int = deletableRows.size
  }

  /** Fold reciprocal states filed under per-half-edge part ids into the one state per constraints that now serves
    * every subscriber, in memory.
    *
    * A legacy row is recognized by its key: the id its own content hashes to under the current funnel is not the id
    * it is filed under. Its content is already exactly what the shared state wants (the constraints are the half
    * edge's type and direction), so the fold moves subscribers and deletes keys; it never reinterprets content.
    * The `andThen` state's subscriber set is rewritten in the same pass: every consumed row's entry comes out, and
    * where the row had subscribers its entry is renamed to the shared id, which for a state the fold creates is
    * also its subscription (`onInitialize` runs only on first creation via the behavior, never at wake). A row can
    * carry subscribers and no entry, though, having been persisted while its own edge was absent so that it had
    * never subscribed, and a state assembled entirely from such rows leaves the fold unsubscribed. The wake-time
    * re-check in `updateMultipleValuesStandingQueriesOnNode` is what links those, through the ordinary subscription
    * path; the invariant both are maintaining is described on [[cypher.EdgeSubscriptionReciprocalState]].
    *
    * Folding is idempotent: an already-folded row short-circuits on its key, subscriber merges are set unions, and
    * a fold interrupted before its writes landed simply happens again at the next wake.
    *
    * @param states     decoded states of this node, transformed in place
    * @param lookupPart resolver for registered query parts, used to recover the `columns` a reciprocal id hashes
    *                   (never persisted; `Columns.Omitted` for every compiled query, so absence is not an error)
    */
  private[graph] def foldLegacyReciprocals(
    states: MultipleValuesStandingQueries,
    lookupPart: MultipleValuesStandingQueryPartId => Option[MultipleValuesStandingQuery],
  ): LegacyReciprocalFold = {
    val updated = mutable.LinkedHashSet.empty[(StandingQueryId, MultipleValuesStandingQueryPartId)]
    val rewrittenAndThens = mutable.LinkedHashSet.empty[(StandingQueryId, MultipleValuesStandingQueryPartId)]
    val deletable = Seq.newBuilder[(StandingQueryId, MultipleValuesStandingQueryPartId)]

    val legacyRows = states
      .collect { case (key, (subscription, reciprocal: EdgeSubscriptionReciprocalState)) =>
        val columns = subscription.subscribers
          .collectFirst { case MultipleValuesStandingQuerySubscriber.NodeSubscriber(_, _, forQuery) =>
            lookupPart(forQuery)
          }
          .flatten
          .collect { case subscribeAcrossEdge: MultipleValuesStandingQuery.SubscribeAcrossEdge =>
            subscribeAcrossEdge.columns
          }
          .getOrElse(Columns.Omitted)
        val canonicalPartId = MultipleValuesStandingQuery
          .EdgeSubscriptionReciprocal(reciprocal.halfEdge, reciprocal.andThenId, columns)
          .queryPartId
        (key, canonicalPartId, subscription, reciprocal)
      }
      .filter { case ((_, rowPartId), canonicalPartId, _, _) => canonicalPartId != rowPartId }

    legacyRows.foreach { case (key @ (sqId, rowPartId), canonicalPartId, subscription, reciprocal) =>
      states.remove(key)
      deletable += key
      val survivors = subscription.subscribers.nonEmpty
      if (survivors) {
        val canonicalKey = sqId -> canonicalPartId
        val (canonicalSubscription, canonicalState) = states.getOrElseUpdate(
          canonicalKey, {
            val fresh = EdgeSubscriptionReciprocalState(canonicalPartId, reciprocal.halfEdge, reciprocal.andThenId)
            MultipleValuesStandingQueryPartSubscription(canonicalPartId, sqId, mutable.Set.empty) -> fresh
          },
        )
        canonicalSubscription.subscribers ++= subscription.subscribers
        canonicalState match {
          // Rows folding together may hold cached copies persisted at different moments; any of them is a valid
          // level, so the first to arrive stands and the andThen's next report corrects it.
          case canonicalReciprocal: EdgeSubscriptionReciprocalState =>
            if (canonicalReciprocal.cachedResult.isEmpty) canonicalReciprocal.cachedResult = reciprocal.cachedResult
          case _ => () // key collision with a non-reciprocal state: impossible while ids include a type tag
        }
        updated += canonicalKey
      }

      // Every consumed row's own subscription to the andThen comes out of the andThen's subscriber set: whatever
      // else happens, the andThen must not go on reporting to an id nothing holds anymore. A row with subscribers
      // hands its entry over renamed; a row without any hands over nothing, since the canonical state it would have
      // renamed it for is not being created. The fold never *adds* a subscription no row carried: a state
      // assembled entirely from rows that never subscribed is linked by the wake-time re-check in
      // `updateMultipleValuesStandingQueriesOnNode` instead, through the ordinary subscription path, which also
      // answers it with the andThen's current results. A bare entry written here would relay nothing until the
      // andThen next changed.
      states.get(sqId -> reciprocal.andThenId).foreach { case (andThenSubscription, _) =>
        val legacyRefs = andThenSubscription.subscribers.collect {
          case MultipleValuesStandingQuerySubscriber.NodeSubscriber(onNode, globalId, `rowPartId`) =>
            MultipleValuesStandingQuerySubscriber.NodeSubscriber(onNode, globalId, rowPartId) ->
              MultipleValuesStandingQuerySubscriber.NodeSubscriber(onNode, globalId, canonicalPartId)
        }
        if (legacyRefs.nonEmpty) {
          legacyRefs.foreach { case (legacyRef, canonicalRef) =>
            andThenSubscription.subscribers -= legacyRef
            if (survivors) andThenSubscription.subscribers += canonicalRef
          }
          rewrittenAndThens += (sqId -> reciprocal.andThenId)
        }
      }
    }

    // Canonical states before rewritten andThens: see the ordering note on [[LegacyReciprocalFold]]
    LegacyReciprocalFold((updated ++ rewrittenAndThens).toSeq, deletable.result())
  }

  /** What making a fold durable is doing, kept as two futures because a node has to wait for one and not the other.
    *
    * @param written the touched blobs, written in order. The node must not run until these land: they are the only
    *                copy of the folded subscribers once the rows below are gone.
    * @param deleted the per-half-edge rows the fold consumed, deleted once every write has landed. Nothing depends
    *                on these having happened (an undeleted row is refolded at the next wake), and there is one
    *                per subscriber, so waiting for them would put all of that on the wake path for no answer.
    */
  final private[graph] case class FoldPersistence(written: Future[Unit], deleted: Future[Unit])

  /** Make a fold durable: write the touched blobs in order, then delete the rows the fold consumed.
    *
    * Every blob is serialized **before any write is issued**, on the thread that owns these states. Serializing a
    * state walks its live subscriber set, and a chain of writes runs its later links on whatever thread finished the
    * one before, so serializing inside the chain would read the node's subscribers while the node was already
    * mutating them, and then delete the legacy rows that were the only other copy. What is written here is what the
    * fold decided, exactly as it decided it.
    */
  private[graph] def persistLegacyReciprocalFold(
    fold: LegacyReciprocalFold,
    states: MultipleValuesStandingQueries,
    persistor: NamespacedPersistenceAgent,
    qid: QuineId,
  ): FoldPersistence = {
    implicit val ec: ExecutionContext = ExecutionContext.parasitic
    val blobs: Seq[((StandingQueryId, MultipleValuesStandingQueryPartId), Array[Byte])] =
      fold.updatedStates.flatMap { key =>
        states.get(key).map(pair => key -> MultipleValuesStandingQueryStateCodec.format.write(pair))
      }
    val written = blobs.foldLeft(Future.unit) { case (prior, (key, bytes)) =>
      prior.flatMap(_ => persistor.setMultipleValuesStandingQueryState(key._1, qid, key._2, Some(bytes)))
    }
    val deleted = written.flatMap { _ =>
      Future
        .traverse(fold.deletableRows) { case (sqId, partId) =>
          persistor.setMultipleValuesStandingQueryState(sqId, qid, partId, None)
        }
        .map(_ => ())
    }
    FoldPersistence(written, deleted)
  }
}
