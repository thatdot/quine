package com.thatdot.quine.graph

import java.time.Instant
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{ConcurrentHashMap, ExecutionException}
import java.{lang, util}

import scala.collection.compat._
import scala.collection.concurrent
import scala.compat.ExecutionContexts
import scala.concurrent.Future
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}

import akka.stream.scaladsl.{BroadcastHub, Flow, Keep, Source}
import akka.stream.{BoundedSourceQueue, KillSwitches, QueueOfferResult, UniqueKillSwitch}
import akka.util.Timeout
import akka.{Done, NotUsed}

import com.google.common.cache.{CacheBuilder, CacheLoader, LoadingCache}

import com.thatdot.quine.graph.MasterStream.SqResultsExecToken
import com.thatdot.quine.graph.messaging.QuineIdAtTime
import com.thatdot.quine.graph.messaging.StandingQueryMessage._

/** Functionality for standing queries. */
trait StandingQueryOpsGraph extends BaseGraph {

  /** Map of all currently live standing queries
    */
  private val standingQueries: concurrent.Map[StandingQueryId, RunningStandingQuery] =
    new ConcurrentHashMap[StandingQueryId, RunningStandingQuery]().asScala

  def runningStandingQueries: scala.collection.Map[StandingQueryId, RunningStandingQuery] = standingQueries.toMap

  def runningStandingQuery(standingQueryId: StandingQueryId): Option[RunningStandingQuery] =
    standingQueries.get(standingQueryId)

  def clearStandingQueries(): Unit = standingQueries.clear()

  requireBehavior(classOf[StandingQueryOpsGraph].getSimpleName, classOf[behavior.CypherStandingBehavior])
  requireBehavior(classOf[StandingQueryOpsGraph].getSimpleName, classOf[behavior.DomainNodeIndexBehavior])

  // NB this initialization needs to occur before we restore standing queries,
  // otherwise a null pointer exception occurs
  private val standingQueryPartIndex: LoadingCache[StandingQueryPartId, cypher.StandingQuery] = CacheBuilder
    .newBuilder()
    .asInstanceOf[CacheBuilder[Any, Any]] // The Java library says this is `AnyRef`, but we want `Any`
    .weakValues()
    .build(new CacheLoader[StandingQueryPartId, cypher.StandingQuery] {
      override def loadAll(
        keys: lang.Iterable[_ <: StandingQueryPartId]
      ): util.Map[StandingQueryPartId, cypher.StandingQuery] = {
        logger.info(
          s"Performing a full update of the standingQueryPartIndex because of query for part IDs ${keys.asScala.toList}"
        )
        val runningSqs = runningStandingQueries.values
          .map(_.query.query)
          .collect { case StandingQueryPattern.SqV4(sq, _, _) => sq }
          .toVector
        val runningSqParts = runningSqs.toSet
          .foldLeft(Set.empty[cypher.StandingQuery])((acc, sq) => cypher.StandingQuery.indexableSubqueries(sq, acc))
          .map(sq => sq.id -> sq)
          .toMap
        val runningPartsKeys = runningSqParts.keySet
        keys.asScala.collectFirst {
          case part if !runningPartsKeys.contains(part) =>
            logger.warn(s"Unable to find running Standing Query part: $part")
        }
        runningSqParts.asJava
      }
      def load(k: StandingQueryPartId): cypher.StandingQuery = loadAll(Seq(k).asJava).get(k)
    })

  /** Report a new result for the specified standing query to this host's results queue for that query
    *
    * @note if the result is not positive and the query ignores cancellations, this is a no-op
    * @param sqId the standing query the result is for
    * @param sqResult the result to enqueue
    * @return if the result was successfully enqueued
    */
  def reportStandingResult(sqId: StandingQueryId, sqResult: SqResultLike): Boolean =
    runningStandingQuery(sqId) exists { standingQuery =>
      if (sqResult.isPositive || standingQuery.query.query.includeCancellation) {
        standingQuery.resultMeter.mark()
        standingQuery.resultsQueue.offer(sqResult.standingQueryResult(standingQuery.query, idProvider)) match {
          case QueueOfferResult.Enqueued =>
            true
          case QueueOfferResult.Failure(err) =>
            standingQuery.droppedCounter.inc()
            logger.warn(
              s"onResult: failed to enqueue Standing Query result for: ${standingQuery.query.name} due to error: ${err.getMessage}"
            )
            logger.info(
              s"onResult: failed to enqueue Standing Query result for: ${standingQuery.query.name}. Result: ${sqResult}",
              err
            )
            false
          case QueueOfferResult.QueueClosed =>
            logger.warn(
              s"onResult: Standing Query Result arrived but result queue already closed for: ${standingQuery.query.name}"
            )
            logger.info(
              s"onResult: Standing Query result queue already closed for: ${standingQuery.query.name}. Dropped result: ${sqResult}"
            )
            false
          case QueueOfferResult.Dropped =>
            logger.warn(
              s"onResult: dropped Standing Query result for: ${standingQuery.query.name}"
            )
            logger.info(
              s"onResult: dropped Standing Query result for: ${standingQuery.query.name}. Result: ${sqResult}"
            )
            false
        }
      } else {
        true
      }
    }

  /** Complete all standing query streams (since the graph is shutting down */
  def shutdownStandingQueries(): Future[Unit] = Future
    .traverse(runningStandingQueries.values)((query: RunningStandingQuery) => query.cancel())(
      implicitly,
      shardDispatcherEC
    )
    .map(_ => ())(shardDispatcherEC)

  /** Register a new standing query
    *
    * TODO: remove `sqId` as an argument once the SQ sync protocol moves into Quine
    * TODO: remove `skipPersistor` as an argument once the SQ sync protocol moves into Quine
    * TODO: don't pass in `outputs` and don't return kill switches
    *
    * @param name the name of the query to register
    * @param pattern the pattern against which the query will match
    * @param outputs the set of outputs, if any, this query should output to
    * @param queueBackpressureThreshold buffer size at which ingest starts being backpressured
    * @param queueMaxSize buffer size at which SQ results start being dropped
    * @param skipPersistor whether to skip modifying durable storage
    * @param sqId internally use a supplied ID if provided or create a new one
    * @return
    */
  def createStandingQuery(
    name: String,
    pattern: StandingQueryPattern,
    outputs: Map[String, Flow[StandingQueryResult, SqResultsExecToken, NotUsed]],
    queueBackpressureThreshold: Int = StandingQuery.DefaultQueueBackpressureThreshold,
    queueMaxSize: Int = StandingQuery.DefaultQueueMaxSize,
    shouldCalculateResultHashCode: Boolean = false,
    skipPersistor: Boolean = false,
    sqId: StandingQueryId = StandingQueryId.fresh() // fresh ID if none is provided
  ): (RunningStandingQuery, Map[String, UniqueKillSwitch]) = {
    requiredGraphIsReady()
    val rsqAndOutputs =
      startStandingQuery(
        sqId = sqId,
        name,
        pattern,
        outputs,
        queueBackpressureThreshold,
        queueMaxSize,
        shouldCalculateResultHashCode
      )
    if (!skipPersistor) {
      persistor.persistStandingQuery(rsqAndOutputs._1.query)
    }
    rsqAndOutputs
  }

  /** Start a standing query that will be registered on all nodes awoken in the graph
    *
    * INV: This will never throw [[GraphNotReadyException]], because it is used as part of readying a graph
    */
  protected def startStandingQuery(
    sqId: StandingQueryId,
    name: String,
    pattern: StandingQueryPattern,
    outputs: Map[String, Flow[StandingQueryResult, SqResultsExecToken, NotUsed]],
    queueBackpressureThreshold: Int,
    queueMaxSize: Int,
    shouldCalculateResultHashCode: Boolean
  ): (RunningStandingQuery, Map[String, UniqueKillSwitch]) = {
    val sq = StandingQuery(name, sqId, pattern, queueBackpressureThreshold, queueMaxSize, shouldCalculateResultHashCode)
    val (runningSq, killSwitches) = runStandingQuery(sq, outputs)
    standingQueries.put(sqId, runningSq)

    // if this is a cypher SQ (at runtime), also register its components in the index as appropriate
    pattern match {
      case runsAsCypher: StandingQueryPattern.SqV4 =>
        standingQueryPartIndex.putAll(
          cypher.StandingQuery.indexableSubqueries(runsAsCypher.compiledQuery).view.map(sq => sq.id -> sq).toMap.asJava
        )
      case _: StandingQueryPattern.Branch =>
    }

    (runningSq, killSwitches)
  }

  /** Cancel a standing query
    *
    * TODO: remove `skipPersistor` as an argument once the SQ sync protocol moves into Quine
    *
    * @param ref which standing query to cancel
    * @param skipPersistor whether to skip modifying durable storage
    * @return Some Future that will return the final state of the standing query, or [[None]] if the standing query
    *         doesn't exist
    */
  def cancelStandingQuery(
    ref: StandingQueryId,
    skipPersistor: Boolean = false
  ): Option[Future[(StandingQuery, Instant, Int)]] = {
    requiredGraphIsReady()
    standingQueries.remove(ref).map { (sq: RunningStandingQuery) =>
      val persistence = if (skipPersistor) Future.unit else persistor.removeStandingQuery(sq.query)
      val cancellation = sq.cancel()
      persistence.zipWith(cancellation)((_, _) => (sq.query, sq.startTime, sq.bufferCount))(shardDispatcherEC)
    }
  }

  private def logSqOutputFailure(name: String, err: Throwable): Unit =
    logger.error(s"Standing query output stream has failed for $name:", err)

  /** List standing queries that are currently registered
    *
    * @return standing query, when it was started (or re-started), and the number of buffered results
    */
  def listStandingQueries: Map[StandingQueryId, (StandingQuery, Instant, Int)] = {
    requiredGraphIsReady()
    runningStandingQueries.view.mapValues(sq => (sq.query, sq.startTime, sq.bufferCount)).toMap
  }

  /** Fetch a source to wire-tap a standing query
    *
    * @return source to wire-tap or [[None]] if the standing query doesn't exist
    */
  def wireTapStandingQuery(ref: StandingQueryId): Option[Source[StandingQueryResult, NotUsed]] = {
    requiredGraphIsReady()
    runningStandingQuery(ref).map(_.resultsHub)
  }

  /** Ensure universal standing queries have been propagated out to all the
    * right nodes
    *
    * @param parallelism propagate to how many nodes at once? (if unset, doesn't wake nodes)
    * @param timeout max time to wait for any particular node (not for the whole propagation)
    * @return future that completes when all the messages have been fired off
    */
  def propagateStandingQueries(parallelism: Option[Int])(implicit timeout: Timeout): Future[Unit] = {
    requiredGraphIsReady()
    parallelism match {
      case Some(par) =>
        enumerateAllNodeIds()
          .mapAsyncUnordered(par)(qid => relayAsk(QuineIdAtTime(qid, None), UpdateStandingQueriesWake(_)))
          .run()
          .map(_ => ())(ExecutionContexts.parasitic)

      case None =>
        enumerateAllNodeIds()
          .map(qid => relayTell(QuineIdAtTime(qid, None), UpdateStandingQueriesNoWake))
          .run()
          .map(_ => ())(ExecutionContexts.parasitic)
    }
  }

  @throws[NoSuchElementException]("When a StandingQueryPartId is not known to this graph")
  def getStandingQueryPart(queryPartId: StandingQueryPartId): cypher.StandingQuery =
    try standingQueryPartIndex.get(queryPartId)
    catch {
      case _: ExecutionException => throw new NoSuchElementException(s"No such standing query part: $queryPartId")
    }

  private def runStandingQuery(
    sq: StandingQuery,
    outputs: Map[String, Flow[StandingQueryResult, SqResultsExecToken, NotUsed]]
  ): (RunningStandingQuery, Map[String, UniqueKillSwitch]) = {

    /* Counter for how many elements are in the queue
     *
     * The fact this is an atomic counter gives us the ability to know exactly
     * when to open/close the valve, since we know we will visit the threshold
     * exactly once when going from below threshold to above (and again once for
     * going from above the threshold back down to below).
     *
     * Using `getAndIncrement` and `getAndDecrement`, we are able to perform
     * the count update and afterwards find out if this was the
     * increment/decrement that should trigger a change in the valve state.
     */
    val inBuffer = new AtomicInteger()

    val ((queue, term), resultsHub: Source[StandingQueryResult, NotUsed]) = Source
      .queue[StandingQueryResult](sq.queueMaxSize) // Queue of top-level results for this StandingQueryId on this member
      .watchTermination() { (mat, done) =>
        done.onComplete { (_: Try[Done]) =>
          if (sq.queueBackpressureThreshold <= inBuffer.getAndSet(0)) {
            ingestValve.open()
          }
        }(shardDispatcherEC)
        mat -> done
      }
      .map { (x: StandingQueryResult) =>
        if (sq.queueBackpressureThreshold == inBuffer.getAndDecrement()) {
          ingestValve.open()
        }
        x
      }
      .named(s"sq-results-for-${sq.name}")
      .toMat(
        BroadcastHub.sink[StandingQueryResult](bufferSize = 8).named(s"sq-results-hub-for-${sq.name}")
      )(Keep.both)
      // bufferSize = 8 ensures all consumers attached to the hub are kept within 8 elements of each other
      .run()

    term.onComplete {
      case Failure(err) =>
        // If the output stream gets terminated badly, cancel the standing query and log the error
        logSqOutputFailure(sq.name, err)
        cancelStandingQuery(sq.id)
      case Success(_) => // Do nothing. This is the shutdown case.
    }(shardDispatcherEC)

    val killSwitches = outputs.map { case (name, o) =>
      val killer = o.viaMat(KillSwitches.single)(Keep.right)
      val sqSrc: Source[SqResultsExecToken, UniqueKillSwitch] = resultsHub.viaMat(killer)(Keep.right)
      name -> masterStream.addSqResultsSrc(sqSrc)
    }

    val runningStandingQuery = new RunningStandingQuery(
      resultsQueue = new BoundedSourceQueue[StandingQueryResult] {
        def complete() = queue.complete()
        def fail(ex: Throwable) = queue.fail(ex)
        def size() = queue.size()
        def offer(r: StandingQueryResult) = {
          val res = queue.offer(r)
          if (res == QueueOfferResult.Enqueued) {
            if (sq.queueBackpressureThreshold == inBuffer.incrementAndGet())
              ingestValve.close()
            if (sq.shouldCalculateResultHashCode)
              // Integrate each standing query result hash code using `add`
              // so the result is order agnostic
              metrics.standingQueryResultHashCode(sq.id).add(r.dataHashCode)
          }
          res
        }
      },
      query = sq,
      resultsHub = resultsHub,
      outputTermination = term,
      metrics = metrics
    )
    (runningStandingQuery, killSwitches)
  }

}

object StandingQueryOpsGraph {

  /** Check if a graph supports standing query operations and refine it if possible */
  def apply(graph: BaseGraph): Option[StandingQueryOpsGraph] =
    if (graph.isInstanceOf[StandingQueryOpsGraph]) Some(graph.asInstanceOf[StandingQueryOpsGraph]) else None
}
