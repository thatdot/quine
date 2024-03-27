package com.thatdot.quine.graph

import java.time.Instant
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{ConcurrentHashMap, ExecutionException}
import java.{lang, util}

import scala.collection.concurrent
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}

import org.apache.pekko.stream.scaladsl.{BroadcastHub, Flow, Keep, Source}
import org.apache.pekko.stream.{BoundedSourceQueue, KillSwitches, QueueOfferResult, UniqueKillSwitch}
import org.apache.pekko.util.Timeout
import org.apache.pekko.{Done, NotUsed}

import cats.implicits._
import com.google.common.cache.{CacheBuilder, CacheLoader, LoadingCache}

import com.thatdot.quine.graph.MasterStream.SqResultsExecToken
import com.thatdot.quine.graph.StandingQueryPattern.DomainGraphNodeStandingQueryPattern
import com.thatdot.quine.graph.cypher.{MultipleValuesStandingQuery, QuinePattern}
import com.thatdot.quine.graph.messaging.SpaceTimeQuineId
import com.thatdot.quine.graph.messaging.StandingQueryMessage._
import com.thatdot.quine.model.DomainGraphNodePackage

/** Functionality for namespaced standing queries. */
trait StandingQueryOpsGraph extends BaseGraph {

  private[this] def requireCompatibleNodeType(): Unit = {
    requireBehavior[StandingQueryOpsGraph, behavior.MultipleValuesStandingQueryBehavior]
    requireBehavior[StandingQueryOpsGraph, behavior.DomainNodeIndexBehavior]
  }

  private val quinePatterns: concurrent.Map[StandingQueryId, QuinePattern] =
    new ConcurrentHashMap[StandingQueryId, QuinePattern]().asScala

  def runningQuinePatterns: scala.collection.Map[StandingQueryId, QuinePattern] = quinePatterns.toMap

  def standingQueries(namespace: NamespaceId): Option[NamespaceStandingQueries] =
    namespaceStandingQueries.get(namespace)

  private val namespaceStandingQueries: collection.concurrent.Map[NamespaceId, NamespaceStandingQueries] =
    new ConcurrentHashMap[NamespaceId, NamespaceStandingQueries].asScala
  namespaceStandingQueries.put(defaultNamespaceId, new NamespaceStandingQueries(defaultNamespaceId))

  def addStandingQueryNamespace(namespace: NamespaceId): NamespaceStandingQueries =
    // Uses `getOrElseUpdate` because its value is call-by-name.
    namespaceStandingQueries.getOrElseUpdate(namespace, new NamespaceStandingQueries(namespace))

  def removeStandingQueryNamespace(namespace: NamespaceId): Option[Unit] =
    namespaceStandingQueries.remove(namespace).map(_.cancelAllStandingQueries())

  val dgnRegistry: DomainGraphNodeRegistry = new DomainGraphNodeRegistry(
    metrics.registerGaugeDomainGraphNodeCount,
    namespacePersistor.persistDomainGraphNodes,
    namespacePersistor.removeDomainGraphNodes
  )

  class NamespaceStandingQueries(namespace: NamespaceId) {

    /** Map of all currently live standing queries */
    private val standingQueries: concurrent.Map[StandingQueryId, RunningStandingQuery] =
      new ConcurrentHashMap[StandingQueryId, RunningStandingQuery]().asScala

    def runningStandingQueries: Map[StandingQueryId, RunningStandingQuery] = standingQueries.toMap

    def runningStandingQuery(standingQueryId: StandingQueryId): Option[RunningStandingQuery] =
      standingQueries.get(standingQueryId)

    def cancelAllStandingQueries(): Unit = {
      standingQueries.foreach { case (sqid, _) =>
        cancelStandingQuery(sqid, skipPersistor = true)
      }
      standingQueries.clear()
    }

    // NB this initialization needs to occur before we restore standing queries,
    // otherwise a null pointer exception occurs
    private val standingQueryPartIndex
      : LoadingCache[MultipleValuesStandingQueryPartId, MultipleValuesStandingQuery] = CacheBuilder
      .newBuilder()
      .asInstanceOf[CacheBuilder[Any, Any]] // The Java library says this is `AnyRef`, but we want `Any`
      .weakValues()
      .build(new CacheLoader[MultipleValuesStandingQueryPartId, MultipleValuesStandingQuery] {
        override def loadAll(
          keys: lang.Iterable[_ <: MultipleValuesStandingQueryPartId]
        ): util.Map[MultipleValuesStandingQueryPartId, MultipleValuesStandingQuery] = {
          logger.info(
            s"Performing a full update of the standingQueryPartIndex because of query for part IDs ${keys.asScala.toList}"
          )
          val runningSqs = runningStandingQueries.values
            .map(_.query.query)
            .collect { case StandingQueryPattern.MultipleValuesQueryPattern(sq, _, _) => sq }
            .toVector
          val runningSqParts = runningSqs.toSet
            .foldLeft(Set.empty[MultipleValuesStandingQuery])((acc, sq) =>
              MultipleValuesStandingQuery.indexableSubqueries(sq, acc)
            )
            .map(sq => sq.id -> sq)
            .toMap
          val runningPartsKeys = runningSqParts.keySet
          keys.asScala.collectFirst {
            case part if !runningPartsKeys.contains(part) =>
              logger.warn(s"Unable to find running Standing Query part: $part")
          }
          runningSqParts.asJava
        }
        def load(k: MultipleValuesStandingQueryPartId): MultipleValuesStandingQuery = loadAll(Seq(k).asJava).get(k)
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
      .traverse(runningStandingQueries.values)((query: RunningStandingQuery) => query.terminateOutputQueue())(
        implicitly,
        shardDispatcherEC
      )
      .map(_ => ())(ExecutionContext.parasitic)

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
      sqId: StandingQueryId
    ): (RunningStandingQuery, Map[String, UniqueKillSwitch]) = {
      requireCompatibleNodeType()
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
        namespacePersistor(namespace)
          .getOrElse(
            throw new IllegalArgumentException(
              s"Could not persist standing query because namespace: $namespace does not exist."
            )
          )
          .persistStandingQuery(rsqAndOutputs._1.query)
      }
      rsqAndOutputs
    }

    /** Start a standing query that will be registered on all nodes awoken in the graph
      *
      * INV: This will never throw [[GraphNotReadyException]], because it is used as part of readying a graph
      */
    def startStandingQuery(
      sqId: StandingQueryId,
      name: String,
      pattern: StandingQueryPattern,
      outputs: Map[String, Flow[StandingQueryResult, SqResultsExecToken, NotUsed]],
      queueBackpressureThreshold: Int,
      queueMaxSize: Int,
      shouldCalculateResultHashCode: Boolean
    ): (RunningStandingQuery, Map[String, UniqueKillSwitch]) = {
      val sq =
        StandingQuery(name, sqId, pattern, queueBackpressureThreshold, queueMaxSize, shouldCalculateResultHashCode)
      val (runningSq, killSwitches) = runStandingQuery(sq, outputs)
      standingQueries.put(sqId, runningSq)

      // if this is a cypher SQ (at runtime), also register its components in the index as appropriate
      pattern match {
        case runsAsCypher: StandingQueryPattern.MultipleValuesQueryPattern =>
          standingQueryPartIndex.putAll(
            MultipleValuesStandingQuery
              .indexableSubqueries(runsAsCypher.compiledQuery)
              .view
              .map(sq => sq.id -> sq)
              .toMap
              .asJava
          )
        case _: StandingQueryPattern.DomainGraphNodeStandingQueryPattern =>
        case _: StandingQueryPattern.QuinePatternQueryPattern =>
      }

      (runningSq, killSwitches)
    }

    /** Cancel a standing query
      *
      * @param standingQueryId which standing query to cancel
      * @param skipPersistor whether to skip modifying durable storage
      * @return Some Future that will return the final state of the standing query, or [[None]] if the standing query
      *         doesn't exist
      */
    def cancelStandingQuery(
      standingQueryId: StandingQueryId,
      skipPersistor: Boolean = false
    ): Option[Future[(StandingQuery, Instant, Int)]] = {
      requireCompatibleNodeType()
      requiredGraphIsReady()
      standingQueries.remove(standingQueryId).map { (sq: RunningStandingQuery) =>
        val persistence = (
          if (skipPersistor) Future.unit
          else namespacePersistor(namespace).map(_.removeStandingQuery(sq.query)).getOrElse(Future.unit)
        ).flatMap { _ =>
          sq.query.query match {
            case dgnPattern: DomainGraphNodeStandingQueryPattern =>
              val dgnPackage = DomainGraphNodePackage(dgnPattern.dgnId, dgnRegistry.getDomainGraphNode(_))
              dgnRegistry.unregisterDomainGraphNodePackage(dgnPackage, standingQueryId, skipPersistor)
            case _ => Future.unit
          }
        }(shardDispatcherEC)
        val cancellation = sq.terminateOutputQueue()
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
      requireCompatibleNodeType()
      runningStandingQueries.fmap(sq => (sq.query, sq.startTime, sq.bufferCount))
    }

    /** Fetch a source to wire-tap a standing query
      *
      * @return source to wire-tap or [[None]] if the standing query doesn't exist
      */
    def wireTapStandingQuery(standingQueryId: StandingQueryId): Option[Source[StandingQueryResult, NotUsed]] = {
      requireCompatibleNodeType()
      runningStandingQuery(standingQueryId).map(_.resultsHub)
    }

    /** Ensure universal standing queries have been propagated out to all the
      * right nodes
      *
      * @param parallelism propagate to how many nodes at once? (if unset, doesn't wake nodes)
      * @param timeout max time to wait for any particular node (not for the whole propagation)
      * @return future that completes when all the messages have been fired off
      */
    def propagateStandingQueries(parallelism: Option[Int])(implicit
      timeout: Timeout
    ): Future[Unit] = {
      requireCompatibleNodeType()
      requiredGraphIsReady()
      parallelism match {
        case Some(par) =>
          enumerateAllNodeIds(namespace)
            .mapAsyncUnordered(par)(qid =>
              relayAsk(SpaceTimeQuineId(qid, namespace, None), UpdateStandingQueriesWake(_))
            )
            .run()
            .map(_ => ())(ExecutionContext.parasitic)

        case None =>
          enumerateAllNodeIds(namespace)
            .map(qid => relayTell(SpaceTimeQuineId(qid, namespace, None), UpdateStandingQueriesNoWake))
            .run()
            .map(_ => ())(ExecutionContext.parasitic)
      }
    }

    @throws[NoSuchElementException]("When a MultipleValuesStandingQueryPartId is not known to this graph")
    def getStandingQueryPart(queryPartId: MultipleValuesStandingQueryPartId): MultipleValuesStandingQuery =
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
        .queue[StandingQueryResult](
          sq.queueMaxSize // Queue of top-level results for this StandingQueryId on this member
        )
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
        namespace,
        resultsHub = resultsHub,
        outputTermination = term,
        metrics = metrics
      )
      (runningStandingQuery, killSwitches)
    }
  }
}

object StandingQueryOpsGraph {

  /** Check if a graph supports standing query operations and refine it if possible */
  def apply(graph: BaseGraph): Option[StandingQueryOpsGraph] = PartialFunction.condOpt(graph) {
    case sqog: StandingQueryOpsGraph => sqog
  }
}
