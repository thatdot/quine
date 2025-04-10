package com.thatdot.quine.graph

import java.time.Instant
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.concurrent
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}

import org.apache.pekko.stream.scaladsl.{BroadcastHub, Keep, Sink, Source}
import org.apache.pekko.stream.{BoundedSourceQueue, QueueOfferResult, UniqueKillSwitch}
import org.apache.pekko.util.Timeout
import org.apache.pekko.{Done, NotUsed}

import cats.implicits._
import com.github.blemale.scaffeine.{LoadingCache, Scaffeine}

import com.thatdot.common.logging.Log.{Safe, SafeLoggableInterpolator}
import com.thatdot.quine.graph.StandingQueryOpsGraph.StandingQueryPartNotFoundException
import com.thatdot.quine.graph.StandingQueryPattern.{DomainGraphNodeStandingQueryPattern, MultipleValuesQueryPattern}
import com.thatdot.quine.graph.cypher.MultipleValuesStandingQuery
import com.thatdot.quine.graph.cypher.quinepattern.LazyQuinePatternQueryPlanner.LazyQueryPlan
import com.thatdot.quine.graph.messaging.SpaceTimeQuineId
import com.thatdot.quine.graph.messaging.StandingQueryMessage._
import com.thatdot.quine.model.DomainGraphNodePackage
import com.thatdot.quine.util.Log.implicits._

/** Functionality for namespaced standing queries. */
trait StandingQueryOpsGraph extends BaseGraph {

  private[this] def requireCompatibleNodeType(): Unit = {
    requireBehavior[StandingQueryOpsGraph, behavior.MultipleValuesStandingQueryBehavior]
    requireBehavior[StandingQueryOpsGraph, behavior.DomainNodeIndexBehavior]
  }

  def standingQueries(namespace: NamespaceId): Option[NamespaceStandingQueries] =
    namespaceStandingQueries.get(namespace)

  case class RunningQuinePattern(
    plan: LazyQueryPlan,
    outputs: Map[String, Sink[StandingQueryResult, UniqueKillSwitch]],
  )

  val quinePatternLazyQueries: collection.concurrent.Map[StandingQueryId, RunningQuinePattern] =
    new ConcurrentHashMap[StandingQueryId, RunningQuinePattern]().asScala

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
    namespacePersistor.removeDomainGraphNodes,
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
    private val standingQueryPartIndex: LoadingCache[MultipleValuesStandingQueryPartId, MultipleValuesStandingQuery] = {
      def loadAll(
        keys: Iterable[MultipleValuesStandingQueryPartId],
      ): Map[MultipleValuesStandingQueryPartId, MultipleValuesStandingQuery] = {
        logger.info(
          safe"Performing a full update of the standingQueryPartIndex because of query for part IDs ${Safe(keys.toList.toString)}",
        )
        val runningSqs = runningStandingQueries.values
          .map(_.query.queryPattern)
          .collect { case MultipleValuesQueryPattern(sq, _, _) => sq }
          .toVector
        val runningSqParts = runningSqs.toSet
          .foldLeft(Set.empty[MultipleValuesStandingQuery])((acc, sq) =>
            MultipleValuesStandingQuery.indexableSubqueries(sq, acc),
          )
        val runningSqPartsMap = runningSqParts.groupBy(_.queryPartId).map { case (partId, sqs) =>
          if (sqs.size > 1)
            logger.error(
              log"""While re-indexing MultipleValues Standing Query parts, found multiple queries with
                   |the same part ID: $partId. This is a bug in MultipleValuesStandingQueryPartId
                   |generation, and nodes that register multiple of the query parts may cause loss of results.
                   |Queries are: ${sqs.toList.toString}
                   |""".cleanLines,
            )
          partId -> sqs.last // There is no right choice here, so we just pick one.
        }
        val runningPartsKeys = runningSqPartsMap.keySet
        keys.collectFirst {
          case partId if !runningPartsKeys.contains(partId) =>
            logger.warn(safe"Unable to find running Standing Query part: ${Safe(partId.toString)}")
        }
        runningSqPartsMap
      }
      Scaffeine()
        .weakValues()
        .build(
          loader = (key: MultipleValuesStandingQueryPartId) =>
            loadAll(Set(key)).getOrElse(
              key,
              throw new StandingQueryPartNotFoundException(key),
            ),
          allLoader = Some(loadAll),
        )

    }

    /** Report a new result for the specified standing query to this host's results queue for that query
      *
      * @note if the result is not positive and the query ignores cancellations, this is a no-op
      * @param sqId the standing query the result is for
      * @param sqResult the result to enqueue
      * @return if the result was successfully enqueued
      */
    def reportStandingResult(sqId: StandingQueryId, sqResult: SqResultLike): Boolean =
      runningStandingQuery(sqId) exists { standingQuery =>
        if (sqResult.isPositive || standingQuery.query.queryPattern.includeCancellation) {
          sqResult
            .standingQueryResults(standingQuery.query, idProvider)
            .forall(standingQuery.offerResult)
        } else {
          true
        }
      }

    /** Complete all standing query streams (since the graph is shutting down) */
    def shutdownStandingQueries(): Future[Unit] = Future
      .traverse(runningStandingQueries.values)((query: RunningStandingQuery) => query.terminateOutputQueue())(
        implicitly,
        shardDispatcherEC,
      )
      .map(_ => ())(ExecutionContext.parasitic)

    /** Register a new standing query
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
      outputs: Map[String, Sink[StandingQueryResult, UniqueKillSwitch]],
      queueBackpressureThreshold: Int = StandingQueryInfo.DefaultQueueBackpressureThreshold,
      queueMaxSize: Int = StandingQueryInfo.DefaultQueueMaxSize,
      shouldCalculateResultHashCode: Boolean = false,
      skipPersistor: Boolean = false,
      sqId: StandingQueryId,
    ): (RunningStandingQuery, Map[String, UniqueKillSwitch]) = {
      requireCompatibleNodeType()
      val rsqAndOutputs =
        startStandingQuery(
          sqId = sqId,
          name,
          pattern,
          outputs,
          queueBackpressureThreshold,
          queueMaxSize,
          shouldCalculateResultHashCode,
        )
      if (!skipPersistor) {
        namespacePersistor(namespace)
          .getOrElse(
            throw new IllegalArgumentException(
              s"Could not persist standing query because namespace: $namespace does not exist.",
            ),
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
      outputs: Map[String, Sink[StandingQueryResult, UniqueKillSwitch]],
      queueBackpressureThreshold: Int,
      queueMaxSize: Int,
      shouldCalculateResultHashCode: Boolean,
    ): (RunningStandingQuery, Map[String, UniqueKillSwitch]) = {
      val sq =
        StandingQueryInfo(name, sqId, pattern, queueBackpressureThreshold, queueMaxSize, shouldCalculateResultHashCode)
      val (runningSq, killSwitches) = runStandingQuery(sq, outputs)
      standingQueries.put(sqId, runningSq)

      // if this is a cypher SQ (at runtime), also register its components in the index as appropriate
      pattern match {
        case StandingQueryPattern.QuinePatternQueryPattern(plan) =>
          quinePatternLazyQueries.put(sqId, RunningQuinePattern(plan, outputs))
        case runsAsCypher: StandingQueryPattern.MultipleValuesQueryPattern =>
          val partsToAdd = MultipleValuesStandingQuery
            .indexableSubqueries(runsAsCypher.compiledQuery)
            .map(sq => sq.queryPartId -> sq)
            .foldLeft(Map.empty[MultipleValuesStandingQueryPartId, MultipleValuesStandingQuery]) {
              case (newPartsAcc, (partId, newPart)) =>
                // The part already registered with the graph for this PartId
                val previouslyRegisteredPart = standingQueryPartIndex.getIfPresent(partId)
                // The part already registered within this `startStandingQuery` for this PartId
                val alreadyInBatchPart = newPartsAcc.get(partId)

                // Decide what to do with the new part -- register it if it's a new part, log an error and ignore
                // it if it conflicts with another part, or ignore it if we've already registered the part
                val newPartRegistration =
                  if (previouslyRegisteredPart.exists(_ != newPart)) {
                    // conflict with already-registered part
                    logger.error(
                      safe"""While indexing MultipleValues Standing Query [part] $newPart (Part ID $partId) for standing
                           |query ${Safe(name)} (id $sqId), found that graph has already registered part ID $partId
                           |as a different query [part]: ${previouslyRegisteredPart.get}. This is a bug in the
                           |MultipleValuesStandingQueryPartId generation, and nodes that register both queries may
                           |miss results. Ignoring the new query part. Results for ID $partId will continue to go to
                           |already-registered part ${previouslyRegisteredPart.get}
                           |""".cleanLines,
                    )
                    None
                  } else if (alreadyInBatchPart.exists(_ != newPart)) {
                    // conflict within registration batch
                    logger.error(
                      safe"""While indexing MultipleValues Standing Query [part] $newPart (Part ID $partId) for standing
                           |query ${Safe(name)} (id $sqId), found that the query also defines another part with the same
                           |ID: ${previouslyRegisteredPart.get}. This is a bug in the MultipleValuesStandingQueryPartId
                           |generation, and nodes that register both queries may miss results. Ignoring the new query
                           |part. Results for ID $partId will go to first-seen part ${previouslyRegisteredPart.get}
                           |""".cleanLines,
                    )
                    None
                  } else if (alreadyInBatchPart.contains(newPart) || previouslyRegisteredPart.contains(newPart)) {
                    // already registered, no benefit to re-registering
                    logger.debug(
                      safe"While registering $newPart as $partId, found that it was already registered. Skipping.",
                    )
                    None
                  } else {
                    // not yet registered
                    logger.trace(
                      safe"Registering MVSQ part $newPart as $partId",
                    )
                    Some(partId -> newPart)
                  }

                newPartsAcc ++ newPartRegistration
            }

          standingQueryPartIndex.putAll(
            partsToAdd,
          )
        case _: StandingQueryPattern.DomainGraphNodeStandingQueryPattern =>
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
      skipPersistor: Boolean = false,
    ): Option[Future[(StandingQueryInfo, Instant, Int)]] = {
      requireCompatibleNodeType()
      // Removing from the `standingQueries` map is the authoritative decision. Absence == cancellation.
      standingQueries.remove(standingQueryId).map { (sq: RunningStandingQuery) =>
        val persistence = (
          if (skipPersistor) Future.unit
          else namespacePersistor(namespace).map(_.removeStandingQuery(sq.query)).getOrElse(Future.unit)
        ).flatMap { _ =>
          sq.query.queryPattern match {
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
      logger.error(log"Standing query output stream has failed for ${Safe(name)}:" withException err)

    /** List standing queries that are currently registered
      *
      * @return standing query, when it was started (or re-started), and the number of buffered results
      */
    def listStandingQueries: Map[StandingQueryId, (StandingQueryInfo, Instant, Int)] = {
      requireCompatibleNodeType()
      runningStandingQueries.fmap(sq => (sq.query, sq.startTime, sq.bufferCount))
    }

    /** Fetch a source to wire-tap a standing query
      *
      * @return source to wire-tap or [[None]] if the standing query doesn't exist
      */
    def standingResultsHub(standingQueryId: StandingQueryId): Option[Source[StandingQueryResult, NotUsed]] = {
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
      timeout: Timeout,
    ): Future[Unit] = {
      requireCompatibleNodeType()
      parallelism match {
        case Some(par) =>
          enumerateAllNodeIds(namespace)
            .mapAsyncUnordered(par)(qid =>
              relayAsk(SpaceTimeQuineId(qid, namespace, None), UpdateStandingQueriesWake(_)),
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

    // TODO switch this to a value-driven error handler
    @throws[StandingQueryPartNotFoundException]("When a MultipleValuesStandingQueryPartId is not known to this graph")
    def getStandingQueryPart(queryPartId: MultipleValuesStandingQueryPartId): MultipleValuesStandingQuery =
      try standingQueryPartIndex.get(queryPartId)
      catch {
        case e: StandingQueryPartNotFoundException => throw e
        case e: IllegalArgumentException =>
          // thrown by scaffeine when it detects infinite loops
          throw new StandingQueryPartNotFoundException(
            s"Looking up standing query part for ID: $queryPartId caused an infinite loop. Short-circuiting.",
            e,
          )
        case e: RuntimeException =>
          throw new StandingQueryPartNotFoundException(queryPartId, e)
      }

    private def runStandingQuery(
      sq: StandingQueryInfo,
      outputs: Map[String, Sink[StandingQueryResult, UniqueKillSwitch]],
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

      val ((queue, term), resultsHub: Source[StandingQueryResult.WithQueueTimer, NotUsed]) = Source
        .queue[StandingQueryResult.WithQueueTimer](
          sq.queueMaxSize, // Queue of top-level results for this StandingQueryId on this member
        )
        .watchTermination() { (mat, done) =>
          done.onComplete { (_: Try[Done]) =>
            if (sq.queueBackpressureThreshold <= inBuffer.getAndSet(0)) {
              ingestValve.open()
            }
          }(shardDispatcherEC)
          mat -> done
        }
        .map { (x: StandingQueryResult.WithQueueTimer) =>
          if (sq.queueBackpressureThreshold == inBuffer.getAndDecrement()) {
            ingestValve.open()
          }
          x
        }
        .named(s"sq-results-for-${sq.name}")
        .toMat(
          BroadcastHub.sink[StandingQueryResult.WithQueueTimer](bufferSize = 8).named(s"sq-results-hub-for-${sq.name}"),
        )(Keep.both)
        // bufferSize = 8 ensures all consumers attached to the hub are kept within 8 elements of each other
        .run() // materialize the stream from result queue to broadcast hub

      val timedResultsHub: Source[StandingQueryResult, NotUsed] = resultsHub.map {
        case StandingQueryResult.WithQueueTimer(r, timerCtx) =>
          timerCtx.stop()
          r
      }

      term.onComplete {
        case Failure(err) =>
          // If the output stream gets terminated badly, cancel the standing query and log the error
          logSqOutputFailure(sq.name, err)
          cancelStandingQuery(sq.id)
        case Success(_) => // Do nothing. This is the shutdown case.
      }(shardDispatcherEC)

      // Start each output stream by attaching to the SQ results hub and the completion tokens stream,
      // accumulating a registry of each output's kill switch
      val killSwitches: Map[String, UniqueKillSwitch] = outputs.view.mapValues { outputStream =>
        timedResultsHub.runWith(outputStream) // materialize the stream from the broadcasthub to the token sink
      }.toMap

      val runningStandingQuery = new RunningStandingQuery(
        resultsQueue = new BoundedSourceQueue[StandingQueryResult.WithQueueTimer] {
          def isCompleted: Boolean = queue.isCompleted
          def complete() = queue.complete()
          def fail(ex: Throwable) = queue.fail(ex)
          def size() = queue.size()
          def offer(r: StandingQueryResult.WithQueueTimer) = {
            val res = queue.offer(r)
            if (res == QueueOfferResult.Enqueued) {
              if (sq.queueBackpressureThreshold == inBuffer.incrementAndGet())
                ingestValve.close()
              if (sq.shouldCalculateResultHashCode)
                // Integrate each standing query result hash code using `add`
                // so the result is order agnostic
                metrics.standingQueryResultHashCode(sq.id).add(r.result.dataHashCode)
            }
            res
          }
        },
        query = sq,
        namespace,
        resultsHub = timedResultsHub,
        outputTermination = term,
        metrics = metrics,
      )
      (runningStandingQuery, killSwitches)
    }
  }
}

object StandingQueryOpsGraph {

  class StandingQueryPartNotFoundException(message: String, cause: Throwable) extends RuntimeException(message, cause) {
    def this(partId: MultipleValuesStandingQueryPartId, cause: Throwable = null) = this(
      s"No standing query part with ID $partId could be found among the currently-running standing queries.",
      cause,
    )
  }

  /** Check if a graph supports standing query operations and refine it if possible */
  def apply(graph: BaseGraph): Option[StandingQueryOpsGraph] = PartialFunction.condOpt(graph) {
    case sqog: StandingQueryOpsGraph => sqog
  }
}
