package com.thatdot.quine.app

import java.time.Instant
import java.time.temporal.ChronoUnit.MILLIS

import scala.collection.compat._
import scala.compat.ExecutionContexts
import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.concurrent.{Await, ExecutionContext, Future, blocking}
import scala.util.{Failure, Success, Try}

import akka.Done
import akka.stream.scaladsl.Keep
import akka.stream.{KillSwitches, UniqueKillSwitch}
import akka.util.Timeout

import cats.effect.unsafe.implicits.global
import cats.implicits._
import com.typesafe.scalalogging.LazyLogging

import com.thatdot.quine.app.ingest.IngestSrcDef
import com.thatdot.quine.app.routes._
import com.thatdot.quine.compiler.cypher
import com.thatdot.quine.graph.MasterStream.SqResultsSrcType
import com.thatdot.quine.graph.StandingQueryPattern.{DomainGraphNodeStandingQueryPattern, MultipleValuesQueryPattern}
import com.thatdot.quine.graph.{
  GraphService,
  HostQuineMetrics,
  InvalidQueryPattern,
  PatternOrigin,
  StandingQuery,
  StandingQueryId
}
import com.thatdot.quine.model.QuineIdProvider
import com.thatdot.quine.persistor.{PersistenceAgent, Version}
import com.thatdot.quine.routes.StandingQueryPattern.StandingQueryMode
import com.thatdot.quine.routes._
import com.thatdot.quine.util.SwitchMode

/** The Quine application state
  *
  * @param graph reference to the underlying graph
  */
final class QuineApp(graph: GraphService)
    extends BaseApp(graph)
    with AdministrationRoutesState
    with QueryUiConfigurationState
    with StandingQueryStore
    with IngestStreamState
    with QueryUiConfigurationSchemas
    with StandingQuerySchemas
    with IngestSchemas
    with com.thatdot.quine.routes.exts.CirceJsonAnySchema
    with LazyLogging {

  import QuineApp._

  implicit private[this] val idProvider: QuineIdProvider = graph.idProvider

  /** == Local state ==
    * Notes on synchronization:
    * Accesses to the following collections must be threadsafe. Additionally, the persisted copy of these collections
    * (ie those accessed by `*Metadata` functions) must be kept in sync with the in-memory copy. Because all of these
    * functions are expected to have a low volume of usage, and thus don't need to be performance-optimized, we
    * aggressively synchronize on `this`. In particular, synchronizing on the variable itself is not sufficient, because
    * the lock offered by `synchronize` is with respect to the locked *value*, not the locked *field* -- so locking on
    * a mutating variable does not result in a mutex. By contrast, locking on `this` is more than is strictly necessary,
    * but represents a deliberate choice to simplify the synchronization logic at the cost of reduced performance,
    * as all these synchronization points should be low-volume.
    *
    * In the case of collections with only `get`/`set` functions, the @volatile annotation is sufficient to ensure the
    * thread-safety of `get`. `set` functions must synchronize with a lock on `this` to ensure that setting both the
    * in-memory and persisted copies of the collection happens at the same time.
    *
    * Get/set example:
    * - `getQuickQueries` relies only on @volatile for its synchronization, because @volatile ensures all threads
    * read the same state of the underlying `quickQueries` variable
    * - `setQuickQueries` is wrapped in a `this.synchronized` to ensure that 2 simultaneous calls to `setQuickQueries`
    * will not interleave their local and remote update steps. Without synchronized, execution (1) might set the local
    * variable while execution (2) sets the persisted version
    *
    * In the case of collections with update (eg `add`/`remove`) semantics, all accesses must be synchronized
    * with a lock on `this`, because all accesses involve both a read and a write which might race concurrent executions.
    *
    * Add example:
    * - `addIngestStream` is wrapped in a `this.synchronized` because the updates it makes to `ingestStreams` depend on
    * the results of a read of `ingestStreams`. Thus, the read and the write must happen atomically with respect to
    * other `addIngestStream` invocations. Additionally, the `synchronized` ensures the local and persisted copies of
    * the collection are kept in sync (as in the get/set case)
    *
    * Additionally, note that each synchronized{} block forces execution synchronization of futures it invokes (ie,
    * each time a future is created, it is Await-ed). By Await-ing all futures created, we ensure that the
    * synchronization boundary accounts for *all* work involved in the operation, not just the parts that happen on the
    * local thread. TODO: instead of Await(), use actors or strengthen persistor guarantees to preserve happens-before
    */

  @volatile
  private[this] var sampleQueries: Vector[SampleQuery] = Vector.empty
  // Locks are on the object; we can't use a var (e.g. the collection) as something to synchronize on
  // as it's always being updated to point to a new object.
  final private[this] val sampleQueriesLock = new AnyRef
  @volatile
  private[this] var quickQueries: Vector[UiNodeQuickQuery] = Vector.empty
  final private[this] val quickQueriesLock = new AnyRef
  @volatile
  private[this] var nodeAppearances: Vector[UiNodeAppearance] = Vector.empty
  final private[this] val nodeAppearancesLock = new AnyRef
  @volatile
  private[this] var standingQueryOutputTargets
    : Map[String, (StandingQueryId, Map[String, (StandingQueryResultOutputUserDef, UniqueKillSwitch)])] = Map.empty
  final private[this] val standingQueryOutputTargetsLock = new AnyRef
  @volatile
  private[this] var ingestStreams: Map[String, IngestStreamWithControl[IngestStreamConfiguration]] = Map.empty
  final private[this] val ingestStreamsLock = new AnyRef

  /** == Accessors == */

  def getSampleQueries: Future[Vector[SampleQuery]] = Future.successful(sampleQueries)

  def getQuickQueries: Future[Vector[UiNodeQuickQuery]] = Future.successful(quickQueries)

  def getNodeAppearances: Future[Vector[UiNodeAppearance]] = Future.successful(nodeAppearances)

  def setSampleQueries(newSampleQueries: Vector[SampleQuery]): Future[Unit] =
    synchronizedFakeFuture(sampleQueriesLock) {
      sampleQueries = newSampleQueries
      storeGlobalMetaData(SampleQueriesKey, sampleQueries)
    }

  def setQuickQueries(newQuickQueries: Vector[UiNodeQuickQuery]): Future[Unit] =
    synchronizedFakeFuture(quickQueriesLock) {
      quickQueries = newQuickQueries
      storeGlobalMetaData(QuickQueriesKey, quickQueries)
    }

  def setNodeAppearances(newNodeAppearances: Vector[UiNodeAppearance]): Future[Unit] =
    synchronizedFakeFuture(nodeAppearancesLock) {
      nodeAppearances = newNodeAppearances.map(QueryUiConfigurationState.renderNodeIcons)
      storeGlobalMetaData(NodeAppearancesKey, nodeAppearances)
    }

  def addStandingQuery(queryName: FriendlySQName, query: StandingQueryDefinition): Future[Boolean] =
    synchronizedFakeFuture(standingQueryOutputTargetsLock) {
      if (standingQueryOutputTargets.contains(queryName)) {
        Future.successful(false)
      } else {
        val sqId = StandingQueryId.fresh()
        val sqResultsConsumers = query.outputs.map { case (k, v) =>
          k -> StandingQueryResultOutput.resultHandlingFlow(k, v, graph)
        }
        val (pattern, dgnPackage) = query.pattern match {
          case StandingQueryPattern.Cypher(cypherQuery, mode) =>
            val pattern = cypher.compileStandingQueryGraphPattern(cypherQuery)(graph.idProvider)
            val origin = PatternOrigin.GraphPattern(pattern, Some(cypherQuery))
            if (mode == StandingQueryMode.DistinctId) {
              if (!pattern.distinct) {
                // TODO unit test this behavior
                throw InvalidQueryPattern("DistinctId Standing Queries must specify a `DISTINCT` keyword")
              }
              val (branch, returnColumn) = pattern.compiledDomainGraphBranch(graph.labelsProperty)
              val dgnPackage = branch.toDomainGraphNodePackage
              val dgnPattern = DomainGraphNodeStandingQueryPattern(
                dgnPackage.dgnId,
                returnColumn.formatAsString,
                returnColumn.aliasedAs,
                query.includeCancellations,
                origin
              )
              (dgnPattern, Some(dgnPackage))
            } else {
              if (pattern.distinct)
                throw InvalidQueryPattern("MultipleValues Standing Queries do not yet support `DISTINCT`") // QU-568
              val compiledQuery = pattern.compiledMultipleValuesStandingQuery(graph.labelsProperty, idProvider)
              val sqv4Pattern = MultipleValuesQueryPattern(compiledQuery, query.includeCancellations, origin)
              (sqv4Pattern, None)
            }
        }
        (dgnPackage match {
          case Some(p) => graph.dgnRegistry.registerAndPersistDomainGraphNodePackage(p, sqId)
          case None => Future.unit
        }).flatMap { _ =>
          val (sq, killSwitches) = graph.createStandingQuery(
            queryName,
            pattern,
            outputs = sqResultsConsumers,
            queueBackpressureThreshold = query.inputBufferSize,
            shouldCalculateResultHashCode = query.shouldCalculateResultHashCode,
            sqId = sqId
          )
          val outputsWithKillSwitches = query.outputs.map { case (name, out) =>
            name -> (out -> killSwitches(name))
          }
          standingQueryOutputTargets += queryName -> (sq.query.id -> outputsWithKillSwitches)
          storeStandingQueries().map(_ => true)(graph.system.dispatcher)
        }(graph.system.dispatcher)
      }
    }

  def cancelStandingQuery(
    queryName: String
  ): Future[Option[RegisteredStandingQuery]] = synchronizedFakeFuture(standingQueryOutputTargetsLock) {
    val cancelledSqState: Option[Future[RegisteredStandingQuery]] = for {
      (sqId, outputs) <- standingQueryOutputTargets.get(queryName)
      cancelledSq <- graph.cancelStandingQuery(sqId)
    } yield {
      standingQueryOutputTargets -= queryName
      cancelledSq.map { case (internalSq, startTime, bufferSize) =>
        makeRegisteredStandingQuery(internalSq, outputs.mapValues(_._1), startTime, bufferSize, graph.metrics)
      }(graph.system.dispatcher)
    }

    // must be implicit for cats sequence
    implicit val applicative: cats.Applicative[Future] = catsStdInstancesForFuture(graph.system.dispatcher)

    cancelledSqState.sequence
      .zipWith(storeStandingQueries())((toReturn, _) => toReturn)(graph.system.dispatcher)
  }

  def addStandingQueryOutput(
    queryName: String,
    outputName: String,
    sqResultOutput: StandingQueryResultOutputUserDef
  ): Future[Option[Boolean]] = synchronizedFakeFuture(standingQueryOutputTargetsLock) {
    val optionFut = for {
      (sqId, outputs) <- standingQueryOutputTargets.get(queryName)
      sqResultSource <- graph.wireTapStandingQuery(sqId)
    } yield
      if (outputs.contains(outputName)) {
        Future.successful(false)
      } else {
        val sqResultSrc: SqResultsSrcType = sqResultSource
          .viaMat(KillSwitches.single)(Keep.right)
          .via(StandingQueryResultOutput.resultHandlingFlow(outputName, sqResultOutput, graph))
        val killSwitch = graph.masterStream.addSqResultsSrc(sqResultSrc)
        standingQueryOutputTargets += queryName -> (sqId -> (outputs + (outputName -> (sqResultOutput -> killSwitch))))
        storeStandingQueries().map(_ => true)(graph.system.dispatcher)
      }

    optionFut match {
      case None => Future.successful(None)
      case Some(fut) => fut.map(Some(_))(graph.system.dispatcher)
    }
  }

  def removeStandingQueryOutput(
    queryName: String,
    outputName: String
  ): Future[Option[StandingQueryResultOutputUserDef]] = synchronizedFakeFuture(standingQueryOutputTargetsLock) {
    val outputOpt = for {
      (sqId, outputs) <- standingQueryOutputTargets.get(queryName)
      (output, killSwitch) <- outputs.get(outputName)
    } yield {
      killSwitch.shutdown()
      standingQueryOutputTargets += queryName -> (sqId -> (outputs - outputName))
      output
    }

    storeStandingQueries().map(_ => outputOpt)(graph.system.dispatcher)
  }

  def getStandingQueries(): Future[List[RegisteredStandingQuery]] =
    getStandingQueriesWithNames(Nil)

  def getStandingQuery(queryName: String): Future[Option[RegisteredStandingQuery]] =
    getStandingQueriesWithNames(List(queryName)).map(_.headOption)(graph.system.dispatcher)

  /** Get standing queries live on the graph with the specified names
    *
    * @param names which standing queries to retrieve, empty list corresponds to all SQs
    * @return queries registered on the graph
    */
  private def getStandingQueriesWithNames(queryNames: List[String]): Future[List[RegisteredStandingQuery]] =
    synchronizedFakeFuture(standingQueryOutputTargetsLock) {
      val matchingInfo = for {
        queryName <- queryNames match {
          case Nil => standingQueryOutputTargets.keys
          case names => names
        }
        (sqId, outputs) <- standingQueryOutputTargets.get(queryName)
        (internalSq, startTime, bufferSize) <- graph.listStandingQueries.get(sqId)
      } yield makeRegisteredStandingQuery(
        internalSq,
        outputs.mapValues(_._1),
        startTime,
        bufferSize,
        graph.metrics
      )

      Future.successful(matchingInfo.toList)
    }

  def getStandingQueryId(queryName: String): Option[StandingQueryId] =
    standingQueryOutputTargets.get(queryName).map(_._1)

  def registerTerminationHooks(name: String, metrics: IngestMetrics)(ec: ExecutionContext): Future[Done] => Unit =
    termSignal => {
      termSignal.onComplete {
        case Failure(err) =>
          val now = Instant.now
          metrics.stop(now)
          logger.error(
            s"Ingest stream '$name' has failed after ${metrics.millisSinceStart(now)}ms",
            err
          )

        case Success(_) =>
          val now = Instant.now
          metrics.stop(now)
          logger.info(
            s"Ingest stream '$name' successfully completed after ${metrics.millisSinceStart(now)}ms"
          )
      }(ec)
    }
  def addIngestStream(
    name: String,
    settings: IngestStreamConfiguration,
    wasRestoredFromStorage: Boolean,
    timeout: Timeout
  ): Try[Boolean] =
    blocking(ingestStreamsLock.synchronized {
      if (ingestStreams.contains(name)) {
        Success(false)
      } else
        Try {
          val ingestSrcDef = IngestSrcDef.createIngestSrcDef(
            name,
            settings,
            if (wasRestoredFromStorage) SwitchMode.Close else SwitchMode.Open
          )(graph)

          val metrics = IngestMetrics(Instant.now, None, ingestSrcDef.meter)
          val ingestSrc = ingestSrcDef.stream(
            shouldResumeIngest = false,
            registerTerminationHooks = registerTerminationHooks(name, metrics)(graph.nodeDispatcherEC)
          )

          graph.masterStream.addIngestSrc(ingestSrc)

          val streamDefWithControl = IngestStreamWithControl(
            settings,
            metrics,
            ingestSrcDef.getControl.map(_.valveHandle),
            ingestSrcDef.getControl.map(_.termSignal),
            ingestSrcDef.getControl.map { c => c.terminate(); () },
            restored = wasRestoredFromStorage
          )

          ingestStreams += name -> streamDefWithControl

          val thisMemberId = 0
          Await.result(
            storeLocalMetaData(
              IngestStreamsKey,
              thisMemberId,
              ingestStreams.map { case (name, stream) => name -> stream.settings }.toMap
            ),
            timeout.duration
          )

          true
        }
    })

  def getIngestStream(name: String): Option[IngestStreamWithControl[IngestStreamConfiguration]] =
    ingestStreams.get(name)

  def getIngestStreams(): Map[String, IngestStreamWithControl[IngestStreamConfiguration]] =
    ingestStreams

  def removeIngestStream(
    name: String
  ): Option[IngestStreamWithControl[IngestStreamConfiguration]] = Try {
    val thisMemberId = 0
    blocking(ingestStreamsLock.synchronized {
      ingestStreams.get(name).map { stream =>
        ingestStreams -= name
        Await.result(
          storeLocalMetaData(
            IngestStreamsKey,
            thisMemberId,
            ingestStreams.map { case (name, stream) => name -> stream.settings }.toMap
          ),
          QuineApp.ConfigApiTimeout
        )
        stream
      }
    })
  }.toOption.flatten

  /** == Utilities == */

  /** If any ingest streams are currently in the `RESTORED` state, start them all. */
  def startRestoredIngests(): Unit =
    ingestStreams.foreach {
      case (_, streamWithControl) if streamWithControl.restored =>
        streamWithControl.valve
          .unsafeToFuture()
          .map { v =>
            // In Quine App, this future is always created with `Future.success(…)`. This is ugly.
            v.flip(SwitchMode.Open)
            streamWithControl.restored =
              false // NB: this is not actually done in a separate thread. see previous comment
          }(graph.system.dispatcher)
      case _ => ()
    }

  private def stopAllIngestStreams(): Future[Unit] =
    Future
      .traverse(ingestStreams: TraversableOnce[(String, IngestStreamWithControl[IngestStreamConfiguration])]) {
        case (name, ingest) =>
          IngestMetered.removeIngestMeter(name)
          ingest.close.unsafeRunAndForget()
          ingest.terminated.recover { case _ => () }.unsafeToFuture()
      }(implicitly, graph.system.dispatcher)
      .map(_ => ())(graph.system.dispatcher)

  /** Prepare for a shutdown */
  def shutdown(): Future[Unit] =
    stopAllIngestStreams() // ... but don't update what is saved to disk

  /** Load all the state from the persistor
    *
    * Not threadsafe - this can race API calls made while starting the system
    *
    * @param timeout       used repeatedly for individual calls to get meta data when restoring ingest streams.
    * @param restoreIngest should restored ingest streams be resumed
    */
  def load(timeout: Timeout, shouldResumeIngest: Boolean): Future[Unit] = {
    val sampleQueriesFut =
      getOrDefaultGlobalMetaData(SampleQueriesKey, SampleQuery.defaults)
    val quickQueriesFut = getOrDefaultGlobalMetaData(QuickQueriesKey, UiNodeQuickQuery.defaults)
    val nodeAppearancesFut = getOrDefaultGlobalMetaData(NodeAppearancesKey, UiNodeAppearance.defaults)
    val standingQueriesFut = getOrDefaultGlobalMetaData(
      StandingQueryOutputsKey,
      graph.runningStandingQueries.map { case (sqId, runningSq) =>
        runningSq.query.name -> (sqId -> Map.empty[String, StandingQueryResultOutputUserDef])
      }.toMap
    )
    val ingestStreamFut =
      getOrDefaultLocalMetaData(IngestStreamsKey, 0, Map.empty[String, IngestStreamConfiguration])

    {
      implicit val ec = graph.system.dispatcher
      for {
        sq <- sampleQueriesFut
        qq <- quickQueriesFut
        na <- nodeAppearancesFut
        st <- standingQueriesFut
        is <- ingestStreamFut
      } yield (sq, qq, na, st, is)
    }.map { case (sq, qq, na, st, is) =>
      sampleQueries = sq
      quickQueries = qq
      nodeAppearances = na

      var graphStandingQueryIds = graph.listStandingQueries
      val filterDeleted = st.view.filter { case (_, (sqId, _)) =>
        val queryExists = graphStandingQueryIds.contains(sqId)
        graphStandingQueryIds -= sqId
        queryExists
      }.toMap
      val augmented = filterDeleted ++ graphStandingQueryIds
        .map { case (sqId, runningSq) =>
          runningSq._1.name -> (sqId -> Map.empty[String, StandingQueryResultOutputUserDef])
        }
      standingQueryOutputTargets = augmented.view.mapValues { case (sqId, outputsStored) =>
        val sqResultSource = graph.wireTapStandingQuery(sqId).get // we check above the SQ exists
        val outputs = outputsStored.view.map { case (outputName, sqResultOutput) =>
          val sqResultSrc: SqResultsSrcType = sqResultSource
            .viaMat(KillSwitches.single)(Keep.right)
            .via(StandingQueryResultOutput.resultHandlingFlow(outputName, sqResultOutput, graph))
          val killSwitch = graph.masterStream.addSqResultsSrc(sqResultSrc)
          outputName -> (sqResultOutput -> killSwitch)
        }.toMap

        sqId -> outputs
      }.toMap

      is.foreach { case (name, settings) =>
        addIngestStream(name, settings, wasRestoredFromStorage = true, timeout) match {
          case Success(true) => ()
          case Success(false) =>
            logger.error(s"Duplicate ingest stream attempted to start with name: $name and settings: $settings")
          case Failure(e) =>
            logger.error(s"Error when restoring ingest stream: $name with settings: $settings", e)
        }
      }
      if (shouldResumeIngest) {
        startRestoredIngests()
      }
    }(graph.system.dispatcher)
  }

  type FriendlySQName = String
  type SQOutputName = String

  implicit private[this] val standingQueriesSchema
    : JsonSchema[Map[FriendlySQName, (StandingQueryId, Map[SQOutputName, StandingQueryResultOutputUserDef])]] = {
    implicit val sqIdSchema = genericRecord[StandingQueryId]
    implicit val tupSchema = genericRecord[(StandingQueryId, Map[SQOutputName, StandingQueryResultOutputUserDef])]
    mapJsonSchema(tupSchema)
  }

  private[this] def storeStandingQueries(): Future[Unit] =
    storeGlobalMetaData(
      StandingQueryOutputsKey,
      standingQueryOutputTargets.map { case (name, (id, outputsMap)) =>
        name -> (id -> outputsMap.view.mapValues(_._1).toMap)
      }.toMap
    )
}

object QuineApp {

  final val VersionKey = "quine_app_state_version"
  final val SampleQueriesKey = "sample_queries"
  final val QuickQueriesKey = "quick_queries"
  final val NodeAppearancesKey = "node_appearances"
  final val StandingQueryOutputsKey = "standing_query_outputs"
  final val IngestStreamsKey = "ingest_streams"

  // the maximum time to allow a configuring API call (eg, "add ingest query" or "update node appearances") to execute
  final val ConfigApiTimeout: FiniteDuration = 30.seconds

  /** Aggressively synchronize a unit of work returning a Future, and block on the Future's completion
    *
    * Multiple executions of synchronizedFakeFuture are guaranteed to not interleave any effects represented by their
    * arguments. This is used to ensure that local and persisted effects within `synchronizeMe` are fully applied
    * without interleaving. For certain persistors, such as Cassandra, synchronization (without an Await) would be
    * sufficient, because the Cassandra persistor guarantees that effects started in sequence will be applied in the
    * same sequence.
    *
    * NB while this does inherit the reentrance properties of `synchronized`, this function might still be prone to
    * deadlocking! Use with *extreme* caution!
    */
  private[app] def synchronizedFakeFuture[T](lock: AnyRef)(synchronizeMe: => Future[T]): Future[T] = blocking(
    lock.synchronized(
      Await.ready(synchronizeMe: Future[T], QuineApp.ConfigApiTimeout)
    )
  )

  /** Version to track schemas saved by Quine app state
    *
    * Remember to increment this if schemas in Quine app state evolve in
    * backwards incompatible ways.
    */
  final val CurrentPersistenceVersion: Version = Version(1, 1, 0)

  def quineAppIsEmpty(persistenceAgent: PersistenceAgent): Future[Boolean] = {
    val metaDataKeys =
      List(SampleQueriesKey, QuickQueriesKey, NodeAppearancesKey, StandingQueryOutputsKey, IngestStreamsKey)
    Future.foldLeft(
      metaDataKeys.map(k => persistenceAgent.getMetaData(k).map(_.isEmpty)(ExecutionContexts.parasitic))
    )(true)(_ && _)(ExecutionContexts.parasitic)
  }

  import com.thatdot.quine._

  /** Aggregate Quine SQ outputs and Quine standing query into a user-facing SQ
    *
    * @note this includes only local information/metrics!
    * @param internal   Quine representation of the SQ
    * @param outputs    SQ outputs registered on the query
    * @param startTime  when the query was started (or re-started)
    * @param bufferSize number of elements buffered in the SQ output queue
    * @param metrics    Quine metrics object
    */
  private def makeRegisteredStandingQuery(
    internal: StandingQuery,
    outputs: Map[String, StandingQueryResultOutputUserDef],
    startTime: Instant,
    bufferSize: Int,
    metrics: HostQuineMetrics
  )(implicit
    idProvider: QuineIdProvider
  ): RegisteredStandingQuery = {
    val mode = internal.query match {
      case _: graph.StandingQueryPattern.DomainGraphNodeStandingQueryPattern => StandingQueryMode.DistinctId
      case _: graph.StandingQueryPattern.MultipleValuesQueryPattern => StandingQueryMode.MultipleValues
    }
    val pattern = internal.query.origin match {
      case graph.PatternOrigin.GraphPattern(_, Some(cypherQuery)) =>
        Some(StandingQueryPattern.Cypher(cypherQuery, mode))
      case _ =>
        None
    }

    val meter = metrics.standingQueryResultMeter(internal.name)
    val outputHashCode = metrics.standingQueryResultHashCode(internal.id)

    RegisteredStandingQuery(
      internal.name,
      internal.id.uuid,
      pattern,
      outputs,
      internal.query.includeCancellation,
      internal.queueBackpressureThreshold,
      stats = Map(
        "local" -> StandingQueryStats(
          rates = RatesSummary(
            count = meter.getCount,
            oneMinute = meter.getOneMinuteRate,
            fiveMinute = meter.getFiveMinuteRate,
            fifteenMinute = meter.getFifteenMinuteRate,
            overall = meter.getMeanRate
          ),
          startTime,
          MILLIS.between(startTime, Instant.now()),
          bufferSize,
          outputHashCode.sum
        )
      )
    )
  }
}
