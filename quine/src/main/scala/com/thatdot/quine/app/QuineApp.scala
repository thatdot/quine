package com.thatdot.quine.app

import java.time.Instant
import java.time.temporal.ChronoUnit.MILLIS

import scala.collection.compat._
import scala.compat.ExecutionContexts
import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.concurrent.{Await, ExecutionContext, Future, blocking}
import scala.util.{Failure, Success, Try}

import org.apache.pekko.Done
import org.apache.pekko.stream.scaladsl.Keep
import org.apache.pekko.stream.{KillSwitches, UniqueKillSwitch}
import org.apache.pekko.util.Timeout

import cats.Applicative
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.instances.future.catsStdInstancesForFuture
import cats.syntax.all._
import com.typesafe.scalalogging.LazyLogging

import com.thatdot.quine.app.ingest.IngestSrcDef
import com.thatdot.quine.app.routes._
import com.thatdot.quine.compiler.cypher
import com.thatdot.quine.graph.InvalidQueryPattern._
import com.thatdot.quine.graph.MasterStream.SqResultsSrcType
import com.thatdot.quine.graph.StandingQueryPattern.{DomainGraphNodeStandingQueryPattern, MultipleValuesQueryPattern}
import com.thatdot.quine.graph.{
  GraphService,
  HostQuineMetrics,
  MemberIdx,
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

  // Constant member index 0 for Quine
  private val thisMemberIdx: MemberIdx = 0

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
            mode match {
              case StandingQueryMode.DistinctId =>
                if (!pattern.distinct) {
                  // TODO unit test this behavior
                  throw DistinctIdMustDistinct
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
              case StandingQueryMode.MultipleValues =>
                if (pattern.distinct) throw MultipleValuesCantDistinct
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
          storeStandingQueries().map(_ => true)(ExecutionContexts.parasitic)
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
        makeRegisteredStandingQuery(
          internalSq,
          outputs.fmap(_._1),
          startTime,
          bufferSize,
          graph.metrics
        )
      }(graph.system.dispatcher)
    }

    // must be implicit for cats sequence
    implicit val applicative: Applicative[Future] = catsStdInstancesForFuture(ExecutionContexts.parasitic)

    cancelledSqState.sequence productL storeStandingQueries()
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
        storeStandingQueries().map(_ => true)(ExecutionContexts.parasitic)
      }

    implicit val futureApplicative: Applicative[Future] = catsStdInstancesForFuture(ExecutionContexts.parasitic)
    optionFut.sequence
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

    storeStandingQueries().map(_ => outputOpt)(ExecutionContexts.parasitic)
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
        outputs.fmap(_._1),
        startTime,
        bufferSize,
        graph.metrics
      )

      Future.successful(matchingInfo.toList)
    }

  def getStandingQueryId(queryName: String): Option[StandingQueryId] =
    standingQueryOutputTargets.get(queryName).map(_._1)

  def registerTerminationHooks(name: String, metrics: IngestMetrics)(ec: ExecutionContext): Future[Done] => Unit = {
    termSignal =>
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
    // restoredStatus is None if stream was not restored at all
    restoredStatus: Option[IngestStreamStatus],
    shouldRestoreIngest: Boolean,
    timeout: Timeout
  ): Try[Boolean] =
    blocking(ingestStreamsLock.synchronized {
      val valveSwitchMode = restoredStatus match {
        case Some(restoredStatus) if shouldRestoreIngest =>
          restoredStatus.position match {
            case ValvePosition.Open => SwitchMode.Open
            case ValvePosition.Closed => SwitchMode.Close
          }
        case Some(_) =>
          SwitchMode.Close
        case None =>
          SwitchMode.Open
      }

      if (ingestStreams.contains(name)) {
        Success(false)
      } else
        IngestSrcDef
          .createIngestSrcDef(
            name,
            settings,
            valveSwitchMode
          )(graph)
          .leftMap(errs => IngestStreamConfiguration.InvalidStreamConfiguration(errs))
          .map { ingestSrcDef =>

            val metrics = IngestMetrics(Instant.now, None, ingestSrcDef.meter)
            val ingestSrc = ingestSrcDef.stream(
              registerTerminationHooks = registerTerminationHooks(name, metrics)(graph.nodeDispatcherEC)
            )

            graph.masterStream.addIngestSrc(ingestSrc)

            val streamDefWithControl = IngestStreamWithControl(
              settings,
              metrics,
              ingestSrcDef.getControl.map(_.valveHandle),
              ingestSrcDef.getControl.map(_.termSignal),
              ingestSrcDef.getControl.map { c => c.terminate(); () },
              restoredStatus = restoredStatus
            )

            ingestStreams += name -> streamDefWithControl

            Await.result(
              syncIngestStreamsMetaData(thisMemberIdx),
              timeout.duration
            )

            true
          }
          .toEither
          .toTry
    })

  def getIngestStream(name: String): Option[IngestStreamWithControl[IngestStreamConfiguration]] =
    ingestStreams.get(name)

  def getIngestStreams(): Map[String, IngestStreamWithControl[IngestStreamConfiguration]] =
    ingestStreams

  protected def getIngestStreamsWithStatus: Future[Map[String, IngestStreamWithStatus]] = {
    implicit val ec: ExecutionContext = graph.nodeDispatcherEC
    ingestStreams.toList
      .traverse { case (name, stream) =>
        for {
          status <- stream.status(graph.materializer)
        } yield (name, IngestStreamWithStatus(stream.settings, Some(status)))
      }
      .map(_.toMap)(ExecutionContexts.parasitic)
  }

  private def syncIngestStreamsMetaData(thisMemberId: Int): Future[Unit] = {
    implicit val ec: ExecutionContext = graph.nodeDispatcherEC
    for {
      streamsWithStatus <- getIngestStreamsWithStatus
      res <- storeLocalMetaData[Map[String, IngestStreamWithStatus]](
        IngestStreamsKey,
        thisMemberId,
        streamsWithStatus
      )
    } yield res
  }

  def removeIngestStream(
    name: String
  ): Option[IngestStreamWithControl[IngestStreamConfiguration]] = Try {
    blocking(ingestStreamsLock.synchronized {
      ingestStreams.get(name).map { stream =>
        ingestStreams -= name
        Await.result(
          syncIngestStreamsMetaData(thisMemberIdx),
          QuineApp.ConfigApiTimeout
        )
        stream
      }
    })
  }.toOption.flatten

  /** == Utilities == */

  private def stopAllIngestStreams(): Future[Unit] =
    Future
      .traverse(ingestStreams.toList) { case (name, ingest) =>
        IngestMetered.removeIngestMeter(name)
        ingest.close.unsafeRunAndForget()
        ingest.terminated.recover { case _ => Future.successful(Done) }.unsafeToFuture().flatten
      }(implicitly, graph.system.dispatcher)
      .map(_ => ())(graph.system.dispatcher)

  /** Prepare for a shutdown */
  def shutdown()(implicit ec: ExecutionContext): Future[Unit] =
    for {
      _ <- syncIngestStreamsMetaData(thisMemberIdx)
      _ <- stopAllIngestStreams() // ... but don't update what is saved to disk
    } yield ()

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
      getOrDefaultLocalMetaDataWithFallback[Map[String, IngestStreamWithStatus], Map[
        String,
        IngestStreamConfiguration
      ]](
        IngestStreamsKey,
        thisMemberIdx,
        Map.empty[String, IngestStreamWithStatus],
        _.fmap(i => IngestStreamWithStatus(config = i, status = None))
      )

    {
      implicit val ec: ExecutionContext = graph.system.dispatcher
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
      val filterDeleted = st.filter { case (_, (sqId, _)) =>
        val queryExists = graphStandingQueryIds.contains(sqId)
        graphStandingQueryIds -= sqId
        queryExists
      }
      val augmented = filterDeleted ++ graphStandingQueryIds
        .map { case (sqId, runningSq) =>
          runningSq._1.name -> (sqId -> Map.empty[String, StandingQueryResultOutputUserDef])
        }
      standingQueryOutputTargets = augmented.fmap { case (sqId, outputsStored) =>
        val sqResultSource = graph.wireTapStandingQuery(sqId).get // we check above the SQ exists
        val outputs = outputsStored.map { case (outputName, sqResultOutput) =>
          val sqResultSrc: SqResultsSrcType = sqResultSource
            .viaMat(KillSwitches.single)(Keep.right)
            .via(StandingQueryResultOutput.resultHandlingFlow(outputName, sqResultOutput, graph))
          val killSwitch = graph.masterStream.addSqResultsSrc(sqResultSrc)
          outputName -> (sqResultOutput -> killSwitch)
        }

        sqId -> outputs
      }

      is.foreach { case (name, ingest) =>
        addIngestStream(name, ingest.config, restoredStatus = ingest.status, shouldResumeIngest, timeout) match {
          case Success(true) => ()
          case Success(false) =>
            logger.error(s"Duplicate ingest stream attempted to start with name: $name and settings: ${ingest.config}")
          case Failure(e) =>
            logger.error(s"Error when restoring ingest stream: $name with settings: ${ingest.config}", e)
        }
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
        name -> (id -> outputsMap.fmap(_._1))
      }
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
  final val CurrentPersistenceVersion: Version = Version(1, 2, 0)

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
