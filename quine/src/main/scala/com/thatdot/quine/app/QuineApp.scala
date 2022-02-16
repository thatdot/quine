package com.thatdot.quine.app

import java.time.Instant
import java.time.temporal.ChronoUnit.MILLIS

import scala.collection.compat._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

import akka.actor.ActorSystem
import akka.stream.contrib.SwitchMode
import akka.stream.scaladsl.Keep
import akka.stream.{KillSwitches, UniqueKillSwitch}
import akka.util.Timeout

import cats.implicits._
import com.typesafe.scalalogging.LazyLogging

import com.thatdot.quine.app.importers.createIngestStream
import com.thatdot.quine.app.routes.{
  AdministrationRoutesState,
  IngestMetered,
  IngestMetrics,
  IngestStreamState,
  IngestStreamWithControl,
  QueryUiConfigurationState,
  StandingQueryStore
}
import com.thatdot.quine.graph.MasterStream.SqResultsSrcType
import com.thatdot.quine.graph.{GraphQueryPattern, GraphService, HostQuineMetrics, StandingQuery, StandingQueryId}
import com.thatdot.quine.model.QuineIdProvider
import com.thatdot.quine.persistor.{PersistenceAgent, Version}
import com.thatdot.quine.routes.StandingQueryPattern.StandingQueryMode
import com.thatdot.quine.routes.{
  IngestSchemas,
  IngestStreamConfiguration,
  QueryUiConfigurationSchemas,
  RatesSummary,
  RegisteredStandingQuery,
  SampleQuery,
  StandingQueryDefinition,
  StandingQueryPattern,
  StandingQueryResultOutputUserDef,
  StandingQuerySchemas,
  StandingQueryStats,
  UiNodeAppearance,
  UiNodeQuickQuery
}

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
    with com.thatdot.quine.routes.exts.UjsonAnySchema
    with LazyLogging {

  import ApiConverters._
  import QuineApp._

  implicit private[this] val idProvider: QuineIdProvider = graph.idProvider
  implicit private[this] val system: ActorSystem = graph.system // implicitly creates a Materializer

  @volatile
  private[this] var sampleQueries: Vector[SampleQuery] = Vector.empty
  @volatile
  private[this] var quickQueries: Vector[UiNodeQuickQuery] = Vector.empty
  @volatile
  private[this] var nodeAppearances: Vector[UiNodeAppearance] = Vector.empty
  @volatile
  private[this] var standingQueryOutputTargets
    : Map[String, (StandingQueryId, Map[String, (StandingQueryResultOutputUserDef, UniqueKillSwitch)])] = Map.empty
  @volatile
  private[this] var ingestStreams: Map[String, IngestStreamWithControl[IngestStreamConfiguration]] = Map.empty

  def getStartingQueries: Future[Vector[SampleQuery]] = Future.successful(sampleQueries)

  def getQuickQueries: Future[Vector[UiNodeQuickQuery]] = Future.successful(quickQueries)

  def getNodeAppearances: Future[Vector[UiNodeAppearance]] = Future.successful(nodeAppearances)

  def setSampleQueries(newSampleQueries: Vector[SampleQuery]): Future[Unit] = {
    sampleQueries = newSampleQueries
    storeGlobalMetaData(SampleQueriesKey, sampleQueries)
  }

  def setQuickQueries(newQuickQueries: Vector[UiNodeQuickQuery]): Future[Unit] = {
    quickQueries = newQuickQueries
    storeGlobalMetaData(QuickQueriesKey, quickQueries)
  }

  def setNodeAppearances(newNodeAppearances: Vector[UiNodeAppearance]): Future[Unit] = {
    nodeAppearances = newNodeAppearances
    storeGlobalMetaData(NodeAppearancesKey, nodeAppearances)
  }
  type FriendlySQName = String
  type SQOutputName = String

  implicit private[this] val standingQueriesSchema
    : JsonSchema[Map[FriendlySQName, (StandingQueryId, Map[SQOutputName, StandingQueryResultOutputUserDef])]] = {
    implicit val sqIdSchema = genericJsonSchema[StandingQueryId]
    implicit val tupSchema = genericJsonSchema[(StandingQueryId, Map[SQOutputName, StandingQueryResultOutputUserDef])]
    mapJsonSchema(tupSchema)
  }

  private[this] def storeStandingQueries(): Future[Unit] =
    storeGlobalMetaData(
      StandingQueryOutputsKey,
      standingQueryOutputTargets.map { case (name, (id, outputsMap)) =>
        name -> (id -> outputsMap.view.mapValues(_._1).toMap)
      }.toMap
    )

  def addStandingQuery(queryName: FriendlySQName, query: StandingQueryDefinition): Future[Boolean] =
    if (standingQueryOutputTargets.contains(queryName)) {
      Future.successful(false)
    } else {
      val sqResultsConsumers = query.outputs.map { case (k, v) =>
        k -> StandingQueryResultOutput.resultHandlingFlow(k, v, graph)
      }
      val (sq, killSwitches) = graph.createStandingQuery(
        queryName,
        pattern = compileToInternalSqPattern(query, graph),
        outputs = sqResultsConsumers,
        queueBackpressureThreshold = query.inputBufferSize
      )

      val outputsWithKillSwitches = query.outputs.map { case (name, out) =>
        name -> (out -> killSwitches(name))
      }
      standingQueryOutputTargets += queryName -> (sq.query.id -> outputsWithKillSwitches)
      storeStandingQueries().map(_ => true)
    }

  def cancelStandingQuery(
    queryName: String
  ): Future[Option[RegisteredStandingQuery]] = {
    val optionResult: Option[Future[RegisteredStandingQuery]] = for {
      (sqId, outputs) <- standingQueryOutputTargets.get(queryName)
      futSq <- graph.cancelStandingQuery(sqId)
    } yield {
      standingQueryOutputTargets -= queryName
      futSq.map { case (internalSq, startTime, bufferSize) =>
        makeRegisteredStandingQuery(internalSq, outputs.mapValues(_._1), startTime, bufferSize, graph.metrics)
      }
    }

    optionResult.sequence.zipWith(storeStandingQueries())((toReturn, _) => toReturn)
  }

  def addStandingQueryOutput(
    queryName: String,
    outputName: String,
    sqResultOutput: StandingQueryResultOutputUserDef
  ): Future[Option[Boolean]] = {
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
        storeStandingQueries().map(_ => true)
      }

    optionFut match {
      case None => Future.successful(None)
      case Some(fut) => fut.map(Some(_))
    }
  }

  def removeStandingQueryOutput(
    queryName: String,
    outputName: String
  ): Future[Option[StandingQueryResultOutputUserDef]] = {
    val outputOpt = for {
      (sqId, outputs) <- standingQueryOutputTargets.get(queryName)
      (output, killSwitch) <- outputs.get(outputName)
    } yield {
      killSwitch.shutdown()
      standingQueryOutputTargets += queryName -> (sqId -> (outputs - outputName))
      output
    }

    storeStandingQueries().map(_ => outputOpt)
  }

  def getStandingQueries(): Future[List[RegisteredStandingQuery]] =
    getStandingQueriesWithNames(Nil)

  def getStandingQuery(queryName: String): Future[Option[RegisteredStandingQuery]] =
    getStandingQueriesWithNames(List(queryName)).map(_.headOption)

  /** Get standing queries live on the graph with the specified names
    *
    * @param names which standing queries to retrieve, empty list corresponds to all SQs
    * @return queries registered on the graph
    */
  private def getStandingQueriesWithNames(queryNames: List[String]): Future[List[RegisteredStandingQuery]] = {
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

  def addIngestStream(
    name: String,
    settings: IngestStreamConfiguration,
    wasRestoredFromStorage: Boolean,
    timeout: Timeout
  ): Try[Boolean] =
    if (ingestStreams.contains(name)) {
      Success(false)
    } else
      // Because this stores asynchronously with `storeLocalMetaData`, concurrent calls to this
      // function run the risk of a race condition overwriting with older data and losing data.
      // Since the function is called infrequently, taking the easy way out with `synchronized`.
      ingestStreams.synchronized {
        Try {
          val meter = IngestMetered.ingestMeter(name)
          val ingestSrc = createIngestStream(
            name,
            settings,
            meter,
            if (wasRestoredFromStorage) SwitchMode.Close else SwitchMode.Open
          )(graph, implicitly, implicitly)

          val controlFuture = graph.masterStream.addIngestSrc(ingestSrc)
          val ingestControl = Await.result(controlFuture, timeout.duration)

          val streamDefWithControl = IngestStreamWithControl(
            settings,
            IngestMetrics(Instant.now, None, meter),
            Future.successful(ingestControl.valveHandle),
            restored = wasRestoredFromStorage
          )
          streamDefWithControl.close = () => {
            ingestControl.terminate(); ()
          }
          streamDefWithControl.terminated = ingestControl.termSignal
          streamDefWithControl.registerTerminationHooks(name, logger)
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
      }

  def getIngestStream(name: String): Option[IngestStreamWithControl[IngestStreamConfiguration]] =
    ingestStreams.get(name)

  def getIngestStreams(): Map[String, IngestStreamWithControl[IngestStreamConfiguration]] =
    ingestStreams

  def removeIngestStream(
    name: String
  ): Option[IngestStreamWithControl[IngestStreamConfiguration]] = Try {
    val thisMemberId = 0
    ingestStreams.synchronized { // `synchronized` is tolerable because it's low-traffic
      ingestStreams.get(name).map { stream =>
        ingestStreams -= name
        storeLocalMetaData(
          IngestStreamsKey,
          thisMemberId,
          ingestStreams.map { case (name, stream) => name -> stream.settings }.toMap
        )
        stream
      }
    }
  }.toOption.flatten

  /** If any ingest streams are currently in the `RESTORED` state, start them all. */
  def startRestoredIngests(): Unit =
    ingestStreams.foreach {
      case (_, streamWithControl) if streamWithControl.restored =>
        streamWithControl.valve.map { v =>
          // In Quine App, this future is always created with `Future.success(…)`. This is ugly.
          v.flip(SwitchMode.Open)
          streamWithControl.restored = false // NB: this is not actually done in a separate thread. see previous comment
        }
      case _ => ()
    }

  private def stopAllIngestStreams(): Future[Unit] =
    Future
      .traverse(ingestStreams: TraversableOnce[(String, IngestStreamWithControl[IngestStreamConfiguration])]) {
        case (name, ingest) =>
          IngestMetered.removeIngestMeter(name)
          ingest.close()
          ingest.terminated.recover { case _ => () }
      }
      .map(_ => ())

  /** Prepare for a shutdown */
  def shutdown(): Future[Unit] =
    stopAllIngestStreams() // ... but don't update what is saved to disk

  /** Load all the state from the persistor
    *
    * @param timeout used repeatedly for individual calls to get meta data when restoring ingest streams.
    * @param restoreIngest should restored ingest streams be resumed
    */
  def load(timeout: Timeout, shouldResumeIngest: Boolean): Future[Unit] = {
    val sampleQueriesFut =
      getOrDefaultGlobalMetaData(SampleQueriesKey, SampleQuery.defaults)
    val quickQueriesFut = getOrDefaultGlobalMetaData(QuickQueriesKey, UiNodeQuickQuery.defaults)
    val nodeAppearancesFut = getOrDefaultGlobalMetaData(NodeAppearancesKey, UiNodeAppearance.defaults)
    val standingQueriesFut = getOrDefaultGlobalMetaData(
      StandingQueryOutputsKey,
      graph.runningStandingQueries.view.map { case (sqId, runningSq) =>
        runningSq.query.name -> (sqId -> Map.empty[String, StandingQueryResultOutputUserDef])
      }.toMap
    )
    val ingestStreamFut =
      getOrDefaultLocalMetaData(IngestStreamsKey, 0, Map.empty[String, IngestStreamConfiguration])

    for {
      sq <- sampleQueriesFut
      qq <- quickQueriesFut
      na <- nodeAppearancesFut
      st <- standingQueriesFut
      is <- ingestStreamFut
    } yield {
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
    }
  }
}

object QuineApp {

  import ApiConverters._

  final val VersionKey = "quine_app_state_version"
  final val SampleQueriesKey = "sample_queries"
  final val QuickQueriesKey = "quick_queries"
  final val NodeAppearancesKey = "node_appearances"
  final val StandingQueryOutputsKey = "standing_query_outputs"
  final val IngestStreamsKey = "ingest_streams"

  /** Version to track schemas saved by Quine app state
    *
    * Remember to increment this if schemas in Quine app state evolve in
    * backwards incompatible ways.
    */
  final val CurrentPersistenceVersion: Version = Version(1, 0, 0)

  def quineAppIsEmpty(persistenceAgent: PersistenceAgent)(implicit ec: ExecutionContext): Future[Boolean] = {
    val metaDataKeys =
      List(SampleQueriesKey, QuickQueriesKey, NodeAppearancesKey, StandingQueryOutputsKey, IngestStreamsKey)
    Future.foldLeft(metaDataKeys.map(k => persistenceAgent.getMetaData(k).map(_.isEmpty)))(true)(_ && _)
  }

  import com.thatdot.quine._

  /** Aggregate Quine SQ outputs and Quine standing query into a user-facing SQ
    *
    * @note this includes only local information/metrics!
    * @param internal Quine representation of the SQ
    * @param outputs SQ outputs registered on the query
    * @param startTime when the query was started (or re-started)
    * @param bufferSize number of elements buffered in the SQ output queue
    * @param metrics Quine metrics object
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
      case _: graph.StandingQueryPattern.Branch => StandingQueryMode.DistinctId
      case _: graph.StandingQueryPattern.SqV4 => StandingQueryMode.MultipleValues
    }
    val pattern = internal.query.origin match {
      case graph.PatternOrigin.GraphPattern(_, Some(cypherQuery)) =>
        Some(StandingQueryPattern.Cypher(cypherQuery, mode))

      case graph.PatternOrigin.GraphPattern(
            GraphQueryPattern(nodes, edges, startingPoint, toExtract, None, Nil, distinct @ _),
            None
          ) =>
        // TODO add `distinct` to StandingQueryPattern.Graph when MultipleValues mode supports it
        Some(
          StandingQueryPattern.Graph(
            nodes.map(nodePattern),
            edges.map(edgePattern),
            startingPoint.id,
            toExtract.map(returnColumn),
            mode
          )
        )

      case _ =>
        None
    }

    val meter = metrics.standingQueryResultMeter(internal.name)

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
          bufferSize
        )
      )
    )
  }
}
