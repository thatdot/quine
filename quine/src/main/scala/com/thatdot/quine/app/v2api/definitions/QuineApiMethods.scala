package com.thatdot.quine.app.v2api.definitions

import java.util.Properties

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters.CollectionHasAsScala
import scala.util.Either

import org.apache.pekko.stream.{Materializer, StreamDetachedException}
import org.apache.pekko.util.Timeout

import cats.data.{EitherT, NonEmptyList}
import cats.implicits._
import shapeless.{:+:, CNil, Coproduct}

import com.thatdot.api.v2.ErrorDetail
import com.thatdot.api.v2.ErrorResponse.{BadRequest, NotFound, ServerError}
import com.thatdot.api.v2.outputs.{Format, OutputFormat}
import com.thatdot.common.logging.Log._
import com.thatdot.common.quineid.QuineId
import com.thatdot.quine.app.config.BaseConfig
import com.thatdot.quine.app.model.ingest.util.KafkaSettingsValidator
import com.thatdot.quine.app.model.ingest.util.KafkaSettingsValidator.ErrorString
import com.thatdot.quine.app.model.ingest2.V2IngestEntities.{QuineIngestConfiguration => V2IngestConfiguration}
import com.thatdot.quine.app.model.ingest2.source.QuineValueIngestQuery
import com.thatdot.quine.app.model.ingest2.{KafkaIngest, V2IngestEntities}
import com.thatdot.quine.app.model.outputs2.query.{AllNodeScanException, CypherQuery => CypherQueryModel}
import com.thatdot.quine.app.routes._
import com.thatdot.quine.app.v2api.converters._
import com.thatdot.quine.app.v2api.definitions.ApiUiStyling.{SampleQuery, TapQuery, UiNodeAppearance, UiNodeQuickQuery}
import com.thatdot.quine.app.v2api.definitions.ingest2.{ApiIngest, DeadLetterQueueOutput}
import com.thatdot.quine.app.v2api.definitions.outputs.QuineDestinationSteps
import com.thatdot.quine.app.v2api.definitions.query.{standing => ApiStanding}
import com.thatdot.quine.app.v2api.endpoints.V2AdministrationEndpointEntities
import com.thatdot.quine.app.v2api.endpoints.V2AdministrationEndpointEntities.{TGraphHashCode, TQuineInfo}
import com.thatdot.quine.app.{BaseApp, BuildInfo, SchemaCache}
import com.thatdot.quine.compiler.cypher
import com.thatdot.quine.exceptions.NamespaceNotFoundException
import com.thatdot.quine.graph.cypher.CypherException
import com.thatdot.quine.graph.{
  AlgorithmGraph,
  BaseGraph,
  CypherOpsGraph,
  InMemoryNodeLimit,
  InvalidQueryPattern,
  LiteralOpsGraph,
  MemberIdx,
  NamespaceId,
  StandingQueryOpsGraph,
}
import com.thatdot.quine.model.Milliseconds
import com.thatdot.quine.persistor.PersistenceAgent
import com.thatdot.quine.util.StringInput.filenameOrUrl
import com.thatdot.quine.{BuildInfo => QuineBuildInfo, routes => V1}

sealed trait ProductVersion
object ProductVersion {
  case object Novelty extends ProductVersion

  case object Oss extends ProductVersion

  case object Enterprise extends ProductVersion
}

object ApplicationApiMethods {

  /** How a caller enumerates ONE namespace's ingest streams for the snapshot. Same shape as
    * [[IngestStreamState.getV2IngestStreams]] applied to one namespace with no member filter.
    *
    * IMPORTANT: implementations must enumerate the LOCAL host's ingests only. QuineEnterpriseApp's
    * `getV2IngestStreams` override routes through the cluster interface, so passing it here from the
    * per-member backpressure callback would nest a cluster-wide broadcast inside the broadcast and
    * attribute every member's ingests to every host's snapshot.
    */
  type LocalIngestEnumeration =
    NamespaceId => Future[Seq[(Option[MemberIdx], String, V2IngestEntities.IngestStreamInfo)]]

  /** Build a [[V2AdministrationEndpointEntities.BackpressureSnapshot]] for the local host from `graph`
    * alone, given a caller-supplied `hostInfo`/`persistorType` (these differ between a config-backed
    * caller like [[QuineApiMethods]] and a config-free caller like `QuineEnterpriseApp`, which builds
    * `hostInfo` from `graph.clusterFormationConfig` instead) and a caller-supplied local ingest
    * enumeration (see [[LocalIngestEnumeration]]).
    */
  def buildBackpressureSnapshot(
    graph: BaseGraph with StandingQueryOpsGraph,
    listLocalIngests: Option[LocalIngestEnumeration],
    hostInfo: V2AdministrationEndpointEntities.HostInfo,
    persistorType: String,
  ): Future[V2AdministrationEndpointEntities.BackpressureSnapshot] = {
    import V2AdministrationEndpointEntities._
    import com.thatdot.quine.util.BackpressureLevel
    val gaugeSnapshot = graph.pressureGaugeRegistry.snapshot()
    val closedCount = graph.ingestValve.getClosedCount

    def bpState(level: BackpressureLevel): String = level match {
      case BackpressureLevel.Flowing => "FLOWING"
      case BackpressureLevel.Constrained => "CONSTRAINED"
      case BackpressureLevel.Backpressured => "BACKPRESSURED"
    }

    // ── Persistor ──
    val metricsReport = GenerateMetrics.metricsReport(graph)
    val persistor = PersistorSnapshot(
      `type` = persistorType,
      writeLatencyMs = metricsReport.timers.find(_.name == "persistor.persist-event").map(_.mean).getOrElse(0.0),
      readLatencyMs = metricsReport.timers.find(_.name == "persistor.get-latest-snapshot").map(_.mean).getOrElse(0.0),
    )

    // ── Global valve ──
    val globalValve = GlobalValve(
      isOpen = closedCount == 0,
      closedCount = closedCount,
      oneMinuteClosures = graph.ingestValve.recentClosureCount,
    )

    // ── Ingests ──
    val gaugesByPipeline = gaugeSnapshot.groupBy { case (k, _) => (k.pipelineType, k.namespace, k.streamName) }

    // Per-stage gauge states for an ingest — present only while it is actively running.
    def ingestStages(ns: String, name: String): IngestStages = {
      val stageMap = gaugesByPipeline
        .getOrElse(("ingest", ns, name), Seq.empty)
        .map { case (k, v) => k.stagePosition -> bpState(v) }
        .toMap
      IngestStages(
        source = stageMap.getOrElse("source", "FLOWING"),
        preGraphWrite = stageMap.getOrElse("pre-graph-write", "FLOWING"),
        postGraphWrite = stageMap.get("post-graph-write"),
      )
    }

    // Enumerate ingests from the authoritative ingest manager, not the gauge registry, so that failed /
    // restored / terminal ingests stay visible with their real (live) status. A terminated ingest has
    // already deregistered its gauges, so a gauge-driven list would silently drop it.
    val ingestsF: Future[Seq[IngestSnapshot]] = listLocalIngests match {
      case None => Future.successful(Seq.empty)
      case Some(listIngests) =>
        implicit val ec: ExecutionContext = graph.nodeDispatcherEC
        Future
          .traverse(graph.getNamespaces.toSeq) { nsId =>
            listIngests(nsId).map { streams =>
              val ns = nsId.name
              streams.map { case (_, name, info) =>
                IngestSnapshot(
                  name = name,
                  namespace = ns,
                  sourceType = info.settings.source.getClass.getSimpleName.stripSuffix("$"),
                  status = info.status.toString.toUpperCase,
                  rateLimit = info.settings.maxPerSecond,
                  rate = info.stats.rates.oneMinute,
                  totalCount = info.stats.ingestedCount,
                  stages = ingestStages(ns, name),
                )
              }
            }
          }
          .map(_.flatten)
    }

    // ── Standing queries ──
    // Group SQ output gauges by (ns, sqName) → outputs
    import scala.jdk.CollectionConverters._
    val allMeters = graph.metrics.metricRegistry.getMeters.asScala

    val sqOutputPipelines: Seq[((String, String), SqOutputSnapshot)] = gaugesByPipeline.toSeq.collect {
      case (("sq-output", ns, streamName), stages) =>
        val parts = streamName.split('/')
        val sqName = parts.head
        val outputName = if (parts.length > 1) parts.tail.mkString("/") else streamName

        // Destinations from gauge keys. `stages` is a Map, so its iteration order is arbitrary —
        // sort by the destination's own index to give every host the same order. Without that, two
        // members' destination lists cannot be zipped together to attribute backpressure to a
        // cluster position, and even a single host's icons could reshuffle between polls.
        val destinations = stages.toSeq
          .collect {
            case (k, v) if k.stagePosition.startsWith("destination-") =>
              // stagePosition is "destination-<index>-<type-slug>" (see StandingQueryResultWorkflow).
              // The index disambiguates multiple destinations of the same type and is carried through
              // to the API; the slug that follows it names the type. The slug itself contains hyphens
              // ("standard-out"), so split on the FIRST hyphen only.
              val rest = k.stagePosition.stripPrefix("destination-")
              val index = rest.takeWhile(_.isDigit).toIntOption.getOrElse(0)
              val slug = rest.dropWhile(_ != '-').stripPrefix("-")
              // Capitalize slug to match API class naming: "standard-out" → "StandardOut", "drop" → "Drop"
              val destType = slug.split('-').map(_.capitalize).mkString
              DestinationSnapshot(index = index, `type` = destType, state = bpState(v))
          }
          .sortBy(_.index)

        // Enrichment detection
        val hasPreWorkflow = stages.keys.exists(_.stagePosition == "pre-workflow")
        val hasPostEnrichMeter = allMeters.contains(s"sq.output.$ns.$streamName.post-enrichment")
        val hasEnrichment = hasPreWorkflow || hasPostEnrichMeter
        val enrichmentState =
          if (hasEnrichment)
            stages
              .collectFirst { case (k, v) if k.stagePosition == "pre-workflow" => bpState(v) }
              .orElse(Some("FLOWING"))
          else None

        // Throughput meter
        val meterSuffix = if (hasEnrichment) "post-enrichment" else "throughput"
        val throughputMeter = graph.metrics.metricRegistry.meter(s"sq.output.$ns.$streamName.$meterSuffix")

        (ns, sqName) -> SqOutputSnapshot(
          name = outputName,
          rate = throughputMeter.getOneMinuteRate,
          totalCount = throughputMeter.getCount,
          hasEnrichment = hasEnrichment,
          enrichmentState = enrichmentState,
          destinations = destinations,
        )
    }

    // Outputs grouped by (ns, sqName). Only outputs register gauges, so an SQ with no outputs has no
    // entry here — which is why the SQ list below is driven by the authoritative running-SQ list.
    val outputsBySq: Map[(String, String), Seq[SqOutputSnapshot]] =
      sqOutputPipelines.groupBy(_._1).map { case (key, entries) => key -> entries.map(_._2) }

    // Enumerate standing queries from the LOCAL running-SQ list (this member only — not the cluster
    // interface), so SQs with no outputs still appear. Queue metrics come from the local running SQ;
    // outputs (with their gauges) are attached where present.
    val standingQueries: Seq[StandingQuerySnapshot] = for {
      nsId <- graph.getNamespaces.toSeq
      nsSqs <- graph.standingQueries(nsId).toSeq
      (_, runningSq) <- nsSqs.runningStandingQueries
    } yield {
      val sqInfo = runningSq.query
      val ns = nsId.name
      val threshold = sqInfo.queueBackpressureThreshold
      val maxSize = sqInfo.queueMaxSize
      val bufferCount = runningSq.bufferCount
      val consumptionMeter = graph.metrics.standingQueryConsumptionMeter(nsId, sqInfo.name)
      val queue = SqQueue(
        bufferCount = bufferCount,
        backpressureThreshold = threshold,
        maxSize = maxSize,
        thresholdRatio = if (threshold > 0) math.min(bufferCount.toDouble / threshold, 1.0) else 0.0,
        capacityRatio = if (maxSize > 0) math.min(bufferCount.toDouble / maxSize, 1.0) else 0.0,
        totalProduced = runningSq.resultMeter.getCount,
        totalCancellations = runningSq.cancellationMeter.getCount,
        totalDropped = runningSq.droppedCounter.getCount,
        totalConsumed = consumptionMeter.getCount,
        productionRate = runningSq.resultMeter.getOneMinuteRate,
        consumptionRate = consumptionMeter.getOneMinuteRate,
      )
      StandingQuerySnapshot(
        name = sqInfo.name,
        namespace = ns,
        queue = queue,
        outputs = outputsBySq.getOrElse((ns, sqInfo.name), Seq.empty),
      )
    }

    // Authoritative graph list — the same source and naming the standing-query enumeration above uses,
    // so idle graphs (no ingests, no standing queries) still reach the diagram rather than being
    // implied only by the pipelines that happen to be running in them.
    val namespaces: Seq[String] = graph.getNamespaces.toSeq.map(_.name).sorted

    ingestsF.map { ingests =>
      BackpressureSnapshot(
        timestamp = java.time.Instant.now(),
        host = hostInfo,
        cluster = None, // Overridden by enterprise
        namespaces = namespaces,
        globalValve = globalValve,
        ingests = ingests,
        standingQueries = standingQueries,
        persistor = persistor,
      )
    }(ExecutionContext.parasitic)
  }
}

trait ApplicationApiMethods {
  val graph: BaseGraph with LiteralOpsGraph with CypherOpsGraph with StandingQueryOpsGraph
  val app: BaseApp with SchemaCache with QueryUiConfigurationState
  def productVersion: ProductVersion
  implicit def timeout: Timeout
  implicit val logConfig: LogConfig
  implicit def materializer: Materializer = graph.materializer
  val config: BaseConfig

  def emptyConfigExample: BaseConfig

  def isReady: Boolean = graph.isReady

  def isLive = true

  // --------------------- Admin Endpoints ------------------------
  def performShutdown(): Future[Unit] = {
    graph.system.terminate()
    Future.successful(())
  }

  def graphHashCode(atTime: Option[Milliseconds], namespace: NamespaceId): Future[TGraphHashCode] =
    graph.requiredGraphIsReadyFuture {
      val at = atTime.getOrElse(Milliseconds.currentTime())
      graph
        .getGraphHashCode(namespace, Some(at))
        .map(elt => TGraphHashCode(elt.toString, java.time.Instant.ofEpochMilli(at.millis)))(ExecutionContext.parasitic)
    }

  def buildInfo: TQuineInfo = {
    val gitCommit: Option[String] = QuineBuildInfo.gitHeadCommit
      .map(_ + (if (QuineBuildInfo.gitUncommittedChanges) "-DIRTY" else ""))
    TQuineInfo(
      BuildInfo.version,
      gitCommit,
      QuineBuildInfo.gitHeadCommitDate,
      QuineBuildInfo.javaVmName + " " + QuineBuildInfo.javaVersion + " (" + QuineBuildInfo.javaVendor + ")",
      javaRuntimeVersion = Runtime.version().toString,
      javaAvailableProcessors = sys.runtime.availableProcessors(),
      javaMaxMemory = sys.runtime.maxMemory(),
      PersistenceAgent.CurrentVersion.shortString,
      quineType = productVersion.toString,
    )
  }

  def metaData(implicit ec: ExecutionContext): Future[Map[String, String]] =
    graph.namespacePersistor.getAllMetaData().flatMap { m =>
      Future.successful(m.view.mapValues(new String(_)).toMap)
    }

  def metrics(memberIdx: Option[MemberIdx]): Future[V1.MetricsReport] =
    Future.successful(GenerateMetrics.metricsReport(graph))

  def backpressureSnapshot(): Future[Seq[V2AdministrationEndpointEntities.BackpressureSnapshot]] = {
    import V2AdministrationEndpointEntities._

    // ── Host info ──
    val webserverConfig = config.systemConfigSummary.webserver
    val hostInfo = HostInfo(
      version = BuildInfo.version,
      address = webserverConfig.map(_.address).getOrElse("unknown"),
      port = webserverConfig.map(_.port).getOrElse(0),
      pid = ProcessHandle.current.pid(),
      // Single-host products have no cluster position. Enterprise overrides this whole method and
      // supplies each member's own index (see QuineEnterpriseApp.buildLocalBackpressureSnapshot).
      memberIdx = None,
    )
    // The loaded persistor's slug, not config's store.type: the slug reflects what actually
    // loaded, and the enterprise per-member path (which has no config reference) reports the
    // same slug, keeping the persistor type string identical across products.
    val persistorType = graph.namespacePersistor.slug

    // Local enumeration is correct here: this base implementation serves single-host products
    // (OSS/Novelty), whose IngestStreamState reads local state. Enterprise overrides this whole
    // method with a cluster broadcast and supplies its own local enumeration per member.
    val listLocalIngests: Option[ApplicationApiMethods.LocalIngestEnumeration] = app match {
      case iss: IngestStreamState => Some(iss.getV2IngestStreams(_, None))
      case _ => None
    }

    ApplicationApiMethods
      .buildBackpressureSnapshot(graph, listLocalIngests, hostInfo, persistorType)
      .map(Seq(_))(ExecutionContext.parasitic)
  }

  /** @param memberIdx cluster position whose shards to report; ignored here (the local graph
    *   is the only member), overridden where member targeting is meaningful.
    */
  def shardSizes(
    resizes: Map[Int, V1.ShardInMemoryLimit],
    memberIdx: Option[MemberIdx],
  ): Future[Map[Int, V1.ShardInMemoryLimit]] =
    graph
      .shardInMemoryLimits(resizes.fmap(l => InMemoryNodeLimit(l.softLimit, l.hardLimit)))
      .map(_.collect { case (shardIdx, Some(InMemoryNodeLimit(soft, hard))) =>
        shardIdx -> V1.ShardInMemoryLimit(soft, hard)
      })(ExecutionContext.parasitic)

  def requestNodeSleep(quineId: QuineId, namespaceId: NamespaceId): Future[Unit] =
    graph.requiredGraphIsReadyFuture(
      graph.requestNodeSleep(namespaceId, quineId),
    )

  def getSamplesQueries(implicit ctx: ExecutionContext): Future[Vector[SampleQuery]] =
    graph.requiredGraphIsReadyFuture(app.getSampleQueries).map(_.map(UiStylingToApi.apply))
  def getNodeAppearances(implicit ctx: ExecutionContext): Future[Vector[UiNodeAppearance]] =
    graph.requiredGraphIsReadyFuture(app.getNodeAppearances.map(_.map(UiStylingToApi.apply)))
  def getQuickQueries(implicit ctx: ExecutionContext): Future[Vector[UiNodeQuickQuery]] =
    graph.requiredGraphIsReadyFuture(app.getQuickQueries.map(_.map(UiStylingToApi.apply)))

  def analyze(queryText: String, parameters: Seq[String]): QueryEffects = {
    val compiled = cypher.compile(queryText, parameters)
    QueryEffects(
      isReadOnly = compiled.isReadOnly,
      canContainAllNodeScan = compiled.canContainAllNodeScan,
    )
  }

//  def isReadOnly(queryText: String, parameters: Seq[String]): Boolean = analyze(queryText, parameters).isReadOnly //cypher.compile(queryText, parameters).isReadOnly
}
// --------------------- End Admin Endpoints ------------------------

// retained functionality methods from in v1 route definitions
import com.thatdot.quine.app.routes.{AlgorithmMethods => V1AlgorithmMethods}

/** Encapsulates access to the running components of quine for individual endpoints. */
trait QuineApiMethods
    extends ApplicationApiMethods
    with V1AlgorithmMethods
    with CypherApiMethods
    with DebugApiMethods
    with AlgorithmApiMethods {

  override val graph: BaseGraph with LiteralOpsGraph with StandingQueryOpsGraph with CypherOpsGraph with AlgorithmGraph
  override val app: BaseApp
    with StandingQueryStoreV1
    with StandingQueryInterfaceV2
    with IngestStreamState
    with QueryUiConfigurationState
    with SchemaCache

  /** Cluster position to target when a request doesn't specify one; `None` when this node
    * has no cluster positions to target (single node). Not necessarily the serving host's
    * own position — implementations may choose another member (e.g. to route requests
    * served by a host that holds no position).
    */
  def defaultTargetMemberIdx: Option[Int]

  def tapBus: com.thatdot.quine.app.model.outputs2.query.standing.TapBus

  private def mkPauseOperationError(
    operation: String,
  ): PartialFunction[Throwable, Either[BadRequest, Nothing]] = {
    case _: StreamDetachedException =>
      // A StreamDetachedException always occurs when the ingest has failed
      Left(BadRequest.apply(s"Cannot $operation a failed ingest."))
    case e: IngestApiEntities.PauseOperationException =>
      Left(BadRequest.apply(s"Cannot $operation a ${e.statusMsg} ingest."))
  }

  //  endpoint business logic functionality.
  def getProperties: Future[Map[String, String]] = {
    val props: Properties = System.getProperties
    Future.successful(props.keySet.asScala.map(s => s.toString -> props.get(s).toString).toMap[String, String])
  }

  def getNamespaces: Future[List[String]] = Future.apply {
    graph.requiredGraphIsReady()
    app.getNamespaces.map(_.name).toList
  }(ExecutionContext.parasitic)

  def createNamespace(namespace: String): Future[Boolean] =
    app.createNamespace(NamespaceId(namespace))

  def deleteNamespace(namespace: String): Future[Boolean] =
    app.deleteNamespace(NamespaceId(namespace))

  def listAllStandingQueries: Future[List[ApiStanding.StandingQuery.RegisteredStandingQuery]] = {
    implicit val executor: ExecutionContext = ExecutionContext.parasitic
    Future
      .sequence(app.getNamespaces.map(app.getStandingQueriesV2))
      .map(_.toList.flatten)
  }

  // --------------------- Standing Query Endpoints ------------------------
  def listStandingQueries(
    namespaceId: NamespaceId,
  ): Future[Either[NotFound, List[ApiStanding.StandingQuery.RegisteredStandingQuery]]] =
    // Readiness is the App's concern: implementations serving from local state guard
    // internally, and those that route the request elsewhere need no ready local graph.
    app
      .getStandingQueriesV2(namespaceId)
      .map(Right(_): Either[NotFound, List[ApiStanding.StandingQuery.RegisteredStandingQuery]])(
        ExecutionContext.parasitic,
      )
      .recoverWith { case _: NamespaceNotFoundException =>
        Future.successful(Left(NotFound(s"Graph ${namespaceId.name} not found")))
      }(ExecutionContext.parasitic)

  def propagateStandingQuery(
    includeSleeping: Boolean,
    namespaceId: NamespaceId,
    wakeUpParallelism: Int,
  ): Future[Either[NotFound, Unit]] =
    if (!graph.getNamespaces.contains(namespaceId))
      Future.successful(Left(NotFound(s"Graph ${namespaceId.name} not found")))
    else
      graph
        .standingQueries(namespaceId)
        .fold(Future.successful[Either[NotFound, Unit]](Right(()))) {
          _.propagateStandingQueries(Some(wakeUpParallelism).filter(_ => includeSleeping))
            .map(_ => Right(()))(ExecutionContext.parasitic)
        }

  /** Default timeout for Kafka bootstrap server connectivity checks */
  private val KafkaConnectivityTimeout: FiniteDuration = 5.seconds

  private def validateCypherQuery(queryText: String, parameter: String, allowAllNodeScan: Boolean): Option[String] =
    try {
      CypherQueryModel.validateAndCompile(queryText, parameter, allowAllNodeScan)
      None
    } catch {
      case e: CypherException => Some(e.pretty)
      case e: AllNodeScanException => Some(e.getMessage)
    }

  private def validateProtobufSchema(
    format: OutputFormat,
  )(implicit ec: ExecutionContext): Future[Option[NonEmptyList[ErrorString]]] =
    format match {
      case OutputFormat.Protobuf(schemaUrl, typeName) =>
        app.protobufSchemaCache
          .getMessageDescriptor(filenameOrUrl(schemaUrl), typeName, flushOnFail = true)
          .map(_ => Option.empty[NonEmptyList[ErrorString]])
          .recover { case e: IllegalArgumentException =>
            Some(NonEmptyList.one(e.getMessage))
          }
      case _ => Future.successful(None)
    }

  private def validateDestinationSteps(
    destinationSteps: QuineDestinationSteps,
  )(implicit ec: ExecutionContext): Future[Option[NonEmptyList[ErrorString]]] =
    destinationSteps match {
      case k: QuineDestinationSteps.Kafka =>
        val kafkaErrors = KafkaSettingsValidator.validatePropertiesWithConnectivity(
          properties = k.kafkaProperties.view.mapValues(_.toString).toMap,
          bootstrapServers = k.bootstrapServers,
          timeout = KafkaConnectivityTimeout,
        )
        val protobufErrors = validateProtobufSchema(k.format)
        Future
          .sequence(List(kafkaErrors, protobufErrors))
          .map(_.foldLeft(Option.empty[NonEmptyList[ErrorString]])(_ |+| _))
      case cq: QuineDestinationSteps.CypherQuery =>
        Future.successful(
          validateCypherQuery(cq.query, cq.parameter, cq.allowAllNodeScan).map(NonEmptyList.one),
        )
      case f: Format =>
        validateProtobufSchema(f.format)
      case _ => Future.successful(None)
    }

  private def validateEnrichmentQuery(
    workflow: ApiStanding.StandingQueryResultWorkflow,
  ): Option[NonEmptyList[ErrorString]] =
    workflow.resultEnrichment.flatMap { cq =>
      validateCypherQuery(cq.query, cq.parameter, cq.allowAllNodeScan).map(e =>
        NonEmptyList.one(s"Enrichment query: $e"),
      )
    }

  private def validateWorkflow(
    workflow: ApiStanding.StandingQueryResultWorkflow,
  )(implicit ec: ExecutionContext): Future[Option[NonEmptyList[ErrorString]]] =
    Future
      .sequence(workflow.destinations.toList.map(validateDestinationSteps))
      .map { destinationErrors =>
        val allErrors = destinationErrors.foldLeft(Option.empty[NonEmptyList[ErrorString]])(_ |+| _)
        allErrors |+| validateEnrichmentQuery(workflow)
      }

  /** Validate DLQ Kafka destinations for an ingest configuration.
    * Checks connectivity to bootstrap servers for any Kafka DLQ destinations.
    */
  private def validateDlqDestinations(
    ingestConfig: V2IngestConfiguration,
  )(implicit ec: ExecutionContext): Future[Option[NonEmptyList[ErrorString]]] = {
    val kafkaDestinations = ingestConfig.onRecordError.deadLetterQueueSettings.destinations.collect {
      case k: DeadLetterQueueOutput.Kafka => k
    }
    if (kafkaDestinations.isEmpty) {
      Future.successful(None)
    } else {
      Future
        .sequence(kafkaDestinations.map { k =>
          KafkaSettingsValidator
            .checkBootstrapConnectivity(k.bootstrapServers, KafkaConnectivityTimeout)
            .map(_.map(errors => errors.map(e => s"DLQ Kafka destination: $e")))
        })
        .map(_.foldLeft(Option.empty[NonEmptyList[ErrorString]])(_ |+| _))
    }
  }

  /** Validate Kafka ingest source connectivity.
    * Checks connectivity to bootstrap servers for Kafka ingest sources.
    */
  private def validateIngestSource(
    ingestConfig: V2IngestConfiguration,
  )(implicit ec: ExecutionContext): Future[Option[NonEmptyList[ErrorString]]] =
    ingestConfig.source match {
      case k: KafkaIngest =>
        KafkaSettingsValidator
          .checkBootstrapConnectivity(k.bootstrapServers, KafkaConnectivityTimeout)
          .map(_.map(errors => errors.map(e => s"Kafka ingest source: $e")))
      case _ => Future.successful(None)
    }

  private type ErrSq = BadRequest :+: NotFound :+: CNil
  private def asBadRequest(msg: String): ErrSq = Coproduct[ErrSq](BadRequest(msg))
  private def asBadRequest(br: BadRequest): ErrSq = Coproduct[ErrSq](br)
  private def asNotFound(msg: String): ErrSq = Coproduct[ErrSq](NotFound(msg))

  def addSQOutput(
    name: String,
    outputName: String,
    namespaceId: NamespaceId,
    workflow: ApiStanding.StandingQueryResultWorkflow,
  ): Future[Either[ErrSq, Unit]] =
    graph
      .requiredGraphIsReadyFuture {
        implicit val ec: ExecutionContext = graph.shardDispatcherEC
        validateWorkflow(workflow).flatMap {
          case Some(errors) =>
            Future.successful(Left(asBadRequest(BadRequest.ofErrorStrings(errors.toList))))

          case None =>
            app
              .addStandingQueryOutputV2(name, outputName, namespaceId, workflow)
              .map {
                case StandingQueryInterfaceV2.Result.Success =>
                  Right(())
                case StandingQueryInterfaceV2.Result.AlreadyExists(name) =>
                  Left(asBadRequest(s"There is already a Standing Query output named '$name'"))
                case StandingQueryInterfaceV2.Result.NotFound(queryName) =>
                  Left(asBadRequest(s"No Standing Query named '$queryName' can be found."))
              }
              .recover {
                case e: IllegalArgumentException =>
                  Left(asBadRequest(s"Cannot create output `$outputName`: ${e.getMessage}"))
                case cypherException: CypherException =>
                  Left(asBadRequest(BadRequest(cypherException.pretty, List(ErrorDetail.cypherError))))
                case e: AllNodeScanException =>
                  Left(asBadRequest(s"Cannot create output `$outputName`: ${e.getMessage}"))
              }
        }
      }
      .recoverWith { case _: NamespaceNotFoundException =>
        Future.successful(Left(asNotFound(s"Graph ${namespaceId.name} not found")))
      }(ExecutionContext.parasitic)

  def setSampleQueries(newSampleQueries: Vector[SampleQuery]): Future[Unit] =
    graph.requiredGraphIsReadyFuture(app.setSampleQueries(newSampleQueries.map(ApiToUiStyling.apply)))

  def setQuickQueries(newQuickQueries: Vector[UiNodeQuickQuery]): Future[Unit] =
    graph.requiredGraphIsReadyFuture(app.setQuickQueries(newQuickQueries.map(ApiToUiStyling.apply)))

  def setNodeAppearances(newNodeAppearances: Vector[UiNodeAppearance]): Future[Unit] =
    graph.requiredGraphIsReadyFuture(app.setNodeAppearances(newNodeAppearances.map(ApiToUiStyling.apply)))

  def getTapQueries(namespace: NamespaceId): Future[Vector[TapQuery]] =
    graph.requiredGraphIsReadyFuture(app.getTapQueries(namespace))

  def setTapQueries(namespace: NamespaceId, newTapQueries: Vector[TapQuery]): Future[Unit] =
    graph.requiredGraphIsReadyFuture(app.setTapQueries(namespace, newTapQueries))

  def deleteSQOutput(
    name: String,
    outputName: String,
    namespaceId: NamespaceId,
  ): Future[Either[NotFound, ApiStanding.StandingQueryResultWorkflow]] =
    graph
      .requiredGraphIsReadyFuture {
        implicit val exc = ExecutionContext.parasitic
        app
          .removeStandingQueryOutputV2(name, outputName, namespaceId)
          .map(
            _.toRight(NotFound(s"Standing Query, $name, does not exist")),
          )
      }
      .recoverWith { case _: NamespaceNotFoundException =>
        Future.successful(Left(NotFound(s"Graph ${namespaceId.name} not found")))
      }(ExecutionContext.parasitic)

  private def validateOutputWorkflows(
    workflows: Seq[ApiStanding.StandingQueryResultWorkflow],
  )(implicit ec: ExecutionContext): Future[Option[NonEmptyList[ErrorString]]] =
    Future
      .sequence(workflows.map(validateWorkflow))
      .map(_.foldLeft(Option.empty[NonEmptyList[ErrorString]])(_ |+| _))

  def createSQ(
    name: String,
    namespaceId: NamespaceId,
    shouldCalculateResultHashCode: Boolean = false,
    sq: ApiStanding.StandingQuery.StandingQueryDefinition,
    propagateTo: ApiStanding.PropagateTo = ApiStanding.PropagateTo.ExcludeSleeping,
  ): Future[Either[ErrSq, ApiStanding.StandingQuery.RegisteredStandingQuery]] = {
    implicit val ctx: ExecutionContext = graph.nodeDispatcherEC
    graph
      .requiredGraphIsReadyFuture {
        for {
          validationErrors <- validateOutputWorkflows(sq.outputs)
          result <- validationErrors match {
            case Some(errors) =>
              Future.successful(Left(asBadRequest(BadRequest.ofErrorStrings(errors.toList))))
            case None =>
              app
                .addStandingQueryV2(name, namespaceId, sq)
                .flatMap {
                  case StandingQueryInterfaceV2.Result.AlreadyExists(_) =>
                    Future.successful(Left(asBadRequest(s"There is already a Standing Query named '$name'")))
                  case StandingQueryInterfaceV2.Result.NotFound(_) =>
                    Future.successful(Left(asBadRequest(s"Graph not found: $namespaceId")))
                  case StandingQueryInterfaceV2.Result.Success =>
                    propagateTo match {
                      case ApiStanding.PropagateTo.None => ()
                      case ApiStanding.PropagateTo.ExcludeSleeping =>
                        propagateStandingQuery(includeSleeping = false, namespaceId, wakeUpParallelism = 4)
                      case ApiStanding.PropagateTo.IncludeSleeping(wakeupParallelism) =>
                        propagateStandingQuery(includeSleeping = true, namespaceId, wakeupParallelism)
                    }
                    app.getStandingQueryV2(name, namespaceId).map {
                      case Some(value) => Right(value)
                      case None => sys.error("Standing Query not found after adding, this should not happen.")
                    }
                }
                .recover {
                  case iqp: InvalidQueryPattern => Left(asBadRequest(iqp.message))
                  case cypherException: CypherException =>
                    Left(asBadRequest(BadRequest(cypherException.pretty, List(ErrorDetail.cypherError))))
                }
          }
        } yield result
      }
      .recoverWith { case _: NamespaceNotFoundException =>
        Future.successful(Left(asNotFound(s"Graph, $namespaceId, Not Found")))
      }
  }

  def deleteSQ(
    name: String,
    namespaceId: NamespaceId,
  ): Future[Either[NotFound, ApiStanding.StandingQuery.RegisteredStandingQuery]] =
    app
      .cancelStandingQueryV2(name, namespaceId)
      .map(
        _.toRight(NotFound(s"Standing Query, $name, does not exist")),
      )(ExecutionContext.parasitic)
      .recoverWith { case _: NamespaceNotFoundException =>
        Future.successful(Left(NotFound(s"Graph ${namespaceId.name} not found")))
      }(ExecutionContext.parasitic)

  def getSQ(
    name: String,
    namespaceId: NamespaceId,
  ): Future[Either[NotFound, ApiStanding.StandingQuery.RegisteredStandingQuery]] =
    app
      .getStandingQueryV2(name, namespaceId)
      .map(
        _.toRight(NotFound(s"Standing Query, $name, does not exist")),
      )(ExecutionContext.parasitic)
      .recoverWith { case _: NamespaceNotFoundException =>
        Future.successful(Left(NotFound(s"Graph ${namespaceId.name} not found")))
      }(ExecutionContext.parasitic)

  // --------------------- Ingest Endpoints ------------------------

  protected type ErrC = ServerError :+: BadRequest :+: CNil
  protected type Warnings = Set[String]

  def createIngestStream[Conf](
    ingestStreamName: String,
    ns: NamespaceId,
    ingestStreamConfig: Conf,
    memberIdx: Option[Int],
  )(implicit
    ec: ExecutionContext,
    configOf: ApiToIngest.OfApiMethod[V2IngestConfiguration, Conf],
  ): Future[Either[ErrC, (ApiIngest.IngestStreamInfoWithName, Warnings)]] = {
    val ingestConfig = configOf(ingestStreamConfig)
    // Behind a load balancer a request without an explicit position resolves to whichever
    // member served it; resolve once so the response reports where the ingest actually ran.
    val resolvedMemberIdx = memberIdx.orElse(defaultTargetMemberIdx)

    def asBadRequest(errors: Seq[String]): ErrC =
      Coproduct[ErrC](BadRequest.ofErrorStrings(errors.toList))
    def asServerError(msg: String): ErrC =
      Coproduct[ErrC](ServerError(msg))

    val result = for {
      _ <- EitherT(validateIngestSource(ingestConfig).map {
        case Some(errors) => Left(asBadRequest(errors.toList))
        case None => Right(())
      })
      _ <- EitherT(validateDlqDestinations(ingestConfig).map {
        case Some(errors) => Left(asBadRequest(errors.toList))
        case None => Right(())
      })
      warnings <- EitherT(
        app
          .addV2IngestStream(
            name = ingestStreamName,
            settings = ingestConfig,
            intoNamespace = ns,
            timeout = timeout,
            memberIdx = resolvedMemberIdx,
          )
          .map(_.leftMap(asBadRequest))
          .map(_.map(_ => QuineValueIngestQuery.getQueryWarnings(ingestConfig.query, ingestConfig.parameter))),
      )
      stream <- EitherT.fromOptionF(
        ingestStreamStatus(ingestStreamName, ns, resolvedMemberIdx),
        asServerError("Ingest was not found after creation"),
      )
    } yield (stream, warnings)

    result.value
  }

  def deleteIngestStream(
    ingestName: String,
    namespaceId: NamespaceId,
    memberIdx: Option[Int],
  ): Future[Option[ApiIngest.IngestStreamInfoWithName]] = {
    val resolvedMemberIdx = memberIdx.orElse(defaultTargetMemberIdx)
    app
      .removeV2IngestStream(ingestName, namespaceId, resolvedMemberIdx)
      .map { maybeIngest =>
        maybeIngest.map(IngestToApi.apply(_, resolvedMemberIdx))
      }(ExecutionContext.parasitic)
  }

  def pauseIngestStream(
    ingestName: String,
    namespaceId: NamespaceId,
    memberIdx: Option[Int],
  ): Future[Either[BadRequest, Option[ApiIngest.IngestStreamInfoWithName]]] = {
    val resolvedMemberIdx = memberIdx.orElse(defaultTargetMemberIdx)
    app
      .pauseV2IngestStream(ingestName, namespaceId, resolvedMemberIdx)
      .map {
        case None => Right(None)
        case Some(ingest) =>
          Right(Some(IngestToApi(ingest, resolvedMemberIdx)))
      }(ExecutionContext.parasitic)
      .recover(mkPauseOperationError("pause"))(ExecutionContext.parasitic)
  }

  def unpauseIngestStream(
    ingestName: String,
    namespaceId: NamespaceId,
    memberIdx: Option[Int],
  ): Future[Either[BadRequest, Option[ApiIngest.IngestStreamInfoWithName]]] = {
    val resolvedMemberIdx = memberIdx.orElse(defaultTargetMemberIdx)
    app
      .unpauseV2IngestStream(ingestName, namespaceId, resolvedMemberIdx)
      .map {
        case None => Right(None)
        case Some(ingest) =>
          Right(Some(IngestToApi(ingest, resolvedMemberIdx)))
      }(ExecutionContext.parasitic)
      .recover(mkPauseOperationError("resume"))(ExecutionContext.parasitic)
  }

  def ingestStreamStatus(
    ingestName: String,
    namespaceId: NamespaceId,
    memberIdx: Option[Int],
  ): Future[Option[ApiIngest.IngestStreamInfoWithName]] = {
    val resolvedMemberIdx = memberIdx.orElse(defaultTargetMemberIdx)
    app
      .getV2IngestStream(ingestName, namespaceId, resolvedMemberIdx)
      .map(maybeIngestInfo => maybeIngestInfo.map(IngestToApi.apply(_, resolvedMemberIdx)))(
        graph.nodeDispatcherEC,
      )
  }

  def listIngestStreams(
    namespaceId: NamespaceId,
    memberIdx: Option[MemberIdx],
  ): Future[Either[NotFound, Seq[ApiIngest.IngestStreamInfoWithName]]] =
    if (!graph.getNamespaces.contains(namespaceId))
      Future.successful(Left(NotFound(s"Graph ${namespaceId.name} not found")))
    else
      app
        .getV2IngestStreams(namespaceId, memberIdx)
        .map(streams =>
          // Default order: by ingest name, then member position. Same-named ingests on different
          // members are independent today (there are no managed cluster ingests yet), but ordering
          // by name keeps them adjacent, matching how users tend to reason about them.
          // Deterministic behind a load balancer.
          Right(
            streams
              .sortBy { case (idx, name, _) => (name, idx.getOrElse(Int.MaxValue)) }
              .map { case (idx, name, ingest) => IngestToApi.apply(ingest.withName(name), idx) },
          ): Either[NotFound, Seq[ApiIngest.IngestStreamInfoWithName]],
        )(ExecutionContext.parasitic)

}
