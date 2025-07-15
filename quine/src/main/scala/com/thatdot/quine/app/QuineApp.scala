package com.thatdot.quine.app

import java.time.Instant
import java.time.temporal.ChronoUnit.MILLIS

import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.concurrent.{Await, ExecutionContext, Future, blocking}
import scala.util.{Failure, Success, Try}

import org.apache.pekko.Done
import org.apache.pekko.stream.KillSwitches
import org.apache.pekko.stream.scaladsl.Keep
import org.apache.pekko.util.Timeout

import cats.Applicative
import cats.data.{Validated, ValidatedNel}
import cats.instances.future.catsStdInstancesForFuture
import cats.syntax.all._

import com.thatdot.api.{v2 => Api2}
import com.thatdot.common.logging.Log.{LazySafeLogging, LogConfig, Safe, SafeLoggableInterpolator}
import com.thatdot.cypher.phases.{LexerPhase, LexerState, ParserPhase, SymbolAnalysisPhase}
import com.thatdot.quine.app.model.ingest.serialization.{CypherParseProtobuf, CypherToProtobuf}
import com.thatdot.quine.app.model.ingest.{IngestSrcDef, QuineIngestSource}
import com.thatdot.quine.app.model.ingest2.V2IngestEntities.{QuineIngestConfiguration, QuineIngestStreamWithStatus}
import com.thatdot.quine.app.model.ingest2.{V2IngestEntities, V2IngestEntityEncoderDecoders}
import com.thatdot.quine.app.routes._
import com.thatdot.quine.app.util.QuineLoggables._
import com.thatdot.quine.app.v2api.converters.ApiToStanding
import com.thatdot.quine.app.v2api.definitions.query.{standing => V2ApiStanding}
import com.thatdot.quine.compiler.cypher
import com.thatdot.quine.compiler.cypher.{CypherStandingWiretap, registerUserDefinedProcedure}
import com.thatdot.quine.graph.InvalidQueryPattern._
import com.thatdot.quine.graph.MasterStream.SqResultsExecToken
import com.thatdot.quine.graph.StandingQueryPattern.{
  DomainGraphNodeStandingQueryPattern,
  MultipleValuesQueryPattern,
  QuinePatternQueryPattern,
}
import com.thatdot.quine.graph.cypher.quinepattern.LazyQuinePatternQueryPlanner
import com.thatdot.quine.graph.metrics.HostQuineMetrics
import com.thatdot.quine.graph.{
  GraphService,
  MemberIdx,
  NamespaceId,
  PatternOrigin,
  StandingQueryId,
  StandingQueryInfo,
  defaultNamespaceId,
  namespaceFromString,
  namespaceToString,
}
import com.thatdot.quine.model.QuineIdProvider
import com.thatdot.quine.persistor.{PrimePersistor, Version}
import com.thatdot.quine.serialization.{AvroSchemaCache, EncoderDecoder, ProtobufSchemaCache}
import com.thatdot.quine.util.Log.implicits._
import com.thatdot.quine.util.{BaseError, SwitchMode}
import com.thatdot.quine.{routes => V1}

/** The Quine application state
  *
  * @param graph reference to the underlying graph
  */
final class QuineApp(
  graph: GraphService,
  helpMakeQuineBetter: Boolean,
  recipe: Option[Recipe] = None,
  recipeCanonicalName: Option[String] = None,
)(implicit val logConfig: LogConfig)
    extends BaseApp(graph)
    with AdministrationRoutesState
    with QueryUiConfigurationState
    with StandingQueryStoreV1
    with StandingQueryInterfaceV2
    with IngestStreamState
    with V1.QueryUiConfigurationSchemas
    with V1.StandingQuerySchemas
    with V1.IngestSchemas
    with EncoderDecoder.DeriveEndpoints4s
    with com.thatdot.quine.routes.exts.CirceJsonAnySchema
    with SchemaCache
    with LazySafeLogging {

  import QuineApp._
  import com.thatdot.quine.app.model.ingest2.V2IngestEntityEncoderDecoders.implicits._
  import com.thatdot.quine.app.StandingQueryResultOutput.OutputTarget

  implicit private[this] val idProvider: QuineIdProvider = graph.idProvider

  /** == Local state ==
    * Notes on synchronization:
    * Accesses to the following collections must be threadsafe. Additionally, the persisted copy of these collections
    * (ie those accessed by `*Metadata` functions) must be kept in sync with the in-memory copy. Because all of these
    * functions are expected to have a low volume of usage, and thus don't need to be performance-optimized, we
    * aggressively synchronize on locks. In particular, synchronizing on the collection itself is not sufficient, because
    * the lock offered by `synchronize` is with respect to the locked *value*, not the locked *field* -- so locking on
    * a mutating variable does not result in a mutex. By contrast, locking on a lock is more than is strictly necessary,
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
    * - `setQuickQueries` is wrapped in a `...Lock.synchronized` to ensure that 2 simultaneous calls to `setQuickQueries`
    * will not interleave their local and remote update steps. Without synchronized, execution (1) might set the local
    * variable while execution (2) sets the persisted version
    *
    * In the case of collections with update (eg `add`/`remove`) semantics, all accesses must be synchronized
    * with a lock on `this`, because all accesses involve both a read and a write which might race concurrent executions.
    *
    * Add example:
    * - `addIngestStream` is wrapped in a `...Lock.synchronized` because the updates it makes to `ingestStreams` depend on
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
  private[this] var sampleQueries: Vector[V1.SampleQuery] = Vector.empty
  // Locks are on the object; we can't use a var (e.g. the collection) as something to synchronize on
  // as it's always being updated to point to a new object.
  final private[this] val sampleQueriesLock = new AnyRef
  @volatile
  private[this] var quickQueries: Vector[V1.UiNodeQuickQuery] = Vector.empty
  final private[this] val quickQueriesLock = new AnyRef
  @volatile
  private[this] var nodeAppearances: Vector[V1.UiNodeAppearance] = Vector.empty
  final private[this] val nodeAppearancesLock = new AnyRef

  @volatile
  private[this] var outputTargets: NamespaceOutputTargets = Map(defaultNamespaceId -> Map.empty)
  final private[this] val outputTargetsLock = new AnyRef

  final private[this] val ingestStreamsLock = new AnyRef

  // Constant member index 0 for Quine
  val thisMemberIdx: MemberIdx = 0

  /** == Accessors == */

  def getSampleQueries: Future[Vector[V1.SampleQuery]] = Future.successful(sampleQueries)

  def getQuickQueries: Future[Vector[V1.UiNodeQuickQuery]] = Future.successful(quickQueries)

  def getNodeAppearances: Future[Vector[V1.UiNodeAppearance]] = Future.successful(nodeAppearances)

  def setSampleQueries(newSampleQueries: Vector[V1.SampleQuery]): Future[Unit] =
    synchronizedFakeFuture(sampleQueriesLock) {
      sampleQueries = newSampleQueries
      storeGlobalMetaData(SampleQueriesKey, sampleQueries)
    }

  def setQuickQueries(newQuickQueries: Vector[V1.UiNodeQuickQuery]): Future[Unit] =
    synchronizedFakeFuture(quickQueriesLock) {
      quickQueries = newQuickQueries
      storeGlobalMetaData(QuickQueriesKey, quickQueries)
    }

  def setNodeAppearances(newNodeAppearances: Vector[V1.UiNodeAppearance]): Future[Unit] =
    synchronizedFakeFuture(nodeAppearancesLock) {
      nodeAppearances = newNodeAppearances.map(QueryUiConfigurationState.renderNodeIcons)
      storeGlobalMetaData(NodeAppearancesKey, nodeAppearances)
    }

  def addStandingQueryV2(
    queryName: String,
    inNamespace: NamespaceId,
    standingQueryDefinition: V2ApiStanding.StandingQuery.StandingQueryDefinition,
  ): Future[StandingQueryInterfaceV2.Result] = onlyIfNamespaceExists(inNamespace) {
    synchronizedFakeFuture(outputTargetsLock) {
      outputTargets
        .get(inNamespace)
        .fold(
          Future.successful[StandingQueryInterfaceV2.Result](
            StandingQueryInterfaceV2.Result.NotFound(namespaceToString(inNamespace)),
          ),
        ) { sqOutputTargets =>
          if (sqOutputTargets.contains(queryName)) {
            Future.successful(
              StandingQueryInterfaceV2.Result.AlreadyExists(queryName),
            )
          } else {
            val sqId = StandingQueryId.fresh()
            implicit val ec: ExecutionContext = graph.nodeDispatcherEC
            Future
              .traverse(standingQueryDefinition.outputs.toVector) { case (outputName, apiWorkflow) =>
                ApiToStanding(apiWorkflow, outputName, inNamespace)(graph, protobufSchemaCache).map(
                  workflowInterpreter =>
                    outputName -> workflowInterpreter
                      .flow(graph)
                      .viaMat(KillSwitches.single)(Keep.right)
                      .map(_ => SqResultsExecToken(s"SQ: $outputName in: $inNamespace"))
                      .to(graph.masterStream.standingOutputsCompletionSink),
                )
              }
              .map(_.toMap)
              .flatMap { sqResultsConsumers =>
                val (pattern, dgnPackage) = standingQueryDefinition.pattern match {
                  case V2ApiStanding.StandingQueryPattern.Cypher(cypherQuery, mode) =>
                    val pattern = cypher.compileStandingQueryGraphPattern(cypherQuery)(graph.idProvider, logConfig)
                    val origin = PatternOrigin.GraphPattern(pattern, Some(cypherQuery))

                    mode match {
                      case V2ApiStanding.StandingQueryPattern.StandingQueryMode.DistinctId =>
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
                          standingQueryDefinition.includeCancellations,
                          origin,
                        )
                        (dgnPattern, Some(dgnPackage))
                      case V2ApiStanding.StandingQueryPattern.StandingQueryMode.MultipleValues =>
                        if (pattern.distinct) throw MultipleValuesCantDistinct
                        val compiledQuery =
                          pattern.compiledMultipleValuesStandingQuery(graph.labelsProperty, idProvider)
                        val sqv4Pattern =
                          MultipleValuesQueryPattern(
                            compiledQuery,
                            standingQueryDefinition.includeCancellations,
                            origin,
                          )
                        (sqv4Pattern, None)
                      case V2ApiStanding.StandingQueryPattern.StandingQueryMode.QuinePattern =>
                        val maybeIsQPEnabled = for {
                          pv <- Option(System.getProperty("qp.enabled"))
                          b <- pv.toBooleanOption
                        } yield b

                        maybeIsQPEnabled match {
                          case Some(true) =>
                            import com.thatdot.language.phases.UpgradeModule._

                            val parser = LexerPhase andThen ParserPhase andThen SymbolAnalysisPhase
                            val (state, result) = parser.process(cypherQuery).value.run(LexerState(Nil)).value
                            val queryPlan = LazyQuinePatternQueryPlanner.planQuery(result.get, state.symbolTable)

                            val qpPattern = QuinePatternQueryPattern(queryPlan)
                            (qpPattern, None)
                          case _ =>
                            sys.error("Quine pattern must be enabled using -Dqp.enabled=true to use this feature.")
                        }
                    }
                }
                (dgnPackage match {
                  case Some(p) =>
                    graph.dgnRegistry.registerAndPersistDomainGraphNodePackage(p, sqId, skipPersistor = false)
                  case None => Future.unit
                }).flatMap { _ =>
                  graph
                    .standingQueries(inNamespace)
                    .fold(
                      Future
                        .successful[StandingQueryInterfaceV2.Result](
                          StandingQueryInterfaceV2.Result.NotFound(queryName),
                        ),
                    ) { sqns => // Ignore if namespace is no longer available.
                      val (sq, killSwitches) = sqns.createStandingQuery(
                        name = queryName,
                        pattern = pattern,
                        outputs = sqResultsConsumers,
                        queueBackpressureThreshold = standingQueryDefinition.inputBufferSize,
                        sqId = sqId,
                      )
                      val outputsWithKillSwitches = standingQueryDefinition.outputs.map { case (name, workflow) =>
                        name -> OutputTarget.V2(workflow, killSwitches(name))
                      }
                      val updatedInnerMap = sqOutputTargets + (queryName -> (sq.query.id -> outputsWithKillSwitches))
                      outputTargets += inNamespace -> updatedInnerMap
                      storeStandingQueryOutputs2().map(_ => StandingQueryInterfaceV2.Result.Success)(
                        ExecutionContext.parasitic,
                      )
                    }
                }(graph.system.dispatcher)
              }
          }
        }
    }
  }

  def addStandingQuery(
    queryName: FriendlySQName,
    inNamespace: NamespaceId,
    query: V1.StandingQueryDefinition,
  ): Future[Boolean] = onlyIfNamespaceExists(inNamespace) {
    synchronizedFakeFuture(outputTargetsLock) {
      outputTargets.get(inNamespace).fold(Future.successful(false)) { namespaceTargets =>
        if (namespaceTargets.contains(queryName)) Future.successful(false)
        else {
          val sqId = StandingQueryId.fresh()
          val sqResultsConsumers = query.outputs.map { case (outputName, outputDefinition) =>
            outputName -> StandingQueryResultOutput
              .resultHandlingSink(outputName, inNamespace, outputDefinition, graph)(protobufSchemaCache, logConfig)
          }
          val (pattern, dgnPackage) = query.pattern match {
            case V1.StandingQueryPattern.Cypher(cypherQuery, mode) =>
              val pattern = cypher.compileStandingQueryGraphPattern(cypherQuery)(graph.idProvider, logConfig)
              val origin = PatternOrigin.GraphPattern(pattern, Some(cypherQuery))

              mode match {
                case V1.StandingQueryPattern.StandingQueryMode.DistinctId =>
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
                    origin,
                  )
                  (dgnPattern, Some(dgnPackage))
                case V1.StandingQueryPattern.StandingQueryMode.MultipleValues =>
                  if (pattern.distinct) throw MultipleValuesCantDistinct
                  val compiledQuery = pattern.compiledMultipleValuesStandingQuery(graph.labelsProperty, idProvider)
                  val sqv4Pattern = MultipleValuesQueryPattern(compiledQuery, query.includeCancellations, origin)
                  (sqv4Pattern, None)
                case V1.StandingQueryPattern.StandingQueryMode.QuinePattern =>
                  val maybeIsQPEnabled = for {
                    pv <- Option(System.getProperty("qp.enabled"))
                    b <- pv.toBooleanOption
                  } yield b

                  maybeIsQPEnabled match {
                    case Some(true) =>
                      import com.thatdot.language.phases.UpgradeModule._

                      val parser = LexerPhase andThen ParserPhase andThen SymbolAnalysisPhase
                      val (state, result) = parser.process(cypherQuery).value.run(LexerState(Nil)).value
                      val queryPlan = LazyQuinePatternQueryPlanner.planQuery(result.get, state.symbolTable)

                      val qpPattern = QuinePatternQueryPattern(queryPlan)
                      (qpPattern, None)
                    case _ => sys.error("Quine pattern must be enabled using -Dqp.enabled=true to use this feature.")
                  }
              }
          }
          (dgnPackage match {
            case Some(p) => graph.dgnRegistry.registerAndPersistDomainGraphNodePackage(p, sqId, skipPersistor = false)
            case None => Future.unit
          }).flatMap { _ =>
            graph
              .standingQueries(inNamespace)
              .fold(Future.successful(false)) { sqns => // Ignore if namespace is no longer available.
                val (sq, killSwitches) = sqns.createStandingQuery(
                  queryName,
                  pattern,
                  outputs = sqResultsConsumers,
                  queueBackpressureThreshold = query.inputBufferSize,
                  shouldCalculateResultHashCode = query.shouldCalculateResultHashCode,
                  sqId = sqId,
                )
                val outputsWithKillSwitches = query.outputs.map { case (name, out) =>
                  name -> OutputTarget.V1(out, killSwitches(name))
                }
                val updatedInnerMap = namespaceTargets + (queryName -> (sq.query.id -> outputsWithKillSwitches))
                outputTargets += inNamespace -> updatedInnerMap
                storeStandingQueryOutputs1().map(_ => true)(ExecutionContext.parasitic)
              }
          }(graph.system.dispatcher)
        }
      }
    }
  }

  def cancelStandingQueryV2(
    queryName: String,
    inNamespace: NamespaceId,
  ): Future[Option[V2ApiStanding.StandingQuery.RegisteredStandingQuery]] = onlyIfNamespaceExists(inNamespace) {
    synchronizedFakeFuture(outputTargetsLock) {
      val cancelledSqState = for {
        (sqId, outputs: Map[SQOutputName, OutputTarget]) <- outputTargets.get(inNamespace).flatMap(_.get(queryName))
        v2Outputs = outputs.collect { case (name, target: OutputTarget.V2) => name -> target.definition }
        cancelledSq <- graph.standingQueries(inNamespace).flatMap(_.cancelStandingQuery(sqId))
      } yield {
        // Remove key from the inner map:
        outputTargets += inNamespace -> (outputTargets(inNamespace) - queryName)

        // Map to return type
        cancelledSq.map { case (internalSq, startTime, bufferSize) =>
          makeRegisteredStandingQueryV2(
            internal = internalSq,
            inNamespace = inNamespace,
            outputs = v2Outputs,
            startTime = startTime,
            bufferSize = bufferSize,
            metrics = graph.metrics,
          )
        }(graph.system.dispatcher)
      }
      // must be implicit for cats sequence
      implicit val applicative: Applicative[Future] = catsStdInstancesForFuture(ExecutionContext.parasitic)
      cancelledSqState.sequence productL storeStandingQueryOutputs()
    }
  }

  /** Cancels an existing standing query.
    *
    * @return Future succeeds/fails when the storing of the updated collection of SQs succeeds/fails. The Option is
    *         `None` when the SQ or namespace doesn't exist. The inner `V1.RegisteredStandingQuery` is the definition of the
    *         successfully removed standing query.
    */
  def cancelStandingQuery(
    queryName: String,
    inNamespace: NamespaceId,
  ): Future[Option[V1.RegisteredStandingQuery]] = onlyIfNamespaceExists(inNamespace) {
    synchronizedFakeFuture(outputTargetsLock) {
      val cancelledSqState: Option[Future[V1.RegisteredStandingQuery]] = for {
        (sqId, outputs) <- outputTargets.get(inNamespace).flatMap(_.get(queryName))
        v1Outputs = outputs.collect { case (name, target: OutputTarget.V1) => name -> target.definition }
        cancelledSq <- graph.standingQueries(inNamespace).flatMap(_.cancelStandingQuery(sqId))
      } yield {
        // Remove key from the inner map:
        outputTargets += inNamespace -> (outputTargets(inNamespace) - queryName)

        // Map to return type
        cancelledSq.map { case (internalSq, startTime, bufferSize) =>
          makeRegisteredStandingQuery(
            internal = internalSq,
            inNamespace = inNamespace,
            outputs = v1Outputs,
            startTime = startTime,
            bufferSize = bufferSize,
            metrics = graph.metrics,
          )
        }(graph.system.dispatcher)
      }
      // must be implicit for cats sequence
      implicit val applicative: Applicative[Future] = catsStdInstancesForFuture(ExecutionContext.parasitic)
      cancelledSqState.sequence productL storeStandingQueryOutputs()
    }
  }

  private def getSources: Future[Option[List[String]]] =
    Future.successful(Some(ImproveQuine.sourcesFromIngestStreams(getIngestStreams(defaultNamespaceId))))

  private def getSinks: Future[Option[List[String]]] =
    getStandingQueries(defaultNamespaceId)
      .map(ImproveQuine.sinksFromStandingQueries)(ExecutionContext.parasitic)
      .map(Some(_))(ExecutionContext.parasitic)

  /** Adds a new user-defined output handler to an existing standing query.
    *
    * @return Future succeeds/fails when the storing of SQs succeeds/fails. The Option is None when the SQ or
    *         namespace doesn't exist. The Boolean indicates whether an output with that name was successfully added (false if
    *         the out name is already in use).
    */
  def addStandingQueryOutputV2(
    queryName: String,
    outputName: String,
    inNamespace: NamespaceId,
    workflow: V2ApiStanding.StandingQueryResultWorkflow,
  ): Future[StandingQueryInterfaceV2.Result] = onlyIfNamespaceExists(inNamespace) {
    synchronizedFakeFuture(outputTargetsLock) {
      val optionFut = for {
        (sqId, outputs) <- outputTargets.get(inNamespace).flatMap(_.get(queryName))
        sqResultsHub <- graph.standingQueries(inNamespace).flatMap(_.standingResultsHub(sqId))
      } yield
        if (outputs.contains(outputName)) {
          Future.successful(StandingQueryInterfaceV2.Result.AlreadyExists(outputName))
        } else {
          ApiToStanding(workflow, outputName, inNamespace)(graph, protobufSchemaCache).flatMap { workflowInterpreter =>
            val killSwitch =
              sqResultsHub
                .viaMat(KillSwitches.single)(Keep.right)
                .via(workflowInterpreter.flow(graph)(logConfig))
                .map(_ => SqResultsExecToken(s"SQ: $outputName in: $inNamespace"))
                .to(graph.masterStream.standingOutputsCompletionSink)
                .run()(graph.materializer)

            val updatedInnerMap = outputTargets(inNamespace) +
              (queryName -> (sqId -> (outputs + (outputName -> OutputTarget.V2(workflow, killSwitch)))))
            outputTargets += inNamespace -> updatedInnerMap
            storeStandingQueryOutputs2().map(_ => StandingQueryInterfaceV2.Result.Success)(ExecutionContext.parasitic)
          }(graph.nodeDispatcherEC)
        }
      optionFut.getOrElse(Future.successful(StandingQueryInterfaceV2.Result.NotFound(queryName)))
    }
  }

  /** Adds a new user-defined output handler to an existing standing query.
    *
    * @return Future succeeds/fails when the storing of SQs succeeds/fails. The Option is None when the SQ or
    *         namespace doesn't exist. The Boolean indicates whether an output with that name was successfully added (false if
    *         the out name is already in use).
    */
  def addStandingQueryOutput(
    queryName: String,
    outputName: String,
    inNamespace: NamespaceId,
    sqResultOutput: V1.StandingQueryResultOutputUserDef,
  ): Future[Option[Boolean]] = onlyIfNamespaceExists(inNamespace) {
    synchronizedFakeFuture(outputTargetsLock) {
      val optionFut = for {
        (sqId, outputs) <- outputTargets.get(inNamespace).flatMap(_.get(queryName))
        sqResultsHub <- graph.standingQueries(inNamespace).flatMap(_.standingResultsHub(sqId))
      } yield
        if (outputs.contains(outputName)) {
          Future.successful(false)
        } else {
          // Materialize the new output stream
          val killSwitch = sqResultsHub.runWith(
            StandingQueryResultOutput.resultHandlingSink(outputName, inNamespace, sqResultOutput, graph)(
              protobufSchemaCache,
              logConfig,
            ),
          )(graph.materializer)
          val updatedInnerMap = outputTargets(inNamespace) +
            (queryName -> (sqId -> (outputs + (outputName -> OutputTarget.V1(sqResultOutput, killSwitch)))))
          outputTargets += inNamespace -> updatedInnerMap
          storeStandingQueryOutputs1().map(_ => true)(ExecutionContext.parasitic)
        }
      // must be implicit for cats sequence
      implicit val futureApplicative: Applicative[Future] = catsStdInstancesForFuture(ExecutionContext.parasitic)
      optionFut.sequence
    }
  }

  def removeStandingQueryOutputV2(
    queryName: String,
    outputName: String,
    inNamespace: NamespaceId,
  ): Future[Option[V2ApiStanding.StandingQueryResultWorkflow]] = onlyIfNamespaceExists(inNamespace) {
    synchronizedFakeFuture(outputTargetsLock) {
      val outputOpt = for {
        (sqId, outputs) <- outputTargets.get(inNamespace).flatMap(_.get(queryName))
        OutputTarget.V2(output, killSwitch) <- outputs.get(outputName)
      } yield {
        killSwitch.shutdown()
        val updatedInnerMap = outputTargets(inNamespace) + (queryName -> (sqId -> (outputs - outputName)))
        outputTargets += inNamespace -> updatedInnerMap
        output
      }
      storeStandingQueryOutputs2().map(_ => outputOpt)(ExecutionContext.parasitic)
      Future.successful(outputOpt)
    }
  }

  /** Removes a standing query output handler by name from an existing standing query.
    *
    * @return Future succeeds/fails when the storing of SQs succeeds/fails. The Option is None when the SQ or
    *         namespace doesn't exist, or if the SQ does not have an output with that name. The inner
    *         `V1.StandingQueryResultOutputUserDef` is the output that was successfully removes.
    */
  def removeStandingQueryOutput(
    queryName: String,
    outputName: String,
    inNamespace: NamespaceId,
  ): Future[Option[V1.StandingQueryResultOutputUserDef]] = onlyIfNamespaceExists(inNamespace) {
    synchronizedFakeFuture(outputTargetsLock) {
      val outputOpt = for {
        (sqId, outputs) <- outputTargets.get(inNamespace).flatMap(_.get(queryName))
        OutputTarget.V1(output, killSwitch) <- outputs.get(outputName)
      } yield {
        killSwitch.shutdown()
        val updatedInnerMap = outputTargets(inNamespace) + (queryName -> (sqId -> (outputs - outputName)))
        outputTargets += inNamespace -> updatedInnerMap
        output
      }
      storeStandingQueryOutputs1().map(_ => outputOpt)(ExecutionContext.parasitic)
    }
  }

  def getStandingQueriesV2(
    inNamespace: NamespaceId,
  ): Future[List[V2ApiStanding.StandingQuery.RegisteredStandingQuery]] =
    getStandingQueriesWithNames2(Nil, inNamespace)

  def getStandingQueryV2(
    queryName: String,
    inNamespace: NamespaceId,
  ): Future[Option[V2ApiStanding.StandingQuery.RegisteredStandingQuery]] =
    getStandingQueriesWithNames2(List(queryName), inNamespace).map(_.headOption)(graph.system.dispatcher)

  /** Get standing queries live on the graph with the specified names
    *
    * @param queryNames which standing queries to retrieve, empty list corresponds to all SQs
    * @return queries registered on the graph. Future never fails. List contains each live `V1.RegisteredStandingQuery`.
    */
  private def getStandingQueriesWithNames2(
    queryNames: List[String],
    inNamespace: NamespaceId,
  ): Future[List[V2ApiStanding.StandingQuery.RegisteredStandingQuery]] = onlyIfNamespaceExists(inNamespace) {
    synchronizedFakeFuture(outputTargetsLock) {
      val matchingInfo = for {
        queryName <- queryNames match {
          case Nil => outputTargets.get(inNamespace).map(_.keys).getOrElse(Iterable.empty)
          case names => names
        }
        (sqId, outputs) <- outputTargets
          .get(inNamespace)
          .flatMap(_.get(queryName).map { case (sqId, outputs) =>
            (
              sqId,
              outputs.collect { case (name, out: OutputTarget.V2) =>
                (name, out)
              },
            )
          })
        (internalSq, startTime, bufferSize) <- graph
          .standingQueries(inNamespace)
          .flatMap(_.listStandingQueries.get(sqId))
      } yield makeRegisteredStandingQueryV2(
        internal = internalSq,
        inNamespace = inNamespace,
        outputs = outputs.fmap(_.definition),
        startTime = startTime,
        bufferSize = bufferSize,
        metrics = graph.metrics,
      )
      Future.successful(matchingInfo.toList)
    }
  }

  def getStandingQueries(inNamespace: NamespaceId): Future[List[V1.RegisteredStandingQuery]] =
    onlyIfNamespaceExists(inNamespace) {
      getStandingQueriesWithNames(Nil, inNamespace)
    }

  def getStandingQuery(queryName: String, inNamespace: NamespaceId): Future[Option[V1.RegisteredStandingQuery]] =
    onlyIfNamespaceExists(inNamespace) {
      getStandingQueriesWithNames(List(queryName), inNamespace).map(_.headOption)(graph.system.dispatcher)
    }

  /** Get standing queries live on the graph with the specified names
    *
    * @param queryNames which standing queries to retrieve, empty list corresponds to all SQs
    * @return queries registered on the graph. Future never fails. List contains each live `V1.RegisteredStandingQuery`.
    */
  private def getStandingQueriesWithNames(
    queryNames: List[String],
    inNamespace: NamespaceId,
  ): Future[List[V1.RegisteredStandingQuery]] = onlyIfNamespaceExists(inNamespace) {
    synchronizedFakeFuture(outputTargetsLock) {
      val matchingInfo = for {
        queryName <- queryNames match {
          case Nil => outputTargets.get(inNamespace).map(_.keys).getOrElse(Iterable.empty)
          case names => names
        }
        (sqId, outputs) <- outputTargets.get(inNamespace).flatMap(_.get(queryName))
        v1Outputs = outputs.collect { case (name, target: OutputTarget.V1) => name -> target.definition }
        (internalSq, startTime, bufferSize) <- graph
          .standingQueries(inNamespace)
          .flatMap(_.listStandingQueries.get(sqId))
      } yield makeRegisteredStandingQuery(
        internalSq,
        inNamespace,
        v1Outputs,
        startTime,
        bufferSize,
        graph.metrics,
      )
      Future.successful(matchingInfo.toList)
    }
  }

  def getStandingQueryIdV2(queryName: String, inNamespace: NamespaceId): Option[StandingQueryId] =
    noneIfNoNamespace(inNamespace) {
      outputTargets.get(inNamespace).flatMap(_.get(queryName)).map(_._1)
    }

  def getStandingQueryId(queryName: String, inNamespace: NamespaceId): Option[StandingQueryId] =
    noneIfNoNamespace(inNamespace) {
      outputTargets.get(inNamespace).flatMap(_.get(queryName)).map(_._1)
    }

  def registerTerminationHooks(name: String, metrics: IngestMetrics)(ec: ExecutionContext): Future[Done] => Unit = {
    termSignal =>
      termSignal.onComplete {
        case Failure(err) =>
          val now = Instant.now
          metrics.stop(now)
          logger.error(
            log"Ingest stream '${Safe(name)}' has failed after ${Safe(metrics.millisSinceStart(now))}ms" withException err,
          )
        case Success(_) =>
          val now = Instant.now
          metrics.stop(now)
          logger.info(
            safe"Ingest stream '${Safe(name)}' successfully completed after ${Safe(metrics.millisSinceStart(now))}ms",
          )
      }(ec)
  }

  val protobufSchemaCache: ProtobufSchemaCache = new ProtobufSchemaCache.AsyncLoading(graph.dispatchers)
  val avroSchemaCache: AvroSchemaCache = new AvroSchemaCache.AsyncLoading(graph.dispatchers)

  def addIngestStream(
    name: String,
    settings: V1.IngestStreamConfiguration,
    intoNamespace: NamespaceId,
    previousStatus: Option[V1.IngestStreamStatus], // previousStatus is None if stream was not restored at all
    shouldResumeRestoredIngests: Boolean,
    timeout: Timeout,
    shouldSaveMetadata: Boolean = true,
    memberIdx: Option[MemberIdx] = Some(thisMemberIdx),
  ): Try[Boolean] = failIfNoNamespace(intoNamespace) {

    val isQPEnabled = sys.props.get("qp.enabled").flatMap(_.toBooleanOption) getOrElse false

    settings match {
      case fileIngest: V1.FileIngest =>
        fileIngest.format match {
          case _: V1.FileIngestFormat.QuinePatternLine =>
            if (!isQPEnabled) {
              sys.error("To use this experimental feature, you must set the `qp.enabled` property to `true`.")
            }
          case _: V1.FileIngestFormat.QuinePatternJson =>
            if (!isQPEnabled) {
              sys.error("To use this experimental feature, you must set the `qp.enabled` property to `true`.")
            }
          case _ => logger.trace(safe"Not using QuinePattern")
        }
      case _ => logger.trace(safe"Not using QuinePattern")
    }

    blocking(ingestStreamsLock.synchronized {
      ingestStreams.get(intoNamespace) match {
        case None => Success(false)
        case Some(ingests) if ingests.contains(name) => Success(false)
        case Some(ingests) =>
          val (initialValveSwitchMode, initialStatus) = previousStatus match {
            case None =>
              // This is a freshly-created ingest, so there is no status to restore
              SwitchMode.Open -> V1.IngestStreamStatus.Running
            case Some(lastKnownStatus) =>
              val newStatus = V1.IngestStreamStatus.decideRestoredStatus(lastKnownStatus, shouldResumeRestoredIngests)
              val switchMode = newStatus.position match {
                case V1.ValvePosition.Open => SwitchMode.Open
                case V1.ValvePosition.Closed => SwitchMode.Close
              }
              switchMode -> newStatus
          }

          val src: ValidatedNel[IngestName, QuineIngestSource] =
            IngestSrcDef
              .createIngestSrcDef(
                name,
                intoNamespace,
                settings,
                initialValveSwitchMode,
              )(graph, protobufSchemaCache, logConfig)

          src
            .leftMap(errs => V1.IngestStreamConfiguration.InvalidStreamConfiguration(errs))
            .map { ingestSrcDef =>

              val metrics = IngestMetrics(Instant.now, None, ingestSrcDef.meter)
              val ingestSrc = ingestSrcDef.stream(
                intoNamespace,
                registerTerminationHooks = registerTerminationHooks(name, metrics)(graph.nodeDispatcherEC),
              )

              val streamDefWithControl: IngestStreamWithControl[UnifiedIngestConfiguration] = IngestStreamWithControl(
                UnifiedIngestConfiguration(Right(settings)),
                metrics,
                () => ingestSrcDef.getControl.map(_.valveHandle)(ExecutionContext.parasitic),
                () => ingestSrcDef.getControl.map(_.termSignal)(ExecutionContext.parasitic),
                close = () => {
                  ingestSrcDef.getControl.flatMap(c => c.terminate())(ExecutionContext.parasitic)
                  () // Intentional fire and forget
                },
                initialStatus,
              )

              val newNamespaceIngests = ingests + (name -> streamDefWithControl)
              ingestStreams += intoNamespace -> newNamespaceIngests

              ingestSrc.runWith(graph.masterStream.ingestCompletionsSink)(graph.materializer)

              if (shouldSaveMetadata)
                Await.result(
                  syncIngestStreamsMetaData(thisMemberIdx),
                  timeout.duration,
                )

              true
            }
            .toEither
            .toTry
      }
    })
  }

  def addV2IngestStream(
    name: String,
    settings: QuineIngestConfiguration,
    intoNamespace: NamespaceId,
    previousStatus: Option[V1.IngestStreamStatus], // previousStatus is None if stream was not restored at all
    shouldResumeRestoredIngests: Boolean,
    timeout: Timeout,
    shouldSaveMetadata: Boolean = true,
    memberIdx: Option[MemberIdx] = Some(thisMemberIdx),
  )(implicit logConfig: LogConfig): ValidatedNel[BaseError, Boolean] =
    invalidIfNoNamespace(intoNamespace) {

      blocking(ingestStreamsLock.synchronized {

        val meter = IngestMetered.ingestMeter(intoNamespace, name, graph.metrics)
        val metrics = IngestMetrics(Instant.now, None, meter)

        val validatedSrc = createV2IngestSource(
          name,
          settings,
          intoNamespace,
          previousStatus,
          shouldResumeRestoredIngests,
          metrics,
          meter,
          graph,
        )(protobufSchemaCache, avroSchemaCache, logConfig)

        validatedSrc.map { quineIngestSrc =>
          val streamSource = quineIngestSrc.stream(
            intoNamespace,
            registerTerminationHooks(name, metrics)(graph.nodeDispatcherEC),
          )
          ingestStreams.get(intoNamespace) foreach { ingests =>
            val initialStatus = previousStatus.fold[V1.IngestStreamStatus] {
              V1.IngestStreamStatus.Running
            } { lastKnownStatus =>
              V1.IngestStreamStatus.decideRestoredStatus(lastKnownStatus, shouldResumeRestoredIngests)
            }
            val streamDefWithControl: IngestStreamWithControl[UnifiedIngestConfiguration] = IngestStreamWithControl(
              UnifiedIngestConfiguration(Left(settings)),
              metrics,
              () => quineIngestSrc.getControl.map(_.valveHandle)(ExecutionContext.parasitic),
              () => quineIngestSrc.getControl.map(_.termSignal)(ExecutionContext.parasitic),
              close = () => {
                quineIngestSrc.getControl.flatMap(c => c.terminate())(ExecutionContext.parasitic)
                () // Intentional fire and forget
              },
              initialStatus,
            )
            val newNamespaceIngests = ingests + (name -> streamDefWithControl)
            ingestStreams += intoNamespace -> newNamespaceIngests
          }
          streamSource.runWith(graph.masterStream.ingestCompletionsSink)(graph.materializer)

          if (shouldSaveMetadata)
            Await.result(
              syncIngestStreamsMetaData(thisMemberIdx),
              timeout.duration,
            )

          true
        }
      })

    }

  def getIngestStreams(namespace: NamespaceId): Map[String, IngestStreamWithControl[V1.IngestStreamConfiguration]] =
    if (getNamespaces.contains(namespace))
      getIngestStreamsFromState(namespace).view
        .mapValues(isc => isc.copy(settings = isc.settings.asV1Config))
        .toMap
    else Map.empty

  def getV2IngestStreams(namespace: NamespaceId): Map[String, IngestStreamWithControl[V2IngestEntities.IngestSource]] =
    if (getNamespaces.contains(namespace))
      getIngestStreamsFromState(namespace).view
        .mapValues(isc => isc.copy(settings = V2IngestEntities.IngestSource(isc.settings)))
        .toMap
    else Map.empty

  protected def getIngestStreamsWithStatus(
    namespace: NamespaceId,
  ): Future[Map[IngestName, Either[V1.IngestStreamWithStatus, QuineIngestStreamWithStatus]]] =
    onlyIfNamespaceExists(namespace) {
      implicit val ec: ExecutionContext = graph.nodeDispatcherEC
      getIngestStreamsFromState(namespace).toList
        .traverse { case (name, isc) =>
          for {
            status <- isc.status(graph.materializer)
          } yield (
            name, {
              isc.settings.config match {
                case Left(v2Settings) => Right(QuineIngestStreamWithStatus(v2Settings, Some(status)))
                case Right(v1Settings) => Left(V1.IngestStreamWithStatus(v1Settings, Some(status)))
              }
            },
          )
        }
        .map(_.toMap)(ExecutionContext.parasitic)
    }

  private def syncIngestStreamsMetaData(thisMemberId: Int): Future[Unit] = {
    implicit val ec: ExecutionContext = graph.nodeDispatcherEC
    Future
      .sequence(
        getNamespaces.map(namespace =>
          for {
            streamsWithStatus <- getIngestStreamsWithStatus(namespace)
            (v1StreamsWithStatus, v2StreamsWithStatus) = streamsWithStatus.partitionMap {
              case (name, Left(v1)) => Left((name, v1))
              case (name, Right(v2)) => Right((name, v2))
            }
            _ <- storeLocalMetaData[Map[String, V1.IngestStreamWithStatus]](
              makeNamespaceMetaDataKey(namespace, IngestStreamsKey),
              thisMemberId,
              v1StreamsWithStatus.toMap,
            )
            _ <- saveV2IngestsToPersistor(
              namespace,
              thisMemberId,
              v2StreamsWithStatus.toMap,
            )
          } yield (),
        ),
      )
      .map(_ => ())
  }

  def removeIngestStream(
    name: String,
    namespace: NamespaceId,
  ): Option[IngestStreamWithControl[V1.IngestStreamConfiguration]] = noneIfNoNamespace(namespace) {
    Try {
      blocking(ingestStreamsLock.synchronized {
        ingestStreams.get(namespace).flatMap(_.get(name)).map { stream =>
          ingestStreams += namespace -> (ingestStreams(namespace) - name)
          Await.result(
            syncIngestStreamsMetaData(thisMemberIdx),
            QuineApp.ConfigApiTimeout,
          )
          stream
        }
      })
    }.toOption.flatten.map(isc => isc.copy(settings = isc.settings.asV1Config))

  }

  /** == Utilities == */

  private def stopAllIngestStreams(): Future[Unit] = {
    implicit val ec: ExecutionContext = graph.nodeDispatcherEC
    Future
      .traverse(ingestStreams.toList) { case (ns, ingestMap) =>
        Future.sequence(ingestMap.map { case (name, ingest) =>
          IngestMetered.removeIngestMeter(ns, name, graph.metrics)
          ingest.close()
          ingest.terminated().recover { case _ => Future.successful(Done) }
        })
      }(implicitly, graph.system.dispatcher)
      .map(_ => ())(graph.system.dispatcher)
  }

  /** Report telemetry only if the user has opted in (always `true` in trial-mode).
    * This needs to be loaded after the webserver is started; if not, the initial telemetry
    * startup message may not get sent.
    *
    * @param testOnlyImproveQuine ⚠️ only for testing: this [unfortunate] approach makes it possible,
    *                             with limited refactoring, to observe the effects of an [[ImproveQuine]]
    *                             class when the relationship between it and the Quine App is the
    *                             effectful relationship under test
    */
  private def initializeTelemetry(testOnlyImproveQuine: Option[ImproveQuine]): Unit =
    if (helpMakeQuineBetter) {
      val iq = testOnlyImproveQuine.getOrElse {
        new ImproveQuine(
          service = "Quine",
          version = BuildInfo.version,
          persistorSlug = graph.namespacePersistor.slug,
          getSources = () => getSources,
          getSinks = () => getSinks,
          recipe = recipe,
          recipeCanonicalName = recipeCanonicalName,
        )(system = graph.system, logConfig = logConfig)
      }
      iq.startTelemetry()
    }

  /** Notifies this Quine App that the web server has started.
    * Intended to enable the App to execute tasks that are not
    * safe to execute until the web server has started.
    *
    * @param testOnlyImproveQuine ⚠️ only for testing: this [unfortunate] approach makes it possible,
    *                             with limited refactoring, to observe the effects of an [[ImproveQuine]]
    *                             class when the relationship between it and the Quine App is the
    *                             effectful relationship under test
    */
  def notifyWebServerStarted(testOnlyImproveQuine: Option[ImproveQuine] = None): Unit =
    initializeTelemetry(testOnlyImproveQuine)

  /** Prepare for a shutdown */
  def shutdown()(implicit ec: ExecutionContext): Future[Unit] =
    for {
      _ <- syncIngestStreamsMetaData(thisMemberIdx)
      _ <- stopAllIngestStreams() // ... but don't update what is saved to disk
    } yield ()

  def restoreNonDefaultNamespacesFromMetaData(implicit ec: ExecutionContext): Future[Unit] =
    getOrDefaultGlobalMetaData(NonDefaultNamespacesKey, List.empty[String])
      .flatMap { nss =>
        Future.traverse(nss)(n => createNamespace(namespaceFromString(n), shouldWriteToPersistor = false))
      }
      .map(rs => require(rs.forall(identity), "Some namespaces could not be restored from persistence."))

  /** Load all the state from the persistor
    *
    * Not threadsafe, but we wait for this to complete before serving up the API.
    *
    * @param timeout            used repeatedly for individual calls to get metadata when restoring ingest streams.
    * @param shouldResumeIngest should restored ingest streams be resumed
    * @return A Future that success/fails indicating whether or not state was successfully restored (if any).
    */
  def loadAppData(timeout: Timeout, shouldResumeIngest: Boolean): Future[Unit] = {
    implicit val ec: ExecutionContext = graph.system.dispatcher
    val sampleQueriesFut =
      getOrDefaultGlobalMetaData(SampleQueriesKey, V1.SampleQuery.defaults)
    val quickQueriesFut = getOrDefaultGlobalMetaData(QuickQueriesKey, V1.UiNodeQuickQuery.defaults)
    val nodeAppearancesFut = getOrDefaultGlobalMetaData(NodeAppearancesKey, V1.UiNodeAppearance.defaults)

    // Register all user-defined procedures that require app/graph information (the rest will be loaded
    // when the first query is compiled by the [[resolveCalls]] step of the Cypher compilation pipeline)
    registerUserDefinedProcedure(
      new CypherParseProtobuf(protobufSchemaCache),
    )
    registerUserDefinedProcedure(
      new CypherToProtobuf(protobufSchemaCache),
    )
    registerUserDefinedProcedure(
      new CypherStandingWiretap((queryName, namespace) => getStandingQueryId(queryName, namespace)),
    )

    val standingQueryOutputsFut = Future
      .sequence(
        getNamespaces.map(ns =>
          getOrDefaultGlobalMetaData(
            makeNamespaceMetaDataKey(ns, StandingQueryOutputsKey),
            Map.empty: Map[FriendlySQName, (StandingQueryId, Map[SQOutputName, V1.StandingQueryResultOutputUserDef])],
          ).map(ns -> _),
        ),
      )
      .map(_.toMap)

    val standingQueryOutputs2DataFut = Future
      .sequence(
        getNamespaces.map(ns =>
          getOrDefaultGlobalMetaData(
            makeNamespaceMetaDataKey(ns, V2StandingQueryOutputsKey),
            Map.empty: V2StandingQueryDataMap,
          ).map(ns -> _),
        ),
      )
      .map(_.toMap)

    // Constructing an output 2 interpreter is asynchronous. It is chained onto the async read of the data version
    // rather than done as a synchronous step afterward like it is for the V1 outputs.
    val standingQueryOutput2Fut = standingQueryOutputs2DataFut.flatMap { nsMap =>
      Future
        .traverse(nsMap.toVector) { case (ns, queryOutputs) =>
          val queriesWithResultHubs = queryOutputs
            .map { case (queryName, (sqId, outputToWorkflowDef)) =>
              (queryName, sqId, outputToWorkflowDef, graph.standingQueries(ns).flatMap(_.standingResultsHub(sqId)))
            }
            .collect { case (queryName, sqId, outputToWorkflowDef, Some(resultHub)) =>
              (queryName, sqId, outputToWorkflowDef, resultHub)
            }
          Future
            .traverse(queriesWithResultHubs.toVector) { case (queryName, sqId, outputToWorkflowDef, resultHub) =>
              Future
                .traverse(outputToWorkflowDef.toVector) { case (outputName, workflowDef) =>
                  ApiToStanding(workflowDef, outputName, ns)(graph, protobufSchemaCache).map { workflowInterpreter =>
                    val killSwitch =
                      resultHub
                        .viaMat(KillSwitches.single)(Keep.right)
                        .via(workflowInterpreter.flow(graph)(logConfig))
                        .map(_ => SqResultsExecToken(s"SQ: $outputName in: $ns"))
                        .to(graph.masterStream.standingOutputsCompletionSink)
                        .run()(graph.materializer)
                    outputName -> OutputTarget.V2(workflowDef, killSwitch)
                  }
                }
                .map { outputNameToV2TargetPairs =>
                  val outputsMap = outputNameToV2TargetPairs.toMap
                  queryName -> (sqId, outputsMap)
                }
            }
            .map(queryNameToSqIdAndOutputTargetPairs => ns -> queryNameToSqIdAndOutputTargetPairs.toMap)
        }
        .map(nsToQueryOutput2TargetPairs => nsToQueryOutput2TargetPairs.toMap)
    }

    val ingestStreamFut = Future
      .sequence(
        getNamespaces.map(ns =>
          getOrDefaultLocalMetaDataWithFallback[Map[IngestName, V1.IngestStreamWithStatus], Map[
            IngestName,
            V1.IngestStreamConfiguration,
          ]](
            makeNamespaceMetaDataKey(ns, IngestStreamsKey),
            thisMemberIdx,
            Map.empty[IngestName, V1.IngestStreamWithStatus],
            _.view.mapValues(i => V1.IngestStreamWithStatus(config = i, status = None)).toMap,
          ).map(v => ns -> v),
        ),
      )
      .map(_.toMap)
    val v2IngestStreamFut = loadV2IngestsFromPersistor(thisMemberIdx)(
      V2IngestEntityEncoderDecoders.implicits.quineIngestStreamWithStatusSchema,
      implicitly,
    )
    for {
      sq <- sampleQueriesFut
      qq <- quickQueriesFut
      na <- nodeAppearancesFut
      so <- standingQueryOutputsFut
      so2 <- standingQueryOutput2Fut
      is <- ingestStreamFut
      is2 <- v2IngestStreamFut
    } yield {
      sampleQueries = sq
      quickQueries = qq
      nodeAppearances = na
      // Note: SQs on _the graph_ are restored and started during GraphService initialization.
      //       This sections restores the external handler for those results that publishes to outside systems.
      val v1OutputNamespaces = so.flatMap { case (namespace, outputTarget) =>
        graph
          .standingQueries(namespace)
          .map { sqns => // Silently ignores any SQs in an absent namespace.
            val restoredOutputTargets = outputTarget
              .map { case (sqName, (sqId, outputsStored)) =>
                (sqName, (sqId, outputsStored, sqns.standingResultsHub(sqId)))
              }
              .collect { case (sqName, (sqId, outputsStored, Some(sqResultSource))) =>
                val outputs = outputsStored.map { case (outputName, sqResultOutput) =>
                  // Attach the SQ result source to each consumer and track completion tokens in the masterStream
                  val killSwitch = sqResultSource.runWith(
                    StandingQueryResultOutput.resultHandlingSink(outputName, namespace, sqResultOutput, graph)(
                      protobufSchemaCache,
                      logConfig,
                    ),
                  )(graph.materializer)
                  outputName -> OutputTarget.V1(sqResultOutput, killSwitch)
                }
                sqName -> (sqId -> outputs)
              }
            Map(namespace -> restoredOutputTargets)
          }
          .getOrElse(Map.empty)
      }

      outputTargets = mergeOutputNamespaces(v1OutputNamespaces, so2)

      is.foreach { case (namespace, ingestMap) =>
        ingestMap.foreach { case (name, ingest) =>
          addIngestStream(
            name,
            ingest.config,
            namespace,
            previousStatus = ingest.status,
            shouldResumeIngest,
            timeout,
            shouldSaveMetadata = false, // We're restoring what was saved.
            Some(thisMemberIdx),
          ) match {
            case Success(true) => ()
            case Success(false) =>
              logger.error(
                safe"Duplicate ingest stream attempted to start with name: ${Safe(name)} and settings: ${ingest.config}",
              )
            case Failure(e) =>
              logger.error(
                log"Error when restoring ingest stream: ${Safe(name)} with settings: ${ingest.config}" withException e,
              )
          }
        }
      }
      is2.foreach { case (namespace, ingestMap) =>
        ingestMap.foreach { case (name, ingest) =>
          addV2IngestStream(
            name,
            ingest.config,
            namespace,
            previousStatus = ingest.status,
            shouldResumeIngest,
            timeout,
            shouldSaveMetadata = false, // We're restoring what was saved.
            Some(thisMemberIdx),
          ) match {
            case Validated.Valid(true) => ()
            case Validated.Valid(false) =>
              logger.error(
                safe"Duplicate ingest stream attempted to start with name: ${Safe(name)} and settings: ${ingest.config}",
              )
            case Validated.Invalid(e) =>
              logger.error(
                log"Error when restoring ingest stream: ${Safe(name)} with settings: ${ingest.config}" withException e.head,
              )
          }
        }
      }
    }
  }

  implicit private[this] val standingQueriesSchema: JsonSchema[
    Map[FriendlySQName, (StandingQueryId, Map[SQOutputName, V1.StandingQueryResultOutputUserDef])],
  ] = {
    implicit val sqIdSchema = genericRecord[StandingQueryId]
    implicit val tupSchema = genericRecord[(StandingQueryId, Map[SQOutputName, V1.StandingQueryResultOutputUserDef])]
    mapJsonSchema(tupSchema)
  }

  private[this] def storeStandingQueryOutputs(): Future[Unit] = {
    storeStandingQueryOutputs1()
    storeStandingQueryOutputs2()
  }

  private[this] def storeStandingQueryOutputs1(): Future[Unit] = {
    implicit val ec = graph.system.dispatcher
    Future
      .sequence(outputTargets.map { case (ns, targets) =>
        storeGlobalMetaData(
          makeNamespaceMetaDataKey(ns, StandingQueryOutputsKey),
          targets.map { case (name, (id, outputsMap)) =>
            name -> (id -> outputsMap.collect { case (outputName, OutputTarget.V1(definition, _)) =>
              outputName -> definition
            })
          },
        )
      })
      .map(_ => ())(ExecutionContext.parasitic)
  }

  private[this] def storeStandingQueryOutputs2(): Future[Unit] = {
    implicit val ec = graph.system.dispatcher
    Future
      .sequence(outputTargets.map { case (ns, targets) =>
        storeGlobalMetaData(
          makeNamespaceMetaDataKey(ns, V2StandingQueryOutputsKey),
          targets.map { case (name, (id, outputsMap)) =>
            name -> (id -> outputsMap.collect { case (outputName, OutputTarget.V2(definition, _)) =>
              outputName -> definition
            })
          },
        )(sqOutputs2Codec)
      })
      .map(_ => ())(ExecutionContext.parasitic)
  }
}

object QuineApp {

  final val VersionKey = "quine_app_state_version"
  final val SampleQueriesKey = "sample_queries"
  final val QuickQueriesKey = "quick_queries"
  final val NodeAppearancesKey = "node_appearances"
  final val StandingQueryOutputsKey = "standing_query_outputs"
  final val V2StandingQueryOutputsKey = "v2_standing_query_outputs"
  final val IngestStreamsKey = "ingest_streams"
  final val V2IngestStreamsKey = "v2_ingest_streams"
  final val NonDefaultNamespacesKey = "live_namespaces"

  type FriendlySQName = String
  type SQOutputName = String
  import com.thatdot.quine.app.StandingQueryResultOutput.OutputTarget

  private type OutputTargetsV1 = Map[SQOutputName, OutputTarget.V1]
  private type QueryOutputTargetsV1 = Map[FriendlySQName, (StandingQueryId, OutputTargetsV1)]
  private type NamespaceOutputTargetsV1 = Map[NamespaceId, QueryOutputTargetsV1]

  private type OutputTargetsV2 = Map[SQOutputName, OutputTarget.V2]
  private type QueryOutputTargetsV2 = Map[FriendlySQName, (StandingQueryId, OutputTargetsV2)]
  private type NamespaceOutputTargetsV2 = Map[NamespaceId, QueryOutputTargetsV2]

  private type OutputTargets = Map[SQOutputName, OutputTarget]
  private type QueryOutputTargets = Map[FriendlySQName, (StandingQueryId, OutputTargets)]
  private type NamespaceOutputTargets = Map[NamespaceId, QueryOutputTargets]

  import com.thatdot.quine.app.v2api.{definitions => Api2Defs}
  private type V2StandingQueryDataMap =
    Map[FriendlySQName, (StandingQueryId, Map[SQOutputName, Api2Defs.query.standing.StandingQueryResultWorkflow])]

  implicit val sqOutputs2Codec: EncoderDecoder[V2StandingQueryDataMap] = {
    import io.circe.generic.auto._
    EncoderDecoder.ofEncodeDecode
  }

  /** Maps the default namespace to the bare metadata key and other namespaces to that key concatenated with a hyphen
    *
    * @see GlobalPersistor.setLocalMetaData for where a local identifier is prepended to these keys with a hyphen.
    */
  def makeNamespaceMetaDataKey(namespace: NamespaceId, basedOnKey: String): String =
    // Example storage keys: "standing_query_outputs-myNamespace" or for default: "standing_query_outputs"
    basedOnKey + namespace.fold("")(_ => "-" + namespaceToString(namespace))

  // the maximum time to allow a configuring API call (e.g., "add ingest query" or "update node appearances") to execute
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
      Await.ready(synchronizeMe: Future[T], QuineApp.ConfigApiTimeout),
    ),
  )

  /** Version to track schemas saved by Quine app state
    *
    * Remember to increment this if schemas in Quine app state evolve in
    * backwards incompatible ways.
    */
  final val CurrentPersistenceVersion: Version = Version(1, 2, 0)

  def quineAppIsEmpty(persistenceAgent: PrimePersistor): Future[Boolean] = {
    val metaDataKeys =
      List(SampleQueriesKey, QuickQueriesKey, NodeAppearancesKey, StandingQueryOutputsKey, IngestStreamsKey)
    Future.foldLeft(
      metaDataKeys.map(k => persistenceAgent.getMetaData(k).map(_.isEmpty)(ExecutionContext.parasitic)),
    )(true)(_ && _)(ExecutionContext.parasitic)
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
    internal: StandingQueryInfo,
    inNamespace: NamespaceId,
    outputs: Map[String, V1.StandingQueryResultOutputUserDef],
    startTime: Instant,
    bufferSize: Int,
    metrics: HostQuineMetrics,
  ): V1.RegisteredStandingQuery = {
    val mode = internal.queryPattern match {
      case _: graph.StandingQueryPattern.DomainGraphNodeStandingQueryPattern =>
        V1.StandingQueryPattern.StandingQueryMode.DistinctId
      case _: graph.StandingQueryPattern.MultipleValuesQueryPattern =>
        V1.StandingQueryPattern.StandingQueryMode.MultipleValues
      case _: graph.StandingQueryPattern.QuinePatternQueryPattern =>
        V1.StandingQueryPattern.StandingQueryMode.QuinePattern
    }
    val pattern = internal.queryPattern.origin match {
      case graph.PatternOrigin.GraphPattern(_, Some(cypherQuery)) =>
        Some(V1.StandingQueryPattern.Cypher(cypherQuery, mode))
      case _ =>
        None
    }

    val meter = metrics.standingQueryResultMeter(inNamespace, internal.name)
    val outputHashCode = metrics.standingQueryResultHashCode(internal.id)

    V1.RegisteredStandingQuery(
      internal.name,
      internal.id.uuid,
      pattern,
      outputs,
      internal.queryPattern.includeCancellation,
      internal.queueBackpressureThreshold,
      stats = Map(
        "local" -> V1.StandingQueryStats(
          rates = V1.RatesSummary(
            count = meter.getCount,
            oneMinute = meter.getOneMinuteRate,
            fiveMinute = meter.getFiveMinuteRate,
            fifteenMinute = meter.getFifteenMinuteRate,
            overall = meter.getMeanRate,
          ),
          startTime,
          MILLIS.between(startTime, Instant.now()),
          bufferSize,
          outputHashCode.sum,
        ),
      ),
    )
  }

  /** Aggregate Quine SQ outputs and Quine standing query into a user-facing SQ, V2
    *
    * @note this includes only local information/metrics!
    * @param internal   Quine representation of the SQ
    * @param outputs    SQ outputs registered on the query
    * @param startTime  when the query was started (or re-started)
    * @param bufferSize number of elements buffered in the SQ output queue
    * @param metrics    Quine metrics object
    */
  private def makeRegisteredStandingQueryV2(
    internal: StandingQueryInfo,
    inNamespace: NamespaceId,
    outputs: Map[String, V2ApiStanding.StandingQueryResultWorkflow],
    startTime: Instant,
    bufferSize: Int,
    metrics: HostQuineMetrics,
  ): V2ApiStanding.StandingQuery.RegisteredStandingQuery = {
    // TODO Make a decision here about return type;
    //  - should callers manage getting this to a "data model" representation?
    //  - should this simply return the "data model" representation?
    //  - should this return an envelope of (spec, status, meta)?
    //    > honestly, is "registered" anything but a piece of "status" or "meta" on otherwise the same spec?
    //  Note that the near-equivalent of this work in QEApp is in getStandingQueriesWithNames2; it does not _need_ the
    //  same question to be answered, since there's no extraction like this, it just doesn't need to transform the data
    //  model back into the "internal [object] model" anymore.
    val mode = internal.queryPattern match {
      case _: graph.StandingQueryPattern.DomainGraphNodeStandingQueryPattern =>
        V2ApiStanding.StandingQueryPattern.StandingQueryMode.DistinctId
      case _: graph.StandingQueryPattern.MultipleValuesQueryPattern =>
        V2ApiStanding.StandingQueryPattern.StandingQueryMode.MultipleValues
      case _: graph.StandingQueryPattern.QuinePatternQueryPattern =>
        V2ApiStanding.StandingQueryPattern.StandingQueryMode.QuinePattern
    }
    val pattern: Option[V2ApiStanding.StandingQueryPattern] = internal.queryPattern.origin match {
      case graph.PatternOrigin.GraphPattern(_, Some(cypherQuery)) =>
        Some(V2ApiStanding.StandingQueryPattern.Cypher(cypherQuery, mode))
      case _ =>
        None
    }

    val meter = metrics.standingQueryResultMeter(inNamespace, internal.name)
    val outputHashCode = metrics.standingQueryResultHashCode(internal.id)

    V2ApiStanding.StandingQuery.RegisteredStandingQuery(
      name = internal.name,
      internalId = internal.id.uuid,
      pattern = pattern,
      outputs = outputs,
      includeCancellations = internal.queryPattern.includeCancellation,
      inputBufferSize = internal.queueBackpressureThreshold,
      stats = Map(
        "local" -> V2ApiStanding.StandingQueryStats(
          rates = Api2.RatesSummary(
            count = meter.getCount,
            oneMinute = meter.getOneMinuteRate,
            fiveMinute = meter.getFiveMinuteRate,
            fifteenMinute = meter.getFifteenMinuteRate,
            overall = meter.getMeanRate,
          ),
          startTime = startTime,
          totalRuntime = MILLIS.between(startTime, Instant.now()),
          bufferSize = bufferSize,
          outputHashCode = outputHashCode.sum,
        ),
      ),
    )
  }

  private def mergeOutputNamespaces(
    outputV1Namespaces: NamespaceOutputTargetsV1,
    outputV2Namespaces: NamespaceOutputTargetsV2,
  ): NamespaceOutputTargets = {
    val namespaces = outputV1Namespaces.keySet ++ outputV2Namespaces.keySet
    namespaces.foldLeft(Map.empty: NamespaceOutputTargets) { case (nsMap, ns) =>
      val v1Queries = outputV1Namespaces.getOrElse(ns, Map.empty)
      val v2Queries = outputV2Namespaces.getOrElse(ns, Map.empty)
      nsMap + (ns -> mergeOutputQueries(v1Queries, v2Queries))
    }
  }

  private def mergeOutputQueries(
    outputV1Queries: QueryOutputTargetsV1,
    outputV2Queries: QueryOutputTargetsV2,
  ): QueryOutputTargets = {
    val queryNames = outputV1Queries.keySet ++ outputV2Queries.keySet
    queryNames.foldLeft(Map.empty: QueryOutputTargets) { case (queryMap, queryName) =>
      (outputV1Queries.get(queryName), outputV2Queries.get(queryName)) match {
        case (Some((id1, outputs1)), Some((_, outputs2))) =>
          queryMap + (queryName -> (id1, outputs1 ++ outputs2))
        case (None, Some((id2, outputs2))) =>
          queryMap + (queryName -> (id2, outputs2))
        case (Some((id1, outputs1)), None) =>
          queryMap + (queryName -> (id1, outputs1))
        case (None, None) => queryMap
      }
    }
  }
}
