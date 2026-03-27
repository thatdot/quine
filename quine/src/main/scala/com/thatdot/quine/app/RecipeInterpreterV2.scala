package com.thatdot.quine.app

import java.net.URL
import java.util.concurrent.TimeoutException

import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.control.NonFatal
import scala.util.{Failure, Success}

import org.apache.pekko.actor.Cancellable
import org.apache.pekko.http.scaladsl.model.Uri
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.{Keep, Sink}

import com.thatdot.common.logging.Log.{LogConfig, Safe, SafeLoggableInterpolator}
import com.thatdot.quine.app.model.ingest2.V2IngestEntities
import com.thatdot.quine.app.model.ingest2.V2IngestEntities.QuineIngestConfiguration
import com.thatdot.quine.app.routes.{IngestStreamState, QueryUiConfigurationState, StandingQueryInterfaceV2}
import com.thatdot.quine.app.v2api.converters.{ApiToIngest, ApiToUiStyling}
import com.thatdot.quine.app.v2api.definitions.query.{standing => ApiStanding}
import com.thatdot.quine.graph.cypher.{RunningCypherQuery, Value}
import com.thatdot.quine.graph.{BaseGraph, CypherOpsGraph, MemberIdx, NamespaceId}
import com.thatdot.quine.model.QuineIdProvider
import com.thatdot.quine.serialization.ProtobufSchemaCache
import com.thatdot.quine.util.Log.implicits._

object RecipeInterpreterV2 {
  type RecipeStateV2 = QueryUiConfigurationState with IngestStreamState with StandingQueryInterfaceV2
}

/** Runs a V2 Recipe by making a series of blocking graph method calls as determined
  * by the recipe content.
  *
  * Also starts fixed rate scheduled tasks to poll for and report status updates. These
  * should be cancelled using the returned Cancellable.
  */
case class RecipeInterpreterV2(
  statusLines: StatusLines,
  recipe: RecipeV2.Recipe,
  appState: RecipeInterpreterV2.RecipeStateV2,
  graphService: CypherOpsGraph,
  quineWebserverUri: Option[URL],
  protobufSchemaCache: ProtobufSchemaCache,
)(implicit idProvider: QuineIdProvider)
    extends Cancellable {

  private var tasks: List[Cancellable] = List.empty

  // Recipes always use the default namespace.
  val namespace: NamespaceId = None

  implicit val ec: ExecutionContext = graphService.system.dispatcher

  /** Cancel all the tasks, returning true if any task cancel returns true. */
  override def cancel(): Boolean = tasks.foldLeft(false)((a, b) => b.cancel() || a)

  /** Returns true if all the tasks report isCancelled true. */
  override def isCancelled: Boolean = tasks.forall(_.isCancelled)

  def run(memberIdx: MemberIdx)(implicit logConfig: LogConfig): Unit = {

    // Set UI appearances using V2 -> V1 converters
    if (recipe.nodeAppearances.nonEmpty) {
      statusLines.info(log"Using ${Safe(recipe.nodeAppearances.length)} node appearances")
      val v1Appearances = recipe.nodeAppearances.map(ApiToUiStyling.apply).toVector
      appState.setNodeAppearances(v1Appearances)
    }
    if (recipe.quickQueries.nonEmpty) {
      statusLines.info(log"Using ${Safe(recipe.quickQueries.length)} quick queries")
      val v1QuickQueries = recipe.quickQueries.map(ApiToUiStyling.apply).toVector
      appState.setQuickQueries(v1QuickQueries)
    }
    if (recipe.sampleQueries.nonEmpty) {
      statusLines.info(log"Using ${Safe(recipe.sampleQueries.length)} sample queries")
      val v1SampleQueries = recipe.sampleQueries.map(ApiToUiStyling.apply).toVector
      appState.setSampleQueries(v1SampleQueries)
    }

    // Create Standing Queries using V2 API
    for {
      (standingQueryDef, sqIndex) <- recipe.standingQueries.zipWithIndex
    } {
      val standingQueryName = standingQueryDef.name.getOrElse(s"standing-query-$sqIndex")

      // Convert recipe SQ definition to API format
      val apiSqDef = ApiStanding.StandingQuery.StandingQueryDefinition(
        name = standingQueryName,
        pattern = standingQueryDef.pattern,
        outputs = standingQueryDef.outputs.zipWithIndex.map { case (workflow, wfIndex) =>
          ApiStanding.StandingQueryResultWorkflow(
            name = workflow.name.getOrElse(s"output-$wfIndex"),
            filter = workflow.filter,
            preEnrichmentTransformation = workflow.preEnrichmentTransformation,
            resultEnrichment = workflow.resultEnrichment.map(e =>
              com.thatdot.quine.app.v2api.definitions.outputs.QuineDestinationSteps.CypherQuery(
                query = e.query,
                parameter = e.parameter,
              ),
            ),
            destinations = workflow.destinations,
          )
        },
        includeCancellations = standingQueryDef.includeCancellations,
        inputBufferSize = standingQueryDef.inputBufferSize,
      )

      val addResult: Future[StandingQueryInterfaceV2.Result] =
        appState.addStandingQueryV2(standingQueryName, namespace, apiSqDef)

      try Await.result(addResult, 5.seconds) match {
        case StandingQueryInterfaceV2.Result.Success =>
          statusLines.info(log"Running Standing Query ${Safe(standingQueryName)}")
          tasks +:= standingQueryProgressReporter(statusLines, appState, graphService, standingQueryName)
        case StandingQueryInterfaceV2.Result.AlreadyExists(_) =>
          statusLines.error(log"Standing Query ${Safe(standingQueryName)} already exists")
        case StandingQueryInterfaceV2.Result.NotFound(msg) =>
          statusLines.error(log"Namespace not found: ${Safe(msg)}")
      } catch {
        case NonFatal(ex) =>
          statusLines.error(
            log"Failed creating Standing Query ${Safe(standingQueryName)}",
            ex,
          )
      }
    }

    // Create Ingest Streams using V2 API
    for {
      (ingestStream, ingestIndex) <- recipe.ingestStreams.zipWithIndex
    } {
      val ingestStreamName = ingestStream.name.getOrElse(s"ingest-stream-$ingestIndex")

      // Convert recipe ingest to V2 internal model
      val v2IngestSource = ApiToIngest(ingestStream.source)
      val onStreamError = ingestStream.onStreamError
        .map(ApiToIngest.apply)
        .getOrElse(V2IngestEntities.LogStreamError)

      val v2IngestConfig = QuineIngestConfiguration(
        name = ingestStreamName,
        source = v2IngestSource,
        query = ingestStream.query,
        parameter = ingestStream.parameter,
        transformation = None, // TODO: handle transformation conversion
        parallelism = ingestStream.parallelism,
        maxPerSecond = ingestStream.maxPerSecond,
        onRecordError = ingestStream.onRecordError,
        onStreamError = onStreamError,
      )

      val result: Future[Either[Seq[String], Unit]] = appState.addV2IngestStream(
        name = ingestStreamName,
        settings = v2IngestConfig,
        intoNamespace = namespace,
        timeout = 5.seconds,
        memberIdx = memberIdx,
      )

      try Await.result(result, 10.seconds) match {
        case Left(errors) =>
          statusLines.error(
            log"Failed creating Ingest Stream ${Safe(ingestStreamName)}: ${Safe(errors.mkString(", "))}",
          )
        case Right(_) =>
          statusLines.info(log"Running Ingest Stream ${Safe(ingestStreamName)}")
          tasks +:= ingestStreamProgressReporter(statusLines, appState, graphService, ingestStreamName)
      } catch {
        case NonFatal(ex) =>
          statusLines.error(
            log"Failed creating Ingest Stream ${Safe(ingestStreamName)}",
            ex,
          )
      }
    }

    // Handle status query
    for {
      statusQuery <- recipe.statusQuery
    } {
      for {
        url <- quineWebserverUri
      } statusLines.info(
        log"Status query URL is ${Safe(
          Uri
            .from(
              scheme = url.getProtocol,
              userinfo = Option(url.getUserInfo).getOrElse(""),
              host = url.getHost,
              port = url.getPort,
              path = url.getPath,
              queryString = None,
              fragment = Some(statusQuery.cypherQuery),
            )
            .toString,
        )}",
      )
      tasks +:= statusQueryProgressReporter(statusLines, graphService, statusQuery)
    }
  }

  private def ingestStreamProgressReporter(
    statusLines: StatusLines,
    appState: RecipeInterpreterV2.RecipeStateV2,
    graphService: BaseGraph,
    ingestStreamName: String,
    interval: FiniteDuration = 1.second,
  )(implicit logConfig: LogConfig): Cancellable = {
    val actorSystem = graphService.system
    val statusLine = statusLines.create()
    lazy val task: Cancellable = actorSystem.scheduler.scheduleAtFixedRate(
      initialDelay = interval,
      interval = interval,
    ) { () =>
      appState.getIngestStream(ingestStreamName, namespace) match {
        case None =>
          statusLines.error(log"Failed getting Ingest Stream ${Safe(ingestStreamName)} (it does not exist)")
          task.cancel()
          statusLines.remove(statusLine)
          ()
        case Some(ingestStream) =>
          ingestStream
            .status(Materializer.matFromSystem(actorSystem))
            .foreach { status =>
              val stats = ingestStream.metrics.toEndpointResponse
              val message =
                s"$ingestStreamName status is ${status.toString.toLowerCase} and ingested ${stats.ingestedCount}"
              if (status.isTerminal) {
                statusLines.info(log"${Safe(message)}")
                task.cancel()
                statusLines.remove(statusLine)
              } else {
                statusLines.update(
                  statusLine,
                  message,
                )
              }
            }(graphService.system.dispatcher)
      }
    }(graphService.system.dispatcher)
    task
  }

  private def standingQueryProgressReporter(
    statusLines: StatusLines,
    appState: RecipeInterpreterV2.RecipeStateV2,
    graph: BaseGraph,
    standingQueryName: String,
    interval: FiniteDuration = 1.second,
  )(implicit logConfig: LogConfig): Cancellable = {
    val actorSystem = graph.system
    val statusLine = statusLines.create()
    lazy val task: Cancellable = actorSystem.scheduler.scheduleAtFixedRate(
      initialDelay = interval,
      interval = interval,
    ) { () =>
      appState
        .getStandingQueryV2(standingQueryName, namespace)
        .onComplete {
          case Failure(ex) =>
            statusLines.error(log"Failed getting Standing Query ${Safe(standingQueryName)}" withException ex)
            task.cancel()
            statusLines.remove(statusLine)
            ()
          case Success(None) =>
            statusLines.error(log"Failed getting Standing Query ${Safe(standingQueryName)} (it does not exist)")
            task.cancel()
            statusLines.remove(statusLine)
            ()
          case Success(Some(standingQuery)) =>
            val standingQueryStatsCount =
              standingQuery.stats.values.view.map(_.rates.count).sum
            statusLines.update(statusLine, s"$standingQueryName count $standingQueryStatsCount")
        }(graph.system.dispatcher)
    }(graph.system.dispatcher)
    task
  }

  private val printQueryMaxResults = 10L

  private def statusQueryProgressReporter(
    statusLines: StatusLines,
    graphService: CypherOpsGraph,
    statusQuery: RecipeV2.StatusQueryV2,
    interval: FiniteDuration = 5.second,
  )(implicit idProvider: QuineIdProvider, logConfig: LogConfig): Cancellable = {
    val actorSystem = graphService.system
    val changed = new OnChanged[String]
    lazy val task: Cancellable = actorSystem.scheduler.scheduleWithFixedDelay(
      initialDelay = interval,
      delay = interval,
    ) { () =>
      val queryResults: RunningCypherQuery = com.thatdot.quine.compiler.cypher.queryCypherValues(
        queryText = statusQuery.cypherQuery,
        namespace = namespace,
      )(graphService)
      try {
        val resultContent: Seq[Seq[Value]] =
          Await.result(
            queryResults.results
              .take(printQueryMaxResults)
              .toMat(Sink.seq)(Keep.right)
              .named("recipe-status-query")
              .run()(graphService.materializer),
            5.seconds,
          )
        changed(queryResultToString(queryResults, resultContent))(s => statusLines.info(log"${Safe(s)}"))
      } catch {
        case _: TimeoutException => statusLines.warn(log"Status query timed out")
      }
    }(graphService.system.dispatcher)
    task
  }

  /** Formats query results into a multi-line string designed to be easily human-readable. */
  private def queryResultToString(queryResults: RunningCypherQuery, resultContent: Seq[Seq[Value]])(implicit
    idProvider: QuineIdProvider,
    logConfig: LogConfig,
  ): String = {
    import java.lang.System.lineSeparator

    def repeated(s: String, times: Int): String =
      Seq.fill(times)(s).mkString

    def fixedLength(s: String, length: Int, padding: Char): String =
      if (s.length < length) {
        s + repeated(padding.toString, length - s.length)
      } else if (s.length > length) {
        s.substring(0, length)
      } else {
        s
      }

    (for { (resultRecord, resultRecordIndex) <- resultContent.zipWithIndex } yield {
      val columnNameFixedWidthMax = 20
      val columnNameFixedWidth =
        Math.min(
          queryResults.columns.map(_.name.length).max,
          columnNameFixedWidthMax,
        )
      val valueStrings = resultRecord.map(Value.toJson(_).noSpaces)
      val valueStringMaxLength = valueStrings.map(_.length).max
      val separator = " | "
      val headerLengthMin = 40
      val headerLengthMax = 200
      val header =
        fixedLength(
          s"---[ Status Query result ${resultRecordIndex + 1} ]",
          Math.max(
            headerLengthMin,
            Math.min(columnNameFixedWidth + valueStringMaxLength + separator.length, headerLengthMax),
          ),
          '-',
        )
      val footer =
        repeated("-", columnNameFixedWidth + 1) + "+" + repeated("-", header.length - columnNameFixedWidth - 2)
      header + lineSeparator + {
        {
          for {
            (columnName, value) <- queryResults.columns.zip(valueStrings)
            fixedLengthColumnName = fixedLength(columnName.name, columnNameFixedWidth, ' ')
          } yield fixedLengthColumnName + separator + value
        } mkString lineSeparator
      } + lineSeparator + footer
    }) mkString lineSeparator
  }
}
