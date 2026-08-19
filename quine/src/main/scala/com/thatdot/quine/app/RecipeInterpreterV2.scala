package com.thatdot.quine.app

import java.net.URL
import java.util.concurrent.TimeoutException

import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.concurrent.{Await, ExecutionContext}
import scala.util.control.NonFatal
import scala.util.{Failure, Success}

import org.apache.pekko.actor.Cancellable
import org.apache.pekko.http.scaladsl.model.Uri
import org.apache.pekko.stream.scaladsl.{Keep, Sink}

import com.thatdot.api.v2.ResourceName
import com.thatdot.common.logging.Log.{LogConfig, Safe, SafeLoggableInterpolator}
import com.thatdot.quine.app.routes.QueryUiConfigurationState
import com.thatdot.quine.app.v2api.converters.ApiToUiStyling
import com.thatdot.quine.graph.cypher.{RunningCypherQuery, Value}
import com.thatdot.quine.graph.{BaseGraph, CypherOpsGraph, NamespaceId, defaultNamespaceId}
import com.thatdot.quine.model.QuineIdProvider
import com.thatdot.quine.serialization.ProtobufSchemaCache
import com.thatdot.quine.util.Log.implicits._

/** Runs a V2 Recipe by making a series of blocking graph method calls as determined
  * by the recipe content.
  *
  * Also starts fixed rate scheduled tasks to poll for and report status updates. These
  * should be cancelled using the returned Cancellable.
  */
case class RecipeInterpreterV2(
  statusLines: StatusLines,
  recipe: RecipeV2.Recipe,
  // Only the cosmetic UI-styling config is set directly through `appState` (node appearances, quick
  // queries, sample queries). Everything that needs validation — standing queries, ingest streams,
  // graph feeds — goes through the `registrar` seam instead.
  appState: QueryUiConfigurationState,
  graphService: CypherOpsGraph,
  quineWebserverUri: Option[URL],
  protobufSchemaCache: ProtobufSchemaCache,
  registrar: RecipeRegistrar,
)(implicit idProvider: QuineIdProvider)
    extends Cancellable {

  private var tasks: List[Cancellable] = List.empty

  // Recipes always use the default namespace.
  val namespace: NamespaceId = defaultNamespaceId

  implicit val ec: ExecutionContext = graphService.system.dispatcher

  /** Ceiling for the blocking wait on each recipe resource registration. Registration now routes
    * through the V2 API's validation (see [[RecipeRegistrar]]), whose dominant cost is the Kafka
    * bootstrap connectivity check (5s per Kafka ingest source / DLQ destination) plus
    * enrichment-Cypher compilation. Kept well above those internal timeouts so a slow-but-successful
    * validation is not reported as a spurious failure — a too-short wait throws `TimeoutException`
    * while the registration keeps running, leaving an untracked resource.
    */
  private val registrationTimeout: FiniteDuration = 30.seconds

  /** Cancel all the tasks, returning true if any task cancel returns true. */
  override def cancel(): Boolean = tasks.foldLeft(false)((a, b) => b.cancel() || a)

  /** Returns true if all the tasks report isCancelled true. */
  override def isCancelled: Boolean = tasks.forall(_.isCancelled)

  def run()(implicit logConfig: LogConfig): Unit = {

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

    // Create Standing Queries through the same validation path as the V2 API.
    for {
      (standingQueryDef, sqIndex) <- recipe.standingQueries.zipWithIndex
    } {
      val standingQueryRn: ResourceName =
        standingQueryDef.name.getOrElse(RecipeRegistrar.syntheticResourceName(s"standing-query-$sqIndex"))
      val standingQueryName: String = standingQueryRn.value

      try Await.result(
        registrar.registerStandingQuery(standingQueryRn, namespace, standingQueryDef),
        registrationTimeout,
      ) match {
        case Right(_) =>
          statusLines.info(log"Running Standing Query ${Safe(standingQueryName)}")
          tasks +:= standingQueryProgressReporter(statusLines, graphService, standingQueryName)
        case Left(errors) =>
          statusLines.error(
            log"Failed creating Standing Query ${Safe(standingQueryName)}: ${Safe(errors.mkString(", "))}",
          )
      } catch {
        case NonFatal(ex) =>
          statusLines.error(log"Failed creating Standing Query ${Safe(standingQueryName)}", ex)
      }
    }

    // Create Ingest Streams through the same validation path as the V2 API.
    for {
      (ingestStream, ingestIndex) <- recipe.ingestStreams.zipWithIndex
    } {
      val ingestStreamRn: ResourceName =
        ingestStream.name.getOrElse(RecipeRegistrar.syntheticResourceName(s"ingest-stream-$ingestIndex"))
      val ingestStreamName: String = ingestStreamRn.value

      try Await.result(
        registrar.registerIngest(ingestStreamRn, namespace, ingestStream),
        registrationTimeout,
      ) match {
        case Right(warnings) =>
          warnings.foreach(w => statusLines.warn(log"Ingest Stream ${Safe(ingestStreamName)}: ${Safe(w)}"))
          statusLines.info(log"Running Ingest Stream ${Safe(ingestStreamName)}")
          tasks +:= ingestStreamProgressReporter(statusLines, graphService, ingestStreamName)
        case Left(errors) =>
          statusLines.error(
            log"Failed creating Ingest Stream ${Safe(ingestStreamName)}: ${Safe(errors.mkString(", "))}",
          )
      } catch {
        case NonFatal(ex) =>
          statusLines.error(log"Failed creating Ingest Stream ${Safe(ingestStreamName)}", ex)
      }
    }

    // Register Graph Feeds through the same validation path as the V2 API (read-only projection
    // queries). Registered after standing queries because a graph feed references a standing query.
    if (recipe.graphFeeds.nonEmpty) {
      statusLines.info(log"Using ${Safe(recipe.graphFeeds.length)} graph feeds")
      try Await.result(registrar.registerGraphFeeds(namespace, recipe.graphFeeds.toVector), registrationTimeout) match {
        case Right(_) => ()
        case Left(errors) =>
          statusLines.error(log"Failed setting graph feeds: ${Safe(errors.mkString(", "))}")
      } catch {
        case NonFatal(ex) =>
          statusLines.error(log"Failed setting graph feeds", ex)
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
      registrar
        .ingestStreamStatus(ingestStreamName, namespace)
        .onComplete {
          case Failure(ex) =>
            statusLines.error(log"Failed getting Ingest Stream ${Safe(ingestStreamName)}" withException ex)
            task.cancel()
            statusLines.remove(statusLine)
            ()
          case Success(None) =>
            statusLines.error(log"Failed getting Ingest Stream ${Safe(ingestStreamName)} (it does not exist)")
            task.cancel()
            statusLines.remove(statusLine)
            ()
          case Success(Some(info)) =>
            val message =
              s"$ingestStreamName status is ${info.status.toString.toLowerCase} and ingested ${info.stats.ingestedCount}"
            if (info.status.isTerminal) {
              statusLines.info(log"${Safe(message)}")
              task.cancel()
              statusLines.remove(statusLine)
            } else {
              statusLines.update(statusLine, message)
            }
        }(graphService.system.dispatcher)
    }(graphService.system.dispatcher)
    task
  }

  private def standingQueryProgressReporter(
    statusLines: StatusLines,
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
      registrar
        .standingQueryStatus(standingQueryName, namespace)
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
