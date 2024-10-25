package com.thatdot.quine.app

import java.lang.System.lineSeparator
import java.net.URL
import java.util.concurrent.TimeoutException
import java.util.concurrent.atomic.AtomicReference

import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.concurrent.{Await, Future}
import scala.util.control.NonFatal
import scala.util.{Failure, Success}

import org.apache.pekko.actor.Cancellable
import org.apache.pekko.http.scaladsl.model.Uri
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.{Keep, Sink}

import com.thatdot.quine.app.RecipeInterpreter.RecipeState
import com.thatdot.quine.app.routes.{IngestStreamState, QueryUiConfigurationState, StandingQueryStore}
import com.thatdot.quine.app.util.QuineLoggables._
import com.thatdot.quine.graph.cypher.{RunningCypherQuery, Value}
import com.thatdot.quine.graph.{BaseGraph, CypherOpsGraph, MemberIdx, NamespaceId}
import com.thatdot.quine.model.QuineIdProvider
import com.thatdot.quine.util.Log._
import com.thatdot.quine.util.Log.implicits._

object RecipeInterpreter {

  type RecipeState = QueryUiConfigurationState with IngestStreamState with StandingQueryStore
}

/** Runs a Recipe by making a series of blocking graph method calls as determined
  * by the recipe content.
  *
  * Also starts fixed rate scheduled tasks to poll for and report status updates. These
  * should be cancelled using the returned Cancellable.
  */
case class RecipeInterpreter(
  statusLines: StatusLines,
  recipe: Recipe,
  appState: RecipeState,
  graphService: CypherOpsGraph,
  quineWebserverUri: Option[URL],
)(implicit idProvider: QuineIdProvider)
    extends Cancellable {

  private var tasks: List[Cancellable] = List.empty

  // Recipes always use the default namespace.
  val namespace: NamespaceId = None

  /** Cancel all the tasks, returning true if any task cancel returns true. */
  override def cancel(): Boolean = tasks.foldLeft(false)((a, b) => b.cancel() || a)

  /** Returns true if all the tasks report isCancelled true. */
  override def isCancelled: Boolean = tasks.forall(_.isCancelled)

  def run(memberIdx: MemberIdx)(implicit logConfig: LogConfig): Unit = {

    if (recipe.nodeAppearances.nonEmpty) {
      statusLines.info(log"Using ${Safe(recipe.nodeAppearances.length)} node appearances")
      appState.setNodeAppearances(recipe.nodeAppearances.toVector)
    }
    if (recipe.quickQueries.nonEmpty) {
      statusLines.info(log"Using ${Safe(recipe.quickQueries.length)} quick queries")
      appState.setQuickQueries(recipe.quickQueries.toVector)
    }
    if (recipe.sampleQueries.nonEmpty) {
      statusLines.info(log"Using ${Safe(recipe.sampleQueries.length)} sample queries")
      appState.setSampleQueries(recipe.sampleQueries.toVector)
    }

    // Create Standing Queries
    for {
      (standingQueryDefinition, i) <- recipe.standingQueries.zipWithIndex
    } {
      val standingQueryName = s"STANDING-${i + 1}"
      val addStandingQueryResult: Future[Boolean] = appState.addStandingQuery(
        standingQueryName,
        namespace,
        standingQueryDefinition,
      )
      try if (!Await.result(addStandingQueryResult, 5 seconds)) {
        statusLines.error(log"Standing Query ${Safe(standingQueryName)} already exists")
      } else {
        statusLines.info(log"Running Standing Query ${Safe(standingQueryName)}")
        tasks +:= standingQueryProgressReporter(statusLines, appState, graphService, standingQueryName)
      } catch {
        case NonFatal(ex) =>
          statusLines.error(
            log"Failed creating Standing Query ${Safe(standingQueryName)}: ${standingQueryDefinition}",
            ex,
          )
      }
      ()
    }

    // Create Ingest Streams
    for {
      (ingestStream, i) <- recipe.ingestStreams.zipWithIndex
    } {
      val ingestStreamName = s"INGEST-${i + 1}"
      appState.addIngestStream(
        ingestStreamName,
        ingestStream,
        namespace,
        previousStatus = None,
        shouldResumeRestoredIngests = false,
        timeout = 5 seconds,
        memberIdx = Some(memberIdx),
      ) match {
        case Failure(ex) =>
          statusLines.error(
            log"Failed creating Ingest Stream ${Safe(ingestStreamName)}\n${ingestStream}",
            ex,
          )
        case Success(false) =>
          statusLines.error(log"Ingest Stream ${Safe(ingestStreamName)} already exists")
        case Success(true) =>
          statusLines.info(log"Running Ingest Stream ${Safe(ingestStreamName)}")
          tasks +:= ingestStreamProgressReporter(statusLines, appState, graphService, ingestStreamName)
      }

      // If status query is defined, print a URL with the query and schedule the query to be executed and printed
      for {
        statusQuery @ StatusQuery(cypherQuery) <- recipe.statusQuery
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
                fragment = Some(cypherQuery),
              )
              .toString,
          )}",
        )
        tasks +:= statusQueryProgressReporter(statusLines, graphService, statusQuery)
      }
    }

  }

  private def ingestStreamProgressReporter(
    statusLines: StatusLines,
    appState: RecipeState,
    graphService: BaseGraph,
    ingestStreamName: String,
    interval: FiniteDuration = 1 second,
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
    appState: RecipeState,
    graph: BaseGraph,
    standingQueryName: String,
    interval: FiniteDuration = 1 second,
  )(implicit logConfig: LogConfig): Cancellable = {
    val actorSystem = graph.system
    val statusLine = statusLines.create()
    lazy val task: Cancellable = actorSystem.scheduler.scheduleAtFixedRate(
      initialDelay = interval,
      interval = interval,
    ) { () =>
      appState
        .getStandingQuery(standingQueryName, namespace)
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
    statusQuery: StatusQuery,
    interval: FiniteDuration = 5 second,
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
            5 seconds,
          )
        changed(queryResultToString(queryResults, resultContent))(s =>
          // s is a query result, and therefore PII, but the entire point of a status query is to repeatedly log
          // this value, so we'll treat that as implied consent to log.
          statusLines.info(log"${Safe(s)}"),
        )
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

    /** Builds a repeated string by concatenation. */
    def repeated(s: String, times: Int): String =
      Seq.fill(times)(s).mkString

    /** Sets the string length, by adding padding or truncating. */
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

/** Simple utility to call a parameterized function only when the input value has changed.
  * E.g. for periodically printing logged status updates only when the log message contains a changed string.
  * Intended for use from multiple concurrent threads.
  * Callback IS called on first invocation.
  *
  * @tparam T The input value that is compared for change using `equals` equality.
  */
class OnChanged[T] {
  private val lastValue: AtomicReference[Option[T]] = new AtomicReference(None)

  def apply(value: T)(callback: T => Unit): Unit = {
    val newValue = Some(value)
    val prevValue = lastValue.getAndSet(newValue)
    if (prevValue != newValue) {
      callback(value)
    }
    ()
  }
}
