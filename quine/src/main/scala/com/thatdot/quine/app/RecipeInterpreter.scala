package com.thatdot.quine.app

import java.lang.System.lineSeparator
import java.util.concurrent.TimeoutException
import java.util.concurrent.atomic.AtomicReference

import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.control.NonFatal
import scala.util.{Failure, Success}

import akka.actor.Cancellable
import akka.stream.Materializer
import akka.stream.scaladsl.{Keep, Sink}

import com.google.common.net.PercentEscaper

import com.thatdot.quine.app.routes.{IngestStreamState, QueryUiConfigurationState, StandingQueryStore}
import com.thatdot.quine.graph.cypher.{QueryResults, Value}
import com.thatdot.quine.graph.{BaseGraph, CypherOpsGraph}
import com.thatdot.quine.model.QuineIdProvider

/** Runs a Recipe by making a series of blocking graph method calls as determined
  * by the recipe content.
  *
  * Also starts fixed rate scheduled tasks to poll for and report status updates. These
  * should be cancelled using the returned Cancellable.
  */
object RecipeInterpreter {

  type RecipeState = QueryUiConfigurationState with IngestStreamState with StandingQueryStore

  def apply(
    statusLines: StatusLines,
    recipe: Recipe,
    appState: RecipeState,
    graphService: CypherOpsGraph,
    quineWebserverUrl: Option[String]
  )(implicit ec: ExecutionContext, idProvider: QuineIdProvider): Cancellable = {
    statusLines.info(s"Running Recipe ${recipe.title}")

    if (recipe.nodeAppearances.nonEmpty) {
      statusLines.info(s"Using ${recipe.nodeAppearances.length} node appearances")
      appState.setNodeAppearances(recipe.nodeAppearances.toVector)
    }
    if (recipe.quickQueries.nonEmpty) {
      statusLines.info(s"Using ${recipe.quickQueries.length} quick queries ")
      appState.setQuickQueries(recipe.quickQueries.toVector)
    }
    if (recipe.sampleQueries.nonEmpty) {
      statusLines.info(s"Using ${recipe.sampleQueries.length} sample queries ")
      appState.setSampleQueries(recipe.sampleQueries.toVector)
    }

    var tasks: List[Cancellable] = List.empty

    // Create Standing Queries
    for {
      (standingQueryDefinition, i) <- recipe.standingQueries.zipWithIndex
    } {
      val standingQueryName = s"STANDING-${i + 1}"
      val addStandingQueryResult: Future[Boolean] = appState.addStandingQuery(
        standingQueryName,
        standingQueryDefinition
      )
      try if (!Await.result(addStandingQueryResult, 5 seconds)) {
        statusLines.error(s"Standing Query $standingQueryName already exists")
      } else {
        statusLines.info(s"Running Standing Query $standingQueryName")
        tasks +:= standingQueryProgressReporter(statusLines, appState, graphService, standingQueryName)
      } catch {
        case NonFatal(ex) =>
          statusLines.error(s"Failed creating Standing Query $standingQueryName: $standingQueryDefinition", ex)
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
        wasRestoredFromStorage = false,
        timeout = 5 seconds
      ) match {
        case Failure(ex) =>
          statusLines.error(s"Failed creating Ingest Stream $ingestStreamName\n$ingestStream", ex)
        case Success(false) =>
          statusLines.error(s"Ingest Stream $ingestStreamName already exists")
        case Success(true) =>
          statusLines.info(s"Running Ingest Stream $ingestStreamName")
          tasks +:= ingestStreamProgressReporter(statusLines, appState, graphService, ingestStreamName)
      }

      // If status query is defined, print a URL with the query and schedule the query to be executed and printed
      for {
        statusQuery @ StatusQuery(cypherQuery) <- recipe.statusQuery
      } {
        for {
          url <- quineWebserverUrl
          escapedQuery = new PercentEscaper("", false).escape(cypherQuery)
        } statusLines.info(s"Status query URL is $url#$escapedQuery")
        tasks +:= statusQueryProgressReporter(statusLines, graphService, statusQuery)
      }
    }

    new MultiCancellable(tasks)
  }

  private def ingestStreamProgressReporter(
    statusLines: StatusLines,
    appState: RecipeState,
    graphService: BaseGraph,
    ingestStreamName: String,
    interval: FiniteDuration = 1 second
  )(implicit ec: ExecutionContext): Cancellable = {
    val actorSystem = graphService.system
    val statusLine = statusLines.create()
    lazy val task: Cancellable = actorSystem.scheduler.scheduleAtFixedRate(
      initialDelay = interval,
      interval = interval
    ) { () =>
      appState.getIngestStream(ingestStreamName) match {
        case None =>
          statusLines.error(s"Failed getting Ingest Stream $ingestStreamName (it does not exist)")
          task.cancel()
          statusLines.remove(statusLine)
          ()
        case Some(ingestStream) =>
          for {
            status <- ingestStream.status(Materializer.matFromSystem(actorSystem))
            stats = ingestStream.metrics.toEndpointResponse
          } {
            val message =
              s"$ingestStreamName status is ${status.toString.toLowerCase} and ingested ${stats.ingestedCount}"
            if (status.isTerminal) {
              statusLines.info(message)
              task.cancel()
              statusLines.remove(statusLine)
            } else {
              statusLines.update(
                statusLine,
                message
              )
            }
          }
      }
    }
    task
  }

  private def standingQueryProgressReporter(
    statusLines: StatusLines,
    appState: RecipeState,
    graph: BaseGraph,
    standingQueryName: String,
    interval: FiniteDuration = 1 second
  )(implicit ec: ExecutionContext): Cancellable = {
    val actorSystem = graph.system
    val statusLine = statusLines.create()
    lazy val task: Cancellable = actorSystem.scheduler.scheduleAtFixedRate(
      initialDelay = interval,
      interval = interval
    ) { () =>
      appState.getStandingQuery(standingQueryName) onComplete {
        case Failure(ex) =>
          statusLines.error(s"Failed getting Standing Query $standingQueryName", ex)
          task.cancel()
          statusLines.remove(statusLine)
          ()
        case Success(None) =>
          statusLines.error(s"Failed getting Standing Query $standingQueryName (it does not exist)")
          task.cancel()
          statusLines.remove(statusLine)
          ()
        case Success(Some(standingQuery)) =>
          val standingQueryStatsCount =
            standingQuery.stats.values.view.map(_.rates.count).sum
          statusLines.update(statusLine, s"$standingQueryName count ${standingQueryStatsCount}")
      }
    }
    task
  }

  private val printQueryMaxResults = 10L
  private def statusQueryProgressReporter(
    statusLines: StatusLines,
    graphService: CypherOpsGraph,
    statusQuery: StatusQuery,
    interval: FiniteDuration = 5 second
  )(implicit ec: ExecutionContext, idProvider: QuineIdProvider): Cancellable = {
    val actorSystem = graphService.system
    val changed = new OnChanged[String]
    lazy val task: Cancellable = actorSystem.scheduler.scheduleWithFixedDelay(
      initialDelay = interval,
      delay = interval
    ) { () =>
      val queryResults: QueryResults = com.thatdot.quine.compiler.cypher.queryCypherValues(
        queryText = statusQuery.cypherQuery
      )(graphService)
      try {
        val resultContent: Seq[Seq[Value]] =
          Await.result(
            queryResults.results
              .take(printQueryMaxResults)
              .toMat(Sink.seq)(Keep.right)
              .run()(graphService.materializer),
            5 seconds
          )
        changed(queryResultToString(queryResults, resultContent))(statusLines.info(_))
      } catch {
        case _: TimeoutException => statusLines.warn("Status query timed out")
      }
    }
    task
  }

  /** Formats query results into a multi-line string designed to be easily human-readable. */
  private def queryResultToString(queryResults: QueryResults, resultContent: Seq[Seq[Value]])(implicit
    idProvider: QuineIdProvider
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
          columnNameFixedWidthMax
        )
      val valueStrings = resultRecord.map(Value.toJson(_).toString)
      val valueStringMaxLength = valueStrings.map(_.length).max
      val separator = " | "
      val headerLengthMin = 40
      val headerLengthMax = 200
      val header =
        fixedLength(
          s"---[ Status Query result ${resultRecordIndex + 1} ]",
          Math.max(
            headerLengthMin,
            Math.min(columnNameFixedWidth + valueStringMaxLength + separator.length, headerLengthMax)
          ),
          '-'
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

class MultiCancellable(tasks: List[Cancellable]) extends Cancellable {

  /** Cancel all the tasks, returning true if any task cancel returns true. */
  override def cancel(): Boolean = tasks.foldLeft(false)((a, b) => b.cancel || a)

  /** Returns true if all the tasks report isCancelled true. */
  override def isCancelled: Boolean = tasks.forall(_.isCancelled)
}
