package com.thatdot.connect

import java.util.concurrent.TimeoutException
import java.util.concurrent.atomic.AtomicReference

import scala.collection.immutable
import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.control.NonFatal
import scala.util.{Failure, Success}

import akka.actor.Cancellable
import akka.stream.scaladsl.{Keep, Sink}

import com.google.common.net.PercentEscaper
import com.typesafe.scalalogging.{LazyLogging, Logger}

import com.thatdot.connect.routes.{IngestStreamState, QueryUiConfigurationState, StandingQueryStore}
import com.thatdot.quine.graph.cypher.{QueryResults, Value}
import com.thatdot.quine.graph.{BaseGraph, CypherOpsGraph}

/** Runs a Recipe by making a series of blocking graph method calls as determined
  * by the recipe content.
  *
  * Also starts fixed rate scheduled tasks to poll for and report status updates. These
  * should be cancelled using the returned Cancellable.
  */
object RecipeInterpreter extends LazyLogging {

  type RecipeState = QueryUiConfigurationState with IngestStreamState with StandingQueryStore

  def apply(
    printLogger: Logger,
    recipe: Recipe,
    appState: RecipeState,
    graphService: CypherOpsGraph,
    connectWebserverUrl: Option[String]
  )(implicit ec: ExecutionContext): Cancellable = {
    printLogger.info(s"Running Recipe ${recipe.title}")

    if (recipe.nodeAppearances.nonEmpty) {
      printLogger.info(s"Using ${recipe.nodeAppearances.length} node appearances")
      appState.setNodeAppearances(recipe.nodeAppearances.toVector)
    }
    if (recipe.quickQueries.nonEmpty) {
      printLogger.info(s"Using ${recipe.quickQueries.length} quick queries ")
      appState.setQuickQueries(recipe.quickQueries.toVector)
    }
    if (recipe.sampleQueries.nonEmpty) {
      printLogger.info(s"Using ${recipe.sampleQueries.length} sample queries ")
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
        logger.error(s"Standing Query $standingQueryName already exists")
      } else {
        printLogger.info(s"Running Standing Query $standingQueryName")
        tasks +:= standingQueryProgressReporter(printLogger, appState, graphService, standingQueryName)
      } catch {
        case NonFatal(ex) =>
          logger.error(s"Failed creating Standing Query $standingQueryName: $standingQueryDefinition", ex)
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
          logger.error(s"Failed creating Ingest Stream $ingestStreamName\n$ingestStream", ex)
        case Success(false) =>
          logger.error(s"Ingest Stream $ingestStreamName already exists")
        case Success(true) =>
          printLogger.info(s"Running Ingest Stream $ingestStreamName")
          tasks +:= ingestStreamProgressReporter(printLogger, appState, graphService, ingestStreamName)
      }

      // If status query is defined, print a URL with the query and schedule the query to be executed and printed
      for {
        statusQuery @ StatusQuery(cypherQuery) <- recipe.statusQuery
      } {
        for {
          url <- connectWebserverUrl
          escapedQuery = new PercentEscaper("", false).escape(cypherQuery)
        } printLogger.info(s"Status query URL is $url#$escapedQuery")
        tasks +:= statusQueryProgressReporter(printLogger, graphService, statusQuery)
      }
    }

    new Cancellable {

      /** Cancel all the tasks, returning true if any task cancel returns true. */
      override def cancel(): Boolean = tasks.foldLeft(false)((a, b) => b.cancel || a)

      /** Returns true if all the tasks report isCancelled true. */
      override def isCancelled: Boolean = tasks.forall(_.isCancelled)
    }
  }

  private def ingestStreamProgressReporter(
    printLogger: Logger,
    appState: RecipeState,
    graphService: BaseGraph,
    ingestStreamName: String,
    interval: FiniteDuration = 1 second
  )(implicit ec: ExecutionContext): Cancellable = {
    val actorSystem = graphService.system
    val onChanged = new OnChanged[String]
    lazy val task: Cancellable = actorSystem.scheduler.scheduleAtFixedRate(
      initialDelay = interval,
      interval = interval
    ) { () =>
      appState.getIngestStream(ingestStreamName) match {
        case None =>
          logger.error(s"Failed getting Ingest Stream $ingestStreamName (it does not exist)")
          task.cancel()
          ()
        case Some(ingestStream) =>
          for {
            status <- ingestStream.status
            stats = ingestStream.metrics.toEndpointResponse
          } {
            onChanged(
              s"$ingestStreamName status is ${status.toString.toLowerCase} and ingested ${stats.ingestedCount}"
            )(printLogger.info(_))
            if (status.isTerminal) {
              task.cancel()
            }
          }
      }
    }
    task
  }

  private def standingQueryProgressReporter(
    printLogger: Logger,
    appState: RecipeState,
    graph: BaseGraph,
    standingQueryName: String,
    interval: FiniteDuration = 1 second
  )(implicit ec: ExecutionContext): Cancellable = {
    val actorSystem = graph.system
    val onChanged = new OnChanged[String]
    lazy val task: Cancellable = actorSystem.scheduler.scheduleAtFixedRate(
      initialDelay = interval,
      interval = interval
    ) { () =>
      appState.getStandingQuery(standingQueryName) onComplete {
        case Failure(ex) =>
          logger.error(s"Failed getting Standing Query $standingQueryName", ex)
          task.cancel()
          ()
        case Success(None) =>
          logger.error(s"Failed getting Standing Query $standingQueryName (it does not exist)")
          task.cancel()
          ()
        case Success(Some(standingQuery)) =>
          val standingQueryStatsCount =
            standingQuery.stats.values.view.map(_.rates.count).sum
          onChanged(s"$standingQueryName count ${standingQueryStatsCount}")(printLogger.info(_))
      }
    }
    task
  }

  private val printQueryMaxResults = 10L
  private def statusQueryProgressReporter(
    printLogger: Logger,
    graphService: CypherOpsGraph,
    statusQuery: StatusQuery,
    interval: FiniteDuration = 5 second
  )(implicit ec: ExecutionContext): Cancellable = {
    val actorSystem = graphService.system
    val onChanged = new OnChanged[String]
    lazy val task: Cancellable = actorSystem.scheduler.scheduleWithFixedDelay(
      initialDelay = interval,
      delay = interval
    ) { () =>
      val queryResult: QueryResults = com.thatdot.quine.compiler.cypher.queryCypherValues(
        queryText = statusQuery.cypherQuery
      )(graphService, 10 seconds)
      try {
        val resultContent: immutable.Seq[Vector[Value]] =
          Await.result(
            queryResult.results.take(printQueryMaxResults).toMat(Sink.seq)(Keep.right).run()(graphService.materializer),
            5 seconds
          )
        onChanged(s"Status query result $resultContent")(printLogger.info(_))
      } catch {
        case _: TimeoutException => onChanged("Status query timed out")(printLogger.warn(_))
      }
    }
    task
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
