package com.thatdot.quine.app

import java.io.File
import java.net.URL
import java.nio.charset.{Charset, StandardCharsets}
import java.text.NumberFormat

import scala.collection.compat._
import scala.compat.ExecutionContexts
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.control.NonFatal
import scala.util.{Failure, Success}

import akka.Done
import akka.actor.{ActorSystem, Cancellable, CoordinatedShutdown}
import akka.util.Timeout

import cats.syntax.either._
import ch.qos.logback.classic.LoggerContext
import com.typesafe.scalalogging.{LazyLogging, Logger}
import org.slf4j.LoggerFactory
import pureconfig.ConfigSource
import pureconfig.error.ConfigReaderException

import com.thatdot.quine.app.config.{PersistenceAgentType, PersistenceBuilder, QuineConfig}
import com.thatdot.quine.app.routes.QuineAppRoutes
import com.thatdot.quine.compiler.cypher.{CypherStandingWiretap, registerUserDefinedProcedure}
import com.thatdot.quine.graph._
import com.thatdot.quine.persistor.ExceptionWrappingPersistenceAgent

object Main extends App with LazyLogging {

  private val statusLines =
    new StatusLines(
      // This name comes from quine's logging.conf
      Logger("thatdot.Interactive"),
      System.err
    )

  // Parse command line arguments.
  // On any failure, print messages and terminate process.
  val cmdArgs: CmdArgs = CmdArgs(args) match {
    case Right(cmdArgs) if cmdArgs.printVersion =>
      Console.err.println(s"Quine universal program version ${BuildInfo.version}")
      sys.exit(0)
    case Right(cmdArgs) => cmdArgs
    case Left(message) =>
      Console.err.println(message)
      sys.exit(1)
  }

  // If there's a recipe URL or file path, block and read it, apply substitutions, and fail fast.
  val recipe: Option[Recipe] = cmdArgs.recipe.map { (recipeIdentifyingString: String) =>
    Recipe.getAndSubstitute(recipeIdentifyingString, cmdArgs.recipeValues) valueOr { messages =>
      messages foreach Console.err.println
      sys.exit(1)
    }
  }

  // Parse config for Quine and apply command line overrides.
  val config: QuineConfig = {
    // Regular HOCON loading of options (from java properties and `conf` files)
    val withoutOverrides = ConfigSource.default.load[QuineConfig] valueOr { failures =>
      Console.err.println(new ConfigReaderException[QuineConfig](failures).getMessage)
      sys.exit(1)
    }
    // Override webserver options
    import QuineConfig.{webserverEnabledLens, webserverPortLens}
    val withPortOverride = cmdArgs.port.fold(withoutOverrides)(webserverPortLens.set(withoutOverrides))
    val withWebserverOverrides =
      if (cmdArgs.disableWebservice) withPortOverride else webserverEnabledLens.set(withPortOverride)(true)

    // Recipe overrides (unless --force-config command line flag is used)
    if (recipe.isDefined && !cmdArgs.forceConfig) {
      val tempDataFile: File = File.createTempFile("quine-", ".db")
      tempDataFile.delete()
      if (cmdArgs.deleteDataFile) {
        tempDataFile.deleteOnExit()
      } else {
        // Only print the data file name when NOT DELETING the temporary file
        statusLines.info(s"Using data path ${tempDataFile.getAbsolutePath}")
      }
      withWebserverOverrides.copy(
        store = PersistenceAgentType.RocksDb(
          filepath = tempDataFile
        )
      )
    } else withWebserverOverrides
  }

  // Optionally print a message on startup
  if (BuildInfo.startupMessage.nonEmpty) {
    statusLines.warn(BuildInfo.startupMessage)
  }

  logger.info {
    val maxHeapSize = sys.runtime.maxMemory match {
      case Long.MaxValue => "no max heap size"
      case maxBytes =>
        val maxGigaBytes = maxBytes.toDouble / 1024d / 1024d / 1024d
        NumberFormat.getInstance.format(maxGigaBytes) + "GiB max heap size"
    }
    val numCores = NumberFormat.getInstance.format(sys.runtime.availableProcessors.toLong)
    s"Running ${BuildInfo.version} with $numCores available cores and $maxHeapSize."
  }

  if (config.dumpConfig) {
    statusLines.info(config.loadedConfigHocon)
  }

  val timeout: Timeout = config.timeout

  config.metricsReporters.foreach(Metrics.addReporter(_, "quine"))
  Metrics.startReporters()

  val graph: GraphService =
    try Await
      .result(
        GraphService(
          persistor = system =>
            new ExceptionWrappingPersistenceAgent(
              PersistenceBuilder.build(config.store, config.persistence)(system),
              system.dispatcher
            ),
          idProvider = config.id.idProvider,
          shardCount = config.shardCount,
          inMemorySoftNodeLimit = config.inMemorySoftNodeLimit,
          inMemoryHardNodeLimit = config.inMemoryHardNodeLimit,
          effectOrder = config.persistence.effectOrder,
          declineSleepWhenWriteWithinMillis = config.declineSleepWhenWriteWithin.toMillis,
          declineSleepWhenAccessWithinMillis = config.declineSleepWhenAccessWithin.toMillis,
          maxCatchUpSleepMillis = config.maxCatchUpSleep.toMillis,
          labelsProperty = config.labelsProperty,
          edgeCollectionFactory = config.edgeIteration.edgeCollectionFactory,
          metricRegistry = Metrics,
          enableDebugMetrics = config.metrics.enableDebugMetrics
        ).flatMap(graph =>
          graph.persistor
            .syncVersion(
              "Quine app state",
              QuineApp.VersionKey,
              QuineApp.CurrentPersistenceVersion,
              () => QuineApp.quineAppIsEmpty(graph.persistor)
            )
            .map(_ => graph)(ExecutionContexts.parasitic)
        )(ExecutionContexts.parasitic),
        atMost = timeout.duration
      )
    catch {
      case NonFatal(err) =>
        statusLines.error("Unable to start graph", err)
        sys.exit(1)
    }

  implicit val system: ActorSystem = graph.system
  val ec: ExecutionContext = graph.shardDispatcherEC
  val appState = new QuineApp(graph)

  registerUserDefinedProcedure(new CypherStandingWiretap(appState.getStandingQueryId))

  // Warn if character encoding is unexpected
  if (Charset.defaultCharset != StandardCharsets.UTF_8) {
    statusLines.warn(
      s"System character encoding is ${Charset.defaultCharset} - did you mean to specify -Dfile.encoding=UTF-8?"
    )
  }

  statusLines.info("Graph is ready")

  // The web service is started unless it was disabled.
  val bindUrl: Option[URL] = Option.when(config.webserver.enabled)(config.webserver.toURL)
  val canonicalUrl: Option[URL] =
    Option.when(config.webserver.enabled)(config.webserverAdvertise.map(_.toURL)).flatten

  @volatile
  var recipeInterpreterTask: Option[Cancellable] = None

  def attemptAppLoad(): Unit =
    appState
      .load(timeout, config.shouldResumeIngest)
      .onComplete {
        case Success(()) =>
          recipeInterpreterTask = recipe.map(r =>
            RecipeInterpreter(statusLines, r, appState, graph, canonicalUrl)(
              graph.idProvider
            )
          )
        case Failure(cause) =>
          statusLines.warn(
            "Failed to load application state. This is most likely due to a failure " +
            "in the persistence backend",
            cause
          )
          system.scheduler.scheduleOnce(500.millis)(attemptAppLoad())(ec)
      }(ec)

  attemptAppLoad()

  bindUrl foreach { url =>
    // if a canonical URL is configured, use that for presentation (eg logging) purposes. Otherwise, infer
    // from the bind URL
    val resolvableUrl = canonicalUrl.getOrElse {
      // hack: if using the bindURL when host is "0.0.0.0" or "::" (INADDR_ANY and IN6ADDR_ANY's most common
      // serialized forms) present the URL as "localhost" to the user. This is necessary because while
      // INADDR_ANY as a source  address means "bind to all interfaces", it cannot necessarily be used as
      // a destination address
      val resolvableHost =
        if (Set("0.0.0.0", "::").contains(url.getHost)) "localhost" else url.getHost
      new java.net.URL(
        url.getProtocol,
        resolvableHost,
        url.getPort,
        url.getFile
      )
    }

    new QuineAppRoutes(graph, appState, config.loadedConfigJson, resolvableUrl, timeout)
      .bindWebServer(interface = url.getHost, port = url.getPort)
      .onComplete {
        case Success(binding) =>
          binding.addToCoordinatedShutdown(hardTerminationDeadline = 30.seconds)
          statusLines.info(s"Quine web server available at $resolvableUrl")
        case Failure(_) => // akka will have logged a stacktrace to the debug logger
      }(ec)
  }

  CoordinatedShutdown(system).addTask(CoordinatedShutdown.PhaseBeforeClusterShutdown, "Shutdown") { () =>
    statusLines.info("Quine is shutting down... ")
    try recipeInterpreterTask.foreach(_.cancel())
    catch {
      case NonFatal(e) =>
        statusLines.error("Graceful shutdown of Recipe interpreter encountered an error:", e)
    }
    implicit val ec = ExecutionContexts.parasitic
    for {
      _ <- appState.shutdown()
      _ <- graph.shutdown()
    } yield {
      statusLines.info("Shutdown complete.")
      Done
    }
  }

  CoordinatedShutdown(system).addTask(CoordinatedShutdown.PhaseActorSystemTerminate, "Cleanup of reporters") { () =>
    Metrics.stopReporters()
    LoggerFactory.getILoggerFactory match {
      case context: LoggerContext => context.stop()
      case _ => ()
    }
    Future.successful(Done)
  }

  // Block the main thread for as long as the ActorSystem is running.
  try Await.ready(system.whenTerminated, Duration.Inf)
  catch { case _: InterruptedException => () }
}
