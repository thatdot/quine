package com.thatdot.quine.app

import java.io.File
import java.nio.charset.{Charset, StandardCharsets}
import java.text.NumberFormat

import scala.compat.ExecutionContexts
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext}
import scala.util.control.NonFatal
import scala.util.{Failure, Success}

import akka.actor.{ActorSystem, Cancellable}
import akka.util.Timeout

import ch.qos.logback.classic.LoggerContext
import com.typesafe.scalalogging.{LazyLogging, Logger}
import org.slf4j.LoggerFactory

import com.thatdot.quine.app.config.PersistenceAgentType
import com.thatdot.quine.app.routes.QuineAppRoutes
import com.thatdot.quine.compiler.cypher.{CypherStandingWiretap, registerUserDefinedProcedure}
import com.thatdot.quine.graph._
import com.thatdot.quine.persistor.{ExceptionWrappingPersistenceAgent, PersistenceConfig}

object Main extends App with LazyLogging {

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
    Recipe.getAndSubstitute(recipeIdentifyingString, cmdArgs.recipeValues) match {
      case Left(messages) =>
        messages.foreach(l => Console.err.println(l))
        sys.exit(1)
      case Right(recipe) => recipe
    }
  }

  // specifies that logback configuration will be loaded from the "logging.quine" Typesafe Config scope
  sys.props("logback-root") = "logging.quine"

  private val statusLines =
    new StatusLines(
      // This name comes from quine's logging.conf
      Logger("thatdot.Interactive"),
      System.err
    )

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

  // When running a recipe, overwrite some configuration values
  // (unless --force-config command line flag is used)
  private val config: Config.QuineConfig = {
    val rawConfig = Config.config
    if (recipe.isDefined && !cmdArgs.forceConfig) {
      val tempDataFile: File = File.createTempFile("quine-", ".db")
      tempDataFile.delete()
      if (cmdArgs.deleteDataFile) {
        tempDataFile.deleteOnExit()
      } else {
        // Only print the data file name when NOT DELETING the temporary file
        statusLines.info(s"Using data path ${tempDataFile.getAbsolutePath}")
      }
      rawConfig.copy(
        webserver = rawConfig.webserver.copy(
          port = cmdArgs.port.getOrElse(rawConfig.webserver.port)
        ),
        store = PersistenceAgentType.RocksDb(
          filepath = tempDataFile
        ),
        persistence = PersistenceConfig(
          journalEnabled = false,
          snapshotSingleton = true
        )
      )
    } else rawConfig
  }

  if (config.dumpConfig) {
    statusLines.info(Config.loadedConfigHocon)
  }

  val timeout: Timeout = config.timeout

  config.metricsReporters.foreach(Metrics.addReporter(_, "quine"))
  Metrics.startReporters()

  val graph: GraphService =
    try {
      implicit val ec: ExecutionContext = ExecutionContexts.parasitic
      Await
        .result(
          for {
            graph <- GraphService(
              persistor = system =>
                new ExceptionWrappingPersistenceAgent(config.store.persistor(config.persistence)(system))(
                  system.dispatcher
                ),
              idProvider = config.id.idProvider,
              shardCount = config.shardCount,
              inMemorySoftNodeLimit = config.inMemorySoftNodeLimit,
              inMemoryHardNodeLimit = config.inMemoryHardNodeLimit,
              declineSleepWhenWriteWithinMillis = config.declineSleepWhenWriteWithin.toMillis,
              declineSleepWhenAccessWithinMillis = config.declineSleepWhenAccessWithin.toMillis,
              labelsProperty = Symbol(config.labelsProperty),
              edgeCollectionFactory = config.edgeIteration.edgeCollectionFactory,
              metricRegistry = Metrics
            )
            _ <- graph.persistor.syncVersion(
              "Quine app state",
              QuineApp.VersionKey,
              QuineApp.CurrentPersistenceVersion,
              () => QuineApp.quineAppIsEmpty(graph.persistor)
            )
          } yield graph,
          timeout.duration
        )
    } catch {
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

  // Inform when the graph is ready
  statusLines.info("Graph is ready!")

  // The web service is started when asked for by command line arguments,
  // or when no command arguments are specified.
  val quineWebserverUrl: Option[String] = if (!cmdArgs.disableWebservice) {
    Some(s"http://${config.webserver.address}:${config.webserver.port}")
  } else {
    None
  }

  @volatile
  var recipeInterpreterTask: Option[Cancellable] = None

  def attemptAppLoad(): Unit =
    appState
      .load(timeout, config.shouldResumeIngest)
      .onComplete {
        case Success(()) =>
          statusLines.info("Application state loaded.")
          recipeInterpreterTask = recipe.map(r =>
            RecipeInterpreter(statusLines, r, appState, graph, quineWebserverUrl)(
              system.dispatcher,
              graph.idProvider
            )
          )
        case Failure(cause) =>
          statusLines.warn(
            "Failed to load application state. This is most likely due to a failure" +
            "in the persistence backend",
            cause
          )
          system.scheduler.scheduleOnce(500.millis)(attemptAppLoad())(ec)
      }(ec)

  attemptAppLoad()

  quineWebserverUrl foreach { url =>
    new QuineAppRoutes(graph, appState, ec, timeout)
      .bindWebServer(interface = config.webserver.address, port = config.webserver.port)
      .onComplete {
        case Success(_) => statusLines.info(s"Quine app web server available at $url")
        case Failure(_) => // akka will have logged a stacktrace to the debug logger
      }(ec)
  }

  sys.addShutdownHook {
    statusLines.info("Quine is shutting down... ")
    try recipeInterpreterTask.foreach(_.cancel())
    catch {
      case NonFatal(e) =>
        statusLines.error("Graceful shutdown of Recipe interpreter encountered an error:", e)
    }
    try {
      Await.result(appState.shutdown(), timeout.duration)
      Metrics.stopReporters()
    } catch {
      case NonFatal(e) =>
        statusLines.error("Graceful shutdown of Quine App encountered an error:", e)
    }
    try Await.result(graph.shutdown(), timeout.duration)
    catch {
      case NonFatal(e) =>
        statusLines.error(s"Graceful shutdown of Quine encountered an error", e)
    }
    statusLines.info("Shutdown complete.")
    LoggerFactory.getILoggerFactory match {
      case context: LoggerContext => context.stop()
      case _ => ()
    }
  }

  // Block the main thread for as long as the ActorSystem is running.
  try Await.ready(system.whenTerminated, Duration.Inf)
  catch { case _: InterruptedException => () }
}
