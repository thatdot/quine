package com.thatdot.quine.app

import java.io.File
import java.nio.charset.{Charset, StandardCharsets}
import java.text.NumberFormat

import scala.collection.compat._
import scala.compat.ExecutionContexts
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.control.NonFatal
import scala.util.{Failure, Success}

import org.apache.pekko.Done
import org.apache.pekko.actor.{ActorSystem, Cancellable, CoordinatedShutdown}
import org.apache.pekko.http.scaladsl.model.Uri
import org.apache.pekko.stream.Materializer
import org.apache.pekko.util.Timeout

import cats.syntax.either._
import ch.qos.logback.classic.LoggerContext
import com.typesafe.scalalogging.{LazyLogging, Logger}
import org.slf4j.LoggerFactory
import pureconfig.ConfigSource
import pureconfig.error.ConfigReaderException

import com.thatdot.quine.app.config.{PersistenceAgentType, PersistenceBuilder, QuineConfig, WebServerConfig}
import com.thatdot.quine.app.routes.QuineAppRoutes
import com.thatdot.quine.compiler.cypher.{CypherStandingWiretap, registerUserDefinedProcedure}
import com.thatdot.quine.graph._
import com.thatdot.quine.persistor.{ExceptionWrappingPersistenceAgent, PrimePersistor}

object Main extends App with LazyLogging {

  private val statusLines =
    new StatusLines(
      // This name comes from quine's logging.conf
      Logger("thatdot.Interactive"),
      System.err
    )

  // Warn if character encoding is unexpected
  if (Charset.defaultCharset != StandardCharsets.UTF_8) {
    statusLines.warn(
      s"System character encoding is ${Charset.defaultCharset} - did you mean to specify -Dfile.encoding=UTF-8?"
    )
  }

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
          persistorMaker = system => {
            val persistor =
              PersistenceBuilder.build(config.store, config.persistence)(Materializer.matFromSystem(system))
            persistor.initializeOnce // Initialize the default namespace
            persistor
          },
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
          graph.namespacePersistor
            .syncVersion(
              "Quine app state",
              QuineApp.VersionKey,
              QuineApp.CurrentPersistenceVersion,
              () => QuineApp.quineAppIsEmpty(graph.namespacePersistor)
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
  val quineApp = new QuineApp(graph)

  quineApp.restoreNonDefaultNamespacesFromMetaData(ec, timeout)

  val loadDataFut: Future[Unit] = quineApp.loadAppData(timeout, config.shouldResumeIngest)
  Await.result(loadDataFut, timeout.duration * 2)

  var recipeInterpreterTask: Option[Cancellable] = recipe.map(r =>
    RecipeInterpreter(statusLines, r, quineApp, graph, bindAndResolvableAddresses.map(_._2))(
      graph.idProvider
    )
  )

  registerUserDefinedProcedure(
    new CypherStandingWiretap((queryName, namespace) => quineApp.getStandingQueryId(queryName, namespace))
  )

  statusLines.info("Graph is ready")

  // The web service is started unless it was disabled.
  val bindAndResolvableAddresses: Option[(WebServerConfig, Uri)] = Option.when(config.webserver.enabled) {
    import config.webserver
    // if a canonical URL is configured, use that for presentation (eg logging) purposes. Otherwise, infer
    // from the bind URL
    webserver -> config.webserverAdvertise.fold(webserver.asResolveableUrl)(
      _.overrideHostAndPort(webserver.asResolveableUrl)
    )
  }

  private val improveQuine = ImproveQuine(
    config.helpMakeQuineBetter,
    BuildInfo.version,
    Uri("https://improve.quine.io/event"),
    system
  )
  improveQuine.started()

  bindAndResolvableAddresses foreach { case (bindAddress, resolvableUrl) =>
    new QuineAppRoutes(graph, quineApp, config.loadedConfigJson, resolvableUrl, timeout)
      .bindWebServer(bindAddress.address.asString, bindAddress.port.asInt, bindAddress.ssl)
      .onComplete {
        case Success(binding) =>
          binding.addToCoordinatedShutdown(hardTerminationDeadline = 30.seconds)
          statusLines.info(s"Quine web server available at $resolvableUrl")
        case Failure(_) => // pekko will have logged a stacktrace to the debug logger
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
      _ <- quineApp.shutdown()
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
