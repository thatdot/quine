package com.thatdot.quine.app

import java.io.File
import java.nio.charset.{Charset, StandardCharsets}
import java.text.NumberFormat

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
import org.slf4j.LoggerFactory
import pureconfig.ConfigSource
import pureconfig.error.ConfigReaderException

import com.thatdot.quine.app.config.{PersistenceAgentType, PersistenceBuilder, QuineConfig, WebServerConfig}
import com.thatdot.quine.app.migrations.QuineMigrations
import com.thatdot.quine.app.routes.QuineAppRoutes
import com.thatdot.quine.graph._
import com.thatdot.quine.migrations.{MigrationError, MigrationVersion}
import com.thatdot.quine.util.Log._
import com.thatdot.quine.util.Log.implicits._

object Main extends App with LazySafeLogging {

  private val statusLines =
    new StatusLines(
      // This name comes from quine's logging.conf
      SafeLogger("thatdot.Interactive"),
      System.err,
    )

  // Warn if character encoding is unexpected
  if (Charset.defaultCharset != StandardCharsets.UTF_8) {
    statusLines.warn(
      log"System character encoding is ${Safe(Charset.defaultCharset)} - did you mean to specify -Dfile.encoding=UTF-8?",
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
        statusLines.info(log"Using data path ${Safe(tempDataFile.getAbsolutePath)}")
      }
      withWebserverOverrides.copy(
        store = PersistenceAgentType.RocksDb(
          filepath = tempDataFile,
        ),
      )
    } else withWebserverOverrides
  }
  implicit protected def logConfig: LogConfig = config.logConfig

  // Optionally print a message on startup
  if (BuildInfo.startupMessage.nonEmpty) {
    statusLines.warn(log"${Safe(BuildInfo.startupMessage)}")
  }

  logger.info {
    val maxHeapSize = sys.runtime.maxMemory match {
      case Long.MaxValue => "no max heap size"
      case maxBytes =>
        val maxGigaBytes = maxBytes.toDouble / 1024d / 1024d / 1024d
        NumberFormat.getInstance.format(maxGigaBytes) + "GiB max heap size"
    }
    val numCores = NumberFormat.getInstance.format(sys.runtime.availableProcessors.toLong)
    safe"Running ${Safe(BuildInfo.version)} with ${Safe(numCores)} available cores and ${Safe(maxHeapSize)}."
  }

  if (config.dumpConfig) {
    statusLines.info(log"${Safe(config.loadedConfigHocon)}")
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
              PersistenceBuilder.build(config.store, config.persistence)(Materializer.matFromSystem(system), logConfig)
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
          enableDebugMetrics = config.metrics.enableDebugMetrics,
        ).flatMap(graph =>
          graph.namespacePersistor
            .syncVersion(
              "Quine app state",
              QuineApp.VersionKey,
              QuineApp.CurrentPersistenceVersion,
              () => QuineApp.quineAppIsEmpty(graph.namespacePersistor),
            )
            .map(_ => graph)(ExecutionContext.parasitic),
        )(ExecutionContext.parasitic),
        atMost = timeout.duration,
      )
    catch {
      case NonFatal(err) =>
        statusLines.error(log"Unable to start graph", err)
        sys.exit(1)
    }

  implicit val system: ActorSystem = graph.system
  val ec: ExecutionContext = graph.shardDispatcherEC
  val quineApp = new QuineApp(graph)

  // Initialize the namespaces and apply migrations
  val hydrateAndMigrate: Future[Either[MigrationError, Unit]] = {
    val allMigrations = migrations.instances.all
    val GoalVersion: MigrationVersion = allMigrations.last.to
    val currentVersionFut = MigrationVersion
      .getFrom(graph.namespacePersistor)
      .map(_.getOrElse(MigrationVersion(0)))(ExecutionContext.parasitic)
    currentVersionFut.flatMap[Either[MigrationError, Unit]] {
      case GoalVersion =>
        // we are already at our goal version, so we can just load namespaces
        quineApp.restoreNonDefaultNamespacesFromMetaData(ec).map(Right(_))(ExecutionContext.parasitic)
      case versionWentBackwards if versionWentBackwards > GoalVersion =>
        // the version we pulled from the persistor is greater than the `to` of the final migration we're aware of
        Future.successful(Left(MigrationError.PreviousMigrationTooAdvanced(versionWentBackwards, GoalVersion)))
      case currentVersion =>
        // the found version indicates we need to run at least one migration
        // TODO figure out which Migration.Apply instances to run based on the needed Migrations and the product
        //  running the migrations. For now, with one migration, and in Quine's main, we know what to run
        require(
          currentVersion == MigrationVersion(0) && GoalVersion == MigrationVersion(1),
          s"Unexpected migration versions (current: $currentVersion, goal: $GoalVersion)",
        )
        val migrationApply = new QuineMigrations.ApplyMultipleValuesRewrite(
          graph.namespacePersistor,
          graph.getNamespaces.toSet,
        )

        quineApp
          .restoreNonDefaultNamespacesFromMetaData(ec)
          .flatMap { _ =>
            migrationApply.run()(graph.dispatchers)
          }(graph.nodeDispatcherEC)
          .flatMap {
            case err @ Left(_) => Future.successful(err)
            case Right(_) =>
              // the migration succeeded, so we can set the version to the `to` version of the migration
              MigrationVersion
                .set(graph.namespacePersistor, migrationApply.migration.to)
                .map(Right(_))(ExecutionContext.parasitic)
          }(ExecutionContext.parasitic)
    }(graph.nodeDispatcherEC)
  }
  // if there was a migration error, present it to the user then exit
  Await.result(hydrateAndMigrate, timeout.duration).left.foreach { error: MigrationError =>
    error match {
      case includeDiagnosticInfo: Throwable =>
        statusLines.error(
          log"Encountered a migration error during startup. Shutting down."
          withException includeDiagnosticInfo,
        )
      case opaque =>
        statusLines.error(
          log"Encountered a migration error during startup. Shutting down. Error: ${opaque.message}",
        )
    }
    sys.exit(1)
  }

  val loadDataFut: Future[Unit] = quineApp.loadAppData(timeout, config.shouldResumeIngest)
  Await.result(loadDataFut, timeout.duration * 2)

  statusLines.info(log"Graph is ready")

  // The web service is started unless it was disabled.
  val bindAndResolvableAddresses: Option[(WebServerConfig, Uri)] = Option.when(config.webserver.enabled) {
    import config.webserver
    // if a canonical URL is configured, use that for presentation (eg logging) purposes. Otherwise, infer
    // from the bind URL
    webserver -> config.webserverAdvertise.fold(webserver.asResolveableUrl)(
      _.overrideHostAndPort(webserver.asResolveableUrl),
    )
  }

  var recipeInterpreterTask: Option[Cancellable] = recipe.map { r =>

    val interpreter = RecipeInterpreter(statusLines, r, quineApp, graph, bindAndResolvableAddresses.map(_._2))(
      graph.idProvider,
    )
    interpreter.run(quineApp.thisMemberIdx)
    interpreter
  }

  bindAndResolvableAddresses foreach { case (bindAddress, resolvableUrl) =>
    new QuineAppRoutes(graph, quineApp, config, resolvableUrl, timeout, config.api2Enabled)(
      ExecutionContext.parasitic,
      logConfig,
    )
      .bindWebServer(bindAddress.address.asString, bindAddress.port.asInt, bindAddress.ssl)
      .onComplete {
        case Success(binding) =>
          binding.addToCoordinatedShutdown(hardTerminationDeadline = 30.seconds)
          statusLines.info(log"Quine web server available at ${Safe(resolvableUrl.toString)}")
          if (config.api2Enabled) {
            statusLines.info(log"Api v2 enabled")
          }
          // report telemetry only if the user has opted in.
          // This needs to be loaded after the webserver is started; if not, the initial telemetry
          // startup message may not get sent.
          if (config.helpMakeQuineBetter) {
            val iq = new ImproveQuine(
              service = "Quine",
              version = BuildInfo.version,
              persistor = config.store.label,
              app = Some(quineApp),
              recipeUsed = recipe.isDefined,
              recipeCanonicalName = if (recipe.isDefined) cmdArgs.recipe.flatMap(Recipe.getCanonicalName) else None,
            )
            iq.startTelemetry()
          }
        case Failure(_) => // pekko will have logged a stacktrace to the debug logger
      }(ec)
  }

  CoordinatedShutdown(system).addTask(CoordinatedShutdown.PhaseBeforeClusterShutdown, "Shutdown") { () =>
    statusLines.info(log"Quine is shutting down... ")
    try recipeInterpreterTask.foreach(_.cancel())
    catch {
      case NonFatal(e) =>
        statusLines.error(log"Graceful shutdown of Recipe interpreter encountered an error:", e)
    }
    implicit val ec = ExecutionContext.parasitic
    for {
      _ <- quineApp.shutdown()
      _ <- graph.shutdown()
    } yield {
      statusLines.info(log"Shutdown complete.")
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
