package com.thatdot.quine.app

import java.util.concurrent.{ExecutorService, ThreadFactory}

import akka.dispatch.{
  DefaultExecutorServiceConfigurator,
  DispatcherPrerequisites,
  ExecutorServiceConfigurator,
  ExecutorServiceFactory,
  ForkJoinExecutorConfigurator,
  ThreadPoolExecutorConfigurator
}

import com.codahale.metrics.InstrumentedExecutorService
import com.google.common.cache.{Cache, CacheBuilder}
import com.typesafe.config.{Config => TypesafeConfig, ConfigException, ConfigRenderOptions}
import com.typesafe.scalalogging.LazyLogging
import pureconfig.ConfigWriter

import com.thatdot.quine.graph.HostQuineMetrics

/** Morally, Metered Executors are more of a Quine construct (internal metering of internal properties) but because
  * MeteredExecutors depend on access to the same HostQuineMetrics instance that the application uses at runtime,
  * we must define these in Quine App.
  */
object MeteredExecutors extends LazyLogging {

  private val instrumentedExecutors: Cache[String, InstrumentedExecutorService] = CacheBuilder.newBuilder().build()

  sealed abstract class Configurator(
    config: TypesafeConfig,
    prerequisites: DispatcherPrerequisites,
    underlying: ExecutorServiceConfigurator,
    registry: HostQuineMetrics
  ) extends ExecutorServiceConfigurator(config, prerequisites) {
    logger.whenDebugEnabled {
      var verbose = false
      logger.whenTraceEnabled {
        verbose = true
      }
      logger.debug(
        s"Metered Configurator created with config read from ${config.origin()}: ${ConfigWriter[TypesafeConfig]
          .to(config)
          .render(
            ConfigRenderOptions.defaults().setComments(verbose).setOriginComments(false).setJson(false)
          )}"
      )
    }

    def createExecutorServiceFactory(id: String, threadFactory: ThreadFactory): ExecutorServiceFactory =
      new ExecutorServiceFactory {
        def createExecutorService: ExecutorService =
          // TODO consider making the cache sensitive to the provided threadFactory -- invalidating entries when
          // threadFactory changes so that the `underlying` delegate is always using the "latest" threadFactory
          instrumentedExecutors.get(
            id,
            () =>
              new InstrumentedExecutorService(
                underlying.createExecutorServiceFactory(id, threadFactory).createExecutorService,
                registry.metricRegistry,
                id
              )
          )
      }
  }

  /** merges config with one of its own keys -- akka's AbstractDispatcher "normally" passes the full `config` to a
    * custom Configurator, but it special cases akka's own configurators, instead passing them only a part of the config
    * based on some key -- this function returns a config which will default to the same behavior as
    * AbstractDispatcher's scoping, but fall back to akka's default but fall back to akka's special casing
    *
    * In effect, this allows using only a single config block for both the underlying configurator *and* the metering
    * wrapper itself, making it easier to switch between the two
    */
  private def mergeConfigWithUnderlying(config: TypesafeConfig, underlyingConfigKey: String): TypesafeConfig =
    config.withFallback(config.getConfig(underlyingConfigKey))

  def quineMetrics(config: TypesafeConfig): HostQuineMetrics = {
    val ConfigPath = "quine.metrics.enable-debug-metrics"
    val useEnhancedMetrics: Boolean =
      try config.getBoolean(ConfigPath)
      catch {
        case _: ConfigException.Missing => false
        case wrongType: ConfigException.WrongType =>
          logger.warn(s"Found invalid setting for boolean config key $ConfigPath", wrongType)
          false
      }

    HostQuineMetrics(
      useEnhancedMetrics,
      Metrics
    ) // INV the metrics instance here matches the one used by the app's Main
  }

  /** An Executor that delegates execution to an Akka [[ThreadPoolExecutorConfigurator]], wrapped in an
    * [[InstrumentedExecutorService]].
    *
    * @note this may used by adding a line within any akka "dispatcher" config block as follows:
    *       `executor = "com.thatdot.quine.app.MeteredExecutors$MeteredThreadPoolConfigurator"`.
    *       Options may still be passed to the underlying thread-pool-executor as normal
    * @see for metrics reported: <https://github.com/dropwizard/metrics/blob/00d1ca1a953be63c1490ddf052f65f2f0c3c45d3/metrics-core/src/main/java/com/codahale/metrics/InstrumentedExecutorService.java#L60-L75>
    */
  final class MeteredThreadPoolConfigurator(config: TypesafeConfig, prerequisites: DispatcherPrerequisites)
      extends Configurator(
        mergeConfigWithUnderlying(config, "thread-pool-executor"),
        prerequisites,
        new ThreadPoolExecutorConfigurator(mergeConfigWithUnderlying(config, "thread-pool-executor"), prerequisites),
        quineMetrics(config)
      )

  /** An Executor that delegates execution to an Akka [[ForkJoinExecutorConfigurator]], wrapped in an
    * [[InstrumentedExecutorService]].
    *
    * @note this may used by adding a line within any akka "dispatcher" config block as follows:
    *       `executor = "com.thatdot.quine.app.MeteredExecutors$MeteredForkJoinConfigurator"`.
    *       Options may still be passed to the underlying fork-join-executor as normal
    * @see for metrics reported: <https://github.com/dropwizard/metrics/blob/00d1ca1a953be63c1490ddf052f65f2f0c3c45d3/metrics-core/src/main/java/com/codahale/metrics/InstrumentedExecutorService.java#L77-L85>
    */
  final class MeteredForkJoinConfigurator(config: TypesafeConfig, prerequisites: DispatcherPrerequisites)
      extends Configurator(
        mergeConfigWithUnderlying(config, "fork-join-executor"),
        prerequisites,
        new ForkJoinExecutorConfigurator(
          mergeConfigWithUnderlying(config, "fork-join-executor"),
          prerequisites
        ),
        quineMetrics(config)
      )

  /** An Executor that delegates execution to an Akka [[DefaultExecutorServiceConfigurator]], wrapped in an
    * [[InstrumentedExecutorService]].
    *
    * @note this may used by adding a line within any akka "dispatcher" config block as follows:
    *       `executor = "com.thatdot.quine.app.MeteredExecutors$MeteredDefaultConfigurator"`.
    *       Options may still be passed to the underlying default-executor as normal, except that
    *       default-executor.fallback is ignored in favor of MeteredForkJoin (chosen because the default value as of akka 2.6.16 was fork-join-executor)
    */
  final class MeteredDefaultConfigurator(config: TypesafeConfig, prerequisites: DispatcherPrerequisites)
      extends Configurator(
        mergeConfigWithUnderlying(config, "default-executor"),
        prerequisites, {
          if (prerequisites.defaultExecutionContext.isEmpty)
            logger.warn(
              "The default akka executor should only be metered in conjunction with an explicit default executor" +
              " (this may be set at akka.actor.default-dispatcher.default-executor). Defaulting to fork-join"
            )
          new DefaultExecutorServiceConfigurator(
            mergeConfigWithUnderlying(config, "default-executor"),
            prerequisites,
            new MeteredForkJoinConfigurator(
              config,
              prerequisites
            )
          )
        },
        quineMetrics(config)
      )

  // AffinityPoolConfigurator is private and @ApiMayChange as of 2.6.16, so there is no MeteredAffinityPoolConfigurator
}
