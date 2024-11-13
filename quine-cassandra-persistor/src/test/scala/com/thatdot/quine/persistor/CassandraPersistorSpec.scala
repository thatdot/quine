package com.thatdot.quine.persistor

import java.net.{InetSocketAddress, Socket}
import java.time.Duration

import scala.concurrent.Await
import scala.jdk.CollectionConverters._
import scala.util.Using
import scala.util.control.NonFatal

import org.apache.pekko.actor.ActorSystem

import com.datastax.oss.driver.api.core.ConsistencyLevel
import com.github.nosan.embedded.cassandra.{Cassandra, CassandraBuilder, Settings, WorkingDirectoryDestroyer}
import org.scalatest.time.SpanSugar.convertIntToGrainOfTime

import com.thatdot.quine.persistor.cassandra
import com.thatdot.quine.persistor.cassandra.support.CassandraStatementSettings
import com.thatdot.quine.persistor.cassandra.vanilla.PrimeCassandraPersistor
import com.thatdot.quine.test.tags.IntegrationTest
import com.thatdot.quine.util.Log._
import com.thatdot.quine.util.Log.implicits._

@IntegrationTest
class CassandraPersistorSpec()(implicit val logConfig: LogConfig) extends PersistenceAgentSpec {

  val statementSettings: CassandraStatementSettings = CassandraStatementSettings(ConsistencyLevel.ONE, 1.second)
  val cassandraWrapper: CassandraInstanceWrapper[PrimeCassandraPersistor] =
    new CassandraInstanceWrapper[PrimeCassandraPersistor](inetSocketAddress =>
      Await.result(
        PrimeCassandraPersistor.create(
          PersistenceConfig(),
          bloomFilterSize = None,
          endpoints = List(inetSocketAddress),
          localDatacenter = "datacenter1",
          replicationFactor = 1,
          keyspace = "quine",
          shouldCreateKeyspace = true,
          shouldCreateTables = true,
          readSettings = statementSettings,
          writeSettings = statementSettings,
          snapshotPartMaxSizeBytes = 1000,
          metricRegistry = None,
        ),
        12.seconds,
      ),
    )

  override def afterAll(): Unit = {
    super.afterAll()
    cassandraWrapper.stop()
  }

  lazy val persistor: PrimePersistor = cassandraWrapper.instance

  override def runnable: Boolean = true
}

/** Wrap a test instance of cassandra.
  *
  * - Attempts to use embedded cassandra
  * - If that fails, will use local cassandra if it is available at the standard local
  * address and port
  * - If that fails will default to InMemoryPersistor
  */
class CassandraInstanceWrapper[T <: cassandra.PrimeCassandraPersistor](buildFromAddress: InetSocketAddress => T)(
  implicit
  val system: ActorSystem,
  protected val logConfig: LogConfig,
) extends LazySafeLogging {

  private var embeddedCassandra: Cassandra = _

  /* Embedded Cassandra may fail to run for a variety of reasons:
   *
   *   - unsupported Java version
   *   - unsupported architecture
   */

  // Extra module flags to enable embedded Cassandra to run on more recent JVM versions
  // Still doesn't work in Java 19 or later due to the removal of SecurityManager
  private val extraJavaModules = List(
    "java.io",
    "java.util",
    "java.util.concurrent",
    "java.util.concurrent.atomic",
    "java.nio",
    "java.lang",
    "sun.nio.ch",
  )

  private def addOpensArg(pkg: String): String = s"--add-opens=java.base/$pkg=ALL-UNNAMED"

  private def launchEmbeddedCassanrda(): Cassandra = {
    val cassandra = new CassandraBuilder()
      .startupTimeout(Duration.ofMinutes(5))
      .addJvmOptions("-XX:+IgnoreUnrecognizedVMOptions")
      .addJvmOptions(extraJavaModules.map(addOpensArg).asJava)
      .workingDirectoryDestroyer(WorkingDirectoryDestroyer.deleteAll()) // don't keep anything
      .build()
    cassandra.start()
    cassandra
  }

  /** Try to establish if there's a local cassandra available for testing. */
  private lazy val localCassandra: Option[InetSocketAddress] = Using(new Socket()) { s =>
    val localAddr = new InetSocketAddress("127.0.0.1", 9042)
    s.connect(localAddr)
    localAddr
  }.toOption

  private def addressFromEmbeddedCassandra(settings: Settings): InetSocketAddress =
    new InetSocketAddress(settings.getAddress, settings.getPort)

  /** Tests should run if Cassandra is available on localhost, or if embedded Cassandra could be started.
    */
  private lazy val runnableAddress: InetSocketAddress = localCassandra getOrElse {
    try {
      embeddedCassandra = launchEmbeddedCassanrda()
      val address = addressFromEmbeddedCassandra(embeddedCassandra.getSettings)
      logger.warn(log"Using embedded cassandra at: ${Safe(address)}")
      address
    } catch {
      case NonFatal(exception) =>
        logger.warn(log"Found no local cassandra, and embedded cassandra failed to launch" withException exception)
        throw exception
    }
  }

  def stop(): Unit = {
    instance.shutdown()
    if (embeddedCassandra != null) embeddedCassandra.stop()
  }

  lazy val instance: T = buildFromAddress(runnableAddress)
}
