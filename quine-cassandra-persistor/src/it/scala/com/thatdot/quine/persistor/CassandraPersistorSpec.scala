package com.thatdot.quine.persistor

import java.net.{InetSocketAddress, Socket}
import java.time.Duration

import scala.util.{Failure, Success, Try}

import akka.actor.ActorSystem

import com.datastax.oss.driver.api.core.DefaultConsistencyLevel
import com.github.nosan.embedded.cassandra.{Cassandra, CassandraBuilder, WorkingDirectoryDestroyer}
import com.typesafe.scalalogging.LazyLogging
import org.scalatest.time.SpanSugar.convertIntToGrainOfTime

import com.thatdot.quine.persistor.cassandra.vanilla.CassandraPersistor

class CassandraPersistorSpec extends PersistenceAgentSpec {

  val cassandraWrapper: CassandraInstanceWrapper[CassandraPersistor] =
    new CassandraInstanceWrapper[CassandraPersistor]() {
      override def buildFromAddress(inetSocketAddress: Option[InetSocketAddress]): Option[CassandraPersistor] =
        inetSocketAddress map { addr =>
          new CassandraPersistor(
            PersistenceConfig(),
            keyspace = "quine",
            replicationFactor = 1,
            readConsistency = DefaultConsistencyLevel.LOCAL_QUORUM,
            writeConsistency = DefaultConsistencyLevel.LOCAL_QUORUM,
            endpoints = List(addr),
            localDatacenter = "datacenter1",
            writeTimeout = 10.seconds,
            readTimeout = 10.seconds,
            shouldCreateTables = true,
            shouldCreateKeyspace = true,
            metricRegistry = None,
            snapshotPartMaxSizeBytes = 1000
          )
        }
    }

  override def afterAll(): Unit = {
    super.afterAll()
    cassandraWrapper.stop()
  }

  lazy val persistor: PersistenceAgent = cassandraWrapper.instance getOrElse InMemoryPersistor.empty

  override def runnable: Boolean = persistor.isInstanceOf[CassandraPersistor]
}

/** Wrap a test instance of cassandra.
  *
  * - Attempts to use embedded cassandra
  * - If that fails, will use local cassandra if it is available at the standard local
  * address and port
  * - If that fails will default to InMemoryPersistor
  */
abstract class CassandraInstanceWrapper[T](implicit val system: ActorSystem) extends LazyLogging {

  private var embeddedCassandra: Cassandra = _

  /* Embedded Cassandra may fail to run for a variety of reasons:
   *
   *   - unsupported Java version
   *   - unsupported architecture
   */

  def launchEmbeddedCassanrda(): Try[Cassandra] = Try {
    val cassandra = new CassandraBuilder()
      .startupTimeout(Duration.ofMinutes(5))
      .addJvmOptions( // Options to hopefully enable tests on as many
        "-XX:+IgnoreUnrecognizedVMOptions",
        "--add-opens=java.base/java.io=ALL-UNNAMED",
        "--add-opens=java.base/java.util=ALL-UNNAMED",
        "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
        "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
        "--add-opens=java.base/java.nio=ALL-UNNAMED",
        "--add-opens=java.base/java.lang=ALL-UNNAMED",
        "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED"
      )
      .workingDirectoryDestroyer(WorkingDirectoryDestroyer.deleteAll()) // don't keep anything
      .build()
    cassandra.start()
    cassandra
  }

  /** Try to establish if there's a local cassandra available for testing. */
  private lazy val localCassandra: Boolean = Try {
    val s: Socket = new Socket("localhost", 9042)
    s.close()
  }.isSuccess

  /** Tests should run if Cassandra is available on localhost, or if embedded Cassandra could be started.
    */
  private lazy val runnableAddress: Option[InetSocketAddress] =
    if (localCassandra) {
      logger.warn(f"Using local Cassandra")
      Some(new InetSocketAddress("127.0.0.1", 9042))
    } else {
      launchEmbeddedCassanrda() match {
        case Success(cassandra) =>
          embeddedCassandra = cassandra
          val settings = cassandra.getSettings
          logger.warn(s"Using embedded cassandra settings: $settings")
          Some(new InetSocketAddress(settings.getAddress, settings.getPort))

        case Failure(exception) =>
          logger.warn("Found no local cassandra, and embedded cassandra failed to launch", exception)
          None
      }
    }

  def stop(): Unit = if (embeddedCassandra != null) embeddedCassandra.stop()

  def buildFromAddress(address: Option[InetSocketAddress]): Option[T]

  lazy val instance: Option[T] = buildFromAddress(runnableAddress)
}
