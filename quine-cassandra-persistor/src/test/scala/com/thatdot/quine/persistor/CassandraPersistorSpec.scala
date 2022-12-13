package com.thatdot.quine.persistor

import java.net.{InetSocketAddress, Socket}
import java.time.Duration

import scala.util.Try

import akka.actor.ActorSystem

import com.datastax.oss.driver.api.core.DefaultConsistencyLevel
import com.github.nosan.embedded.cassandra.{Cassandra, CassandraBuilder, WorkingDirectoryDestroyer}
import com.typesafe.scalalogging.LazyLogging
import org.scalatest.time.SpanSugar.convertIntToGrainOfTime

import com.thatdot.quine.persistor.cassandra.vanilla.CassandraPersistor

class CassandraPersistorSpec extends PersistenceAgentSpec {

  val cassandraInstance: CassandraTestInstance[PersistenceAgent] = new CassandraTestInstance[PersistenceAgent]() {
    override def buildFromAddress(inetSocketAddress: Option[InetSocketAddress]): PersistenceAgent =
      inetSocketAddress match {
        case Some(addr) =>
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

        case _ =>
          logger.warn("Running InMemoryPersistor instead of Cassandra because it was not runnable.")
          InMemoryPersistor.empty
      }

  }

  override def afterAll(): Unit = {
    super.afterAll()
    cassandraInstance.stop()
  }

  lazy val persistor: PersistenceAgent = cassandraInstance.persistor

}

/** Wrap a test instance of cassandra.
  *
  * - Attempts to use embedded cassandra
  * - If that fails, will use local cassandra if it is available at the standard local
  * address and port
  * - If that fails will default to InMemoryPersistor
  */
abstract class CassandraTestInstance[T](implicit val system: ActorSystem) extends LazyLogging {

  /* Embedded Cassandra may fail to run for a variety of reasons:
   *
   *   - unsupported Java version
   *   - unsupported architecture
   */

  final lazy val embeddedCassandraTry: Try[Cassandra] = Try {
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

  def buildFromAddress(maybeAddress: Option[InetSocketAddress]): T

  /** Try to establish if there's a local cassandra available for testing. */
  private lazy val localCassandra: Boolean =
    try {
      val s: Socket = new Socket("localhost", 9042)
      s.close()
      true
    } catch {
      case _: Exception => false
    }

  /** Tests should run if Cassandra could be started or if in CI (in CI, we want
    * to know if tests couldn't run).
    */
  private lazy val runnableAddress: Option[InetSocketAddress] =
    if (sys.env.contains("CI") || embeddedCassandraTry.isSuccess) {

      val settings = embeddedCassandraTry.get.getSettings
      logger.warn(f"Using embedded cassandra settings: $settings")
      Some(new InetSocketAddress(settings.getAddress, settings.getPort))
    } else if (localCassandra) {
      logger.warn(f"Using local Cassandra")
      Some(new InetSocketAddress("127.0.0.1", 9042))
    } else {
      None
    }

  def stop(): Unit = embeddedCassandraTry.foreach(_.stop())

  lazy val persistor: T = buildFromAddress(runnableAddress)

}
