package com.thatdot.quine.persistor

import java.net.InetSocketAddress
import java.time.Duration

import scala.concurrent.duration.DurationInt
import scala.util.Try

import com.datastax.oss.driver.api.core.DefaultConsistencyLevel
import com.github.nosan.embedded.cassandra.{Cassandra, CassandraBuilder, WorkingDirectoryDestroyer}

import com.thatdot.quine.persistor.cassandra.vanilla.CassandraPersistor

class CassandraPersistorSpec extends PersistenceAgentSpec {

  /* Embedded Cassandra may fail to run for a variety of reasons:
   *
   *   - unsupported Java version
   *   - unsupported architecture
   */
  final lazy val cassandraTry: Try[Cassandra] = Try {
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

  /** Tests should run if Cassandra could be started or if in CI (in CI, we want
    * to know if tests couldn't run).
    */
  override lazy val runnable: Boolean = sys.env.contains("CI") || cassandraTry.isSuccess

  override def afterAll(): Unit = {
    super.afterAll()
    cassandraTry.foreach(_.stop())
  }

  lazy val persistor: PersistenceAgent =
    if (runnable) {
      new CassandraPersistor(
        PersistenceConfig(),
        keyspace = "quine",
        replicationFactor = 1,
        readConsistency = DefaultConsistencyLevel.LOCAL_QUORUM,
        writeConsistency = DefaultConsistencyLevel.LOCAL_QUORUM,
        endpoints = {
          val settings = cassandraTry.get.getSettings
          List(new InetSocketAddress(settings.getAddress, settings.getPort))
        },
        localDatacenter = "datacenter1",
        insertTimeout = 10.seconds,
        selectTimeout = 10.seconds,
        shouldCreateTables = true,
        shouldCreateKeyspace = true,
        metricRegistry = None,
        snapshotPartMaxSizeBytes = 1000
      )
    } else {
      EmptyPersistor
    }
}
