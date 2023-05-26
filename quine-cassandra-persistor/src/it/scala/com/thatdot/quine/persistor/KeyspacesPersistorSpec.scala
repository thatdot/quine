package com.thatdot.quine.persistor

import scala.concurrent.duration._
import com.datastax.oss.driver.api.core.ConsistencyLevel
import com.thatdot.quine.persistor.cassandra.aws.KeyspacesPersistor
import com.thatdot.quine.persistor.cassandra.support.CassandraStatementSettings

class KeyspacesPersistorSpec extends PersistenceAgentSpec {

  private val statementSettings = CassandraStatementSettings(ConsistencyLevel.LOCAL_QUORUM, 1.second)
  override def afterAll(): Unit = {
    super.afterAll()
    val _ = persistor.shutdown()
  }

  lazy val persistor: PersistenceAgent = new KeyspacesPersistor(
    PersistenceConfig(),
    keyspace = sys.env.getOrElse("CI_AKS_KEYSPACE", "blah"),
    awsRegion = None,
    readSettings = statementSettings,
    writeTimeout = 1.second,
    shouldCreateTables = true,
    shouldCreateKeyspace = true,
    metricRegistry = None,
    snapshotPartMaxSizeBytes = 1000
  )

  override val runnable: Boolean = true
}
