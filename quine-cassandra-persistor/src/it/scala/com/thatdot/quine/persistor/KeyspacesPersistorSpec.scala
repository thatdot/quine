package com.thatdot.quine.persistor

import scala.concurrent.duration._
import com.datastax.oss.driver.api.core.ConsistencyLevel
import com.thatdot.quine.persistor.cassandra.aws.PrimeKeyspacesPersistor
import com.thatdot.quine.persistor.cassandra.support.CassandraStatementSettings

import scala.concurrent.Await

class KeyspacesPersistorSpec extends PersistenceAgentSpec {

  private val statementSettings = CassandraStatementSettings(ConsistencyLevel.LOCAL_QUORUM, 1.second)
  override def afterAll(): Unit = {
    super.afterAll()
    val _ = persistor.shutdown()
  }

  // Skip Purge Namespace Test, it is currently non-functional on AWS Keyspaces
  override def runPurgeNamespaceTest: Boolean = false

  lazy val persistor: PrimePersistor = Await.result(
    PrimeKeyspacesPersistor.create(
      PersistenceConfig(),
      bloomFilterSize = None,
      keyspace = sys.env.getOrElse("CI_AKS_KEYSPACE", "blah"),
      awsRegion = None,
      awsRoleArn = None,
      readSettings = statementSettings,
      writeTimeout = 1.second,
      shouldCreateKeyspace = true,
      shouldCreateTables = true,
      metricRegistry = None,
      snapshotPartMaxSizeBytes = 1000
    ),
    38.seconds
  )

  override val runnable: Boolean = true
}
