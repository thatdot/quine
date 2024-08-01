package com.thatdot.quine.persistor.cassandra.support

import scala.concurrent.duration.FiniteDuration

import com.datastax.oss.driver.api.core.ConsistencyLevel
import com.datastax.oss.driver.api.core.cql.Statement

// to be applied to a statement
case class CassandraStatementSettings(consistency: ConsistencyLevel, timeout: FiniteDuration) {
  import scala.jdk.javaapi.DurationConverters.toJava
  def apply[SelfT <: Statement[SelfT]](statement: SelfT): SelfT =
    statement.setConsistencyLevel(consistency).setTimeout(toJava(timeout))
}
