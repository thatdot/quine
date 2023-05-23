package com.thatdot.quine.persistor.cassandra.aws

import com.datastax.oss.driver.api.core.cql.SimpleStatement

import com.thatdot.quine.persistor.cassandra.SnapshotsTableDefinition

object Snapshots extends SnapshotsTableDefinition {
  protected val selectAllQuineIds: SimpleStatement = select.column(quineIdColumn.name).build
}
