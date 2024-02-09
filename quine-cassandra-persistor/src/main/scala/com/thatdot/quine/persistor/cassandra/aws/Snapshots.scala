package com.thatdot.quine.persistor.cassandra.aws

import com.datastax.oss.driver.api.core.cql.SimpleStatement

import com.thatdot.quine.graph.NamespaceId
import com.thatdot.quine.persistor.cassandra.SnapshotsTableDefinition

class KeyspacesSnapshotsDefinition(namespace: NamespaceId) extends SnapshotsTableDefinition(namespace) {
  protected val selectAllQuineIds: SimpleStatement = select.column(quineIdColumn.name).build
}
