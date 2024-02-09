package com.thatdot.quine.persistor.cassandra.vanilla

import com.datastax.oss.driver.api.core.cql.SimpleStatement

import com.thatdot.quine.graph.NamespaceId
import com.thatdot.quine.persistor.cassandra.SnapshotsTableDefinition

class SnapshotsDefinition(namespace: NamespaceId) extends SnapshotsTableDefinition(namespace) {
  protected val selectAllQuineIds: SimpleStatement = select.distinct.column(quineIdColumn.name).build
}
