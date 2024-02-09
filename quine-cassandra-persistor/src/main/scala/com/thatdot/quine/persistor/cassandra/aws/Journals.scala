package com.thatdot.quine.persistor.cassandra.aws

import com.datastax.oss.driver.api.core.cql.SimpleStatement

import com.thatdot.quine.graph.NamespaceId
import com.thatdot.quine.persistor.cassandra.JournalsTableDefinition

class KeyspacesJournalsDefinition(namespace: NamespaceId) extends JournalsTableDefinition(namespace) {
  protected val selectAllQuineIds: SimpleStatement = select
    .column(quineIdColumn.name)
    .build
}
