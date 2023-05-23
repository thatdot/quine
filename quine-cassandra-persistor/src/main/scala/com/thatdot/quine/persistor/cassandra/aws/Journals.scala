package com.thatdot.quine.persistor.cassandra.aws

import com.datastax.oss.driver.api.core.cql.SimpleStatement

import com.thatdot.quine.persistor.cassandra.JournalsTableDefinition

object Journals extends JournalsTableDefinition {
  protected val selectAllQuineIds: SimpleStatement = select
    .column(quineIdColumn.name)
    .build
}
