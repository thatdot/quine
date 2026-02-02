package com.thatdot.quine.app.config

import java.io.File

/** Persistence builder instance for Quine.
  *
  * Uses Quine-specific defaults:
  *   - Uses "quine" as the default Cassandra keyspace name
  *   - Uses "quine.db" as the default RocksDB file path
  *   - ClickHouse throws an error (Enterprise-only feature)
  */
object QuinePersistenceBuilder {

  val instance: PersistenceBuilder = PersistenceBuilder(
    defaultKeyspace = "quine",
    defaultRocksDbFilepath = new File("quine.db"),
    buildClickHouse = { (_, _, _, _) =>
      throw new IllegalArgumentException(
        "ClickHouse is not available in Quine. If you are interested in using ClickHouse, please contact us to discuss upgrading to Quine Enterprise.",
      )
    },
  )
}
