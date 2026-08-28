package com.thatdot.quine.app.migrations.instances

import com.thatdot.quine.app.migrations.Migration
import com.thatdot.quine.migrations.MigrationVersion

/** Rename the persisted global metadata key holding the saved graph-feed list from the legacy
  * `tap_queries` to `graph_feeds`.
  *
  * The feature was renamed "tap queries" -> "graph feeds" in the source; the persisted value shape
  * is unchanged (the stored records gained only an additive field with a default), so this migration
  * simply moves the stored bytes from the old key to the new one. It applies identically in OSS and
  * Enterprise, since both wrote the value under the same global metadata key.
  */
object GraphFeedsRename extends Migration {
  val from: MigrationVersion = MigrationVersion(2)
}
