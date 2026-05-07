package com.thatdot.quine.app.migrations.instances

import com.thatdot.quine.app.migrations.Migration
import com.thatdot.quine.migrations.MigrationVersion

/** Rename the default namespace from "default" to "quine".
  *
  * In Enterprise, this migration checks that no user-created namespace named "quine" already exists
  * (which would collide with the new default) and defensively renames any metadata keys suffixed
  * with "-default" to "-quine".
  *
  * In OSS and Novelty this is a no-op: OSS cannot have user-created namespaces, and the default
  * namespace storage is already unprefixed.
  */
object DefaultNamespaceRename extends Migration {
  val from: MigrationVersion = MigrationVersion(1)
}
