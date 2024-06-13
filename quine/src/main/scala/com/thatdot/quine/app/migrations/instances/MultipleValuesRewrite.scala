package com.thatdot.quine.app.migrations.instances

import com.thatdot.quine.app.migrations.Migration
import com.thatdot.quine.migrations.MigrationVersion

/** The MultipleValues rewrite introduced in Quine 1.7.0
  */
object MultipleValuesRewrite extends Migration {
  val from: MigrationVersion = MigrationVersion(0)
}
