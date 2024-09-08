package com.thatdot.quine.app.migrations

import scala.concurrent.Future

import com.thatdot.quine.migrations.{MigrationError, MigrationVersion}
import com.thatdot.quine.util.ComputeAndBlockingExecutionContext

/** A migration represents a need to change the state of the system from one version to the next.
  * Note that the migration itself may be applied differently by different products, so the typeclass
  * pattern is used to define how to apply a migration (see [[Migration.Apply]]).
  * This trait is itself defined in the least common "application" package, i.e., the Quine application
  * itself. Conceptually, it's close to belonging in quine-core, but as quine-core is supposed to be
  * completely unaware of external systems, and many/most migrations will be dealing with external systems,
  * the interface and utilities are defined in an application package instead.
  */
trait Migration {
  val from: MigrationVersion

  @deprecatedOverriding(
    "Are you sure you want to introduce a migration that skips versions? If so, suppress this warning",
    "1.7.0",
  )
  def to: MigrationVersion = MigrationVersion(from.version + 1)
}
object Migration {

  /** Typeclass for applying a migration. This is used to define how to apply a migration to a specific
    * product. The caller should ensure that `run` is only called when the current system version is at least
    * [[migration.from]].
    * Migrations should be idempotent, so that they can be rerun if necessary, for example, due to network
    * failures or races from multiple clustered application instances
    */
  trait Apply[M <: Migration] {
    val migration: M
    def run()(implicit ecs: ComputeAndBlockingExecutionContext): Future[Either[MigrationError, Unit]]
  }
}
