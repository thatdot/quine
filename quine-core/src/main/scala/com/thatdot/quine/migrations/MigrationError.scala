package com.thatdot.quine.migrations

sealed trait MigrationError {
  def message: String
}

// utility mixin for Exception-based errors
sealed private[migrations] trait ExceptionMigrationError extends MigrationError { self: Exception =>
  def message: String = self.getMessage
}

object MigrationError {
  case class UserInterventionRequired(message: String) extends MigrationError

  class PersistorError(err: Throwable)
      extends Exception("Persistence error during migration application", err)
      with ExceptionMigrationError

  case class PreviousMigrationTooAdvanced(foundVersion: MigrationVersion, expectedMaxVersion: MigrationVersion)
      extends MigrationError {
    val message: String =
      s"Migration version is out of order: $foundVersion is beyond the greatest-known version: $expectedMaxVersion"
  }
}
