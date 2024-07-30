package com.thatdot.quine.migrations

import com.thatdot.quine.util.Log.OnlySafeStringInterpolator

sealed trait MigrationError {
  def message: String
}

// utility mixin for Exception-based errors
sealed private[migrations] trait ExceptionMigrationError extends MigrationError { self: Exception =>
  def message: String = self.getMessage
}

object MigrationError {
  class UserInterventionRequired private (val message: String) extends MigrationError {
    override def toString(): String = s"UserInterventionRequired($message)"
    override def hashCode(): Int = message.hashCode()
    override def equals(obj: Any): Boolean = obj match {
      case error: UserInterventionRequired => message == error.message
      case _ => false
    }
  }
  object UserInterventionRequired {
    def apply(message: OnlySafeStringInterpolator) = new UserInterventionRequired(message.safeString())
    def unapply(error: UserInterventionRequired): Option[String] = Some(error.message)
  }

  class PersistorError(err: Throwable)
      extends Exception("Persistence error during migration application", err)
      with ExceptionMigrationError

  case class PreviousMigrationTooAdvanced(foundVersion: MigrationVersion, expectedMaxVersion: MigrationVersion)
      extends MigrationError {
    val message: String =
      s"Migration version is out of order: $foundVersion is beyond the greatest-known version: $expectedMaxVersion"
  }
}
