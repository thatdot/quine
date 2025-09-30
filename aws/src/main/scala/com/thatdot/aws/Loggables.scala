package com.thatdot.aws

import org.polyvariant.sttp.oauth2.Secret

import com.thatdot.common.logging.Log.AlwaysSafeLoggable

object Loggables {
  object implicits {

    /** This manner of logging secret information relies on the `Secret`
      * type's own obfuscation rather than our usual redaction mechanism.
      *
      * If we need to log the plaintext value or use other means of redaction,
      * the `value` accessor of `Secret` returns the un-obfuscated plaintext.
      */
    implicit val logSecretStrings: AlwaysSafeLoggable[Secret[String]] = _.toString
  }
}
