package com.thatdot.quine.app.model.ingest2.sources

import com.thatdot.common.logging.Log._

/** Shared logging helpers for the Delta Sharing client and its enterprise
  * cert-auth acquirer. Centralized so the redaction policy lives in one place.
  *
  * Redaction policy:
  *   - Never log raw access tokens, client secrets, signed assertions, or
  *     `Authorization:` header values.
  *   - Decoded JWT claims (iss, sub, aud, exp, iat, jti, scope) are not
  *     secrets and are safe to log.
  *   - Presigned-URL query strings carry SigV4-style signatures and are
  *     never logged.
  */
object DeltaSharingLogging extends LazySafeLogging {

  /** Decode the middle segment of a JWS and log non-secret standard claims.
    * Makes outbound-to-Databricks tokens debuggable without leaking the bearer.
    *
    * `mode` is one of "cert" | "client_credentials" | "bearer" and is included
    * so logs are searchable across auth paths.
    */
  def logTokenClaims(token: String, mode: String): Unit = {
    val parts = token.split('.')
    if (parts.length < 2) {
      logger.warn(safe"Delta Sharing token (mode=${Safe(mode)}) is not a JWS, cannot decode claims")
    } else {
      try {
        val payload = new String(java.util.Base64.getUrlDecoder.decode(parts(1)), "UTF-8")
        io.circe.parser.parse(payload).toOption.flatMap(_.asObject) match {
          case Some(obj) =>
            val pick = (k: String) => obj(k).flatMap(_.asString).orElse(obj(k).map(_.noSpaces)).getOrElse("(missing)")
            logger.info(
              safe"Delta Sharing access_token (mode=${Safe(mode)}) claims: " +
              safe"iss=${Safe(pick("iss"))} sub=${Safe(pick("sub"))} aud=${Safe(pick("aud"))} " +
              safe"exp=${Safe(pick("exp"))} iat=${Safe(pick("iat"))} jti=${Safe(pick("jti"))} " +
              safe"scope=${Safe(pick("scope"))}",
            )
          case None =>
            logger.warn(safe"Delta Sharing token (mode=${Safe(mode)}) payload not a JSON object")
        }
      } catch {
        case _: Throwable =>
          logger.warn(safe"Delta Sharing token (mode=${Safe(mode)}) claims decode failed")
      }
    }
  }
}
