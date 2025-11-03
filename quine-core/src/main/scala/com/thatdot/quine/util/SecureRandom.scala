package com.thatdot.quine.util

import java.nio.ByteBuffer
import java.security.{SecureRandom => JSecureRandom}
import java.util.UUID

import scala.util.Try

/** UUID generation using FIPS-compliant SecureRandom when available.
  *
  * In FIPS-enabled JVMs, `getInstanceStrong()` automatically uses the configured
  * FIPS provider. Falls back to standard SecureRandom if strong RNG is unavailable.
  *
  * Thread-safe. Initializes lazily on first use and may block briefly while
  * gathering entropy.
  */
object SecureRandom {

  private lazy val strongRandom: JSecureRandom =
    Try(JSecureRandom.getInstanceStrong()).getOrElse(new JSecureRandom())

  /** Generate cryptographically strong UUID for security-critical operations
    * (OAuth2 tokens, JWT claims, audit IDs, billing identifiers).
    *
    * First call may block during RNG initialization.
    */
  def randomUUID(): UUID = {
    val randomBytes = new Array[Byte](16)
    strongRandom.nextBytes(randomBytes)

    // RFC 4122 section 4.4: version 4 UUID with proper variant bits
    randomBytes(6) = ((randomBytes(6) & 0x0F) | 0x40).toByte
    randomBytes(8) = ((randomBytes(8) & 0x3F) | 0x80).toByte

    val bb = ByteBuffer.wrap(randomBytes)
    new UUID(bb.getLong(), bb.getLong())
  }
}
