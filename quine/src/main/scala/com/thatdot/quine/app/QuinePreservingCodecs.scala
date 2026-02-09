package com.thatdot.quine.app

import io.circe.Encoder

import com.thatdot.common.security.Secret
import com.thatdot.quine.app.model.ingest2.V2IngestEntities.QuineIngestStreamWithStatus
import com.thatdot.quine.serialization.EncoderDecoder

/** Quine codecs that preserve credentials (instead of redacting them).
  *
  * WARNING: Only use for persistence, NEVER for API responses.
  *
  * == Background ==
  *
  * Quine API types derive encoders that redact `Secret` values using `Secret.toString` which
  * produces "Secret(****)". This is correct for API responses but would break persistence
  * by destroying credentials.
  *
  * == Solution ==
  *
  * Each type provides a `preservingEncoder` method that emits the actual credential value.
  * This object wires those preserving encoders into complete codecs for persistence.
  *
  * == Usage ==
  *
  * All codec methods require `import Secret.Unsafe._` at the call site:
  * {{{
  * import Secret.Unsafe._
  * val codec = QuinePreservingCodecs.ingestStreamWithStatusCodec
  * }}}
  */
object QuinePreservingCodecs {

  /** Codec for `QuineIngestStreamWithStatus` persistence.
    * Requires witness (`import Secret.Unsafe._`) to call.
    */
  def ingestStreamWithStatusCodec(implicit
    ev: Secret.UnsafeAccess,
  ): EncoderDecoder[QuineIngestStreamWithStatus] = {
    implicit val enc: Encoder[QuineIngestStreamWithStatus] = QuineIngestStreamWithStatus.preservingEncoder
    EncoderDecoder.ofEncodeDecode
  }
}
