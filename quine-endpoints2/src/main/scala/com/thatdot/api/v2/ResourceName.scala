package com.thatdot.api.v2

import io.circe.{Decoder, Encoder}
import sttp.tapir.{Codec, CodecFormat, DecodeResult, Schema}

/** A non-empty identifier safe to use as a URL path segment in V2 APIs.
  *
  * The validation rule follows Google AIP-122 ("Resource names"): the value matches
  * `[A-Za-z0-9_.~-]{1,128}` — the RFC 3986 "unreserved" character set, bounded in
  * length. The colon in particular is forbidden because AIP-136 reserves it as the
  * delimiter between a resource name and a custom verb (e.g. `{name}:pause`), so a
  * colon-bearing name would make those URLs ambiguous.
  *
  * Constructed only via [[ResourceName.apply]] (private constructor), and
  * the codecs below decode through `apply`, so any `ResourceName` reaching the
  * application layer has already passed validation. To enforce this property at API
  * boundaries, use `path[ResourceName](...)` for URL captures and type request-body
  * name fields as `ResourceName` rather than `String`.
  */
final class ResourceName private (val value: String) extends AnyVal {
  override def toString: String = value
}

object ResourceName {

  /** AIP-122 resource ID format: RFC 3986 unreserved characters, length 1-128. */
  private val ValidPattern = """[A-Za-z0-9_.~-]{1,128}""".r

  def apply(s: String): Either[String, ResourceName] =
    if (ValidPattern.matches(s)) Right(new ResourceName(s))
    else Left(invalidMessage(s))

  /** Wrap a string as a `ResourceName` without validating it.
    *
    * Only for echoing back values that originated as a validated `ResourceName` and were
    * stored internally as `String` (e.g. when constructing a response DTO from an internal
    * record whose name field was validated at create time). Never use for values that
    * could have come from an untrusted source — use [[apply]] for those.
    */
  def unsafeFromString(s: String): ResourceName = new ResourceName(s)

  def invalidMessage(s: String): String =
    s"'$s' is not a valid resource name. Resource names must be 1-128 characters long and " +
    "contain only ASCII letters, digits, underscore ('_'), hyphen ('-'), period ('.'), or " +
    "tilde ('~')."

  /** Human-readable description of the rule, intended for API documentation. Plain
    * enough that an end user reading the OpenAPI schema can decide what they may type.
    */
  val schemaDescription: String =
    "Resource identifier. 1-128 characters; ASCII letters, digits, and `_ - . ~` only. " +
    "Colons are not allowed (they delimit custom-method verbs in V2 URLs)."

  implicit val tapirSchema: Schema[ResourceName] =
    Schema.string[ResourceName].format("resource-name").description(schemaDescription)

  // Attach `tapirSchema` so the OpenAPI document advertises the validation rule on
  // every `path[ResourceName](...)` site (the default codec schema is the underlying
  // String, which would hide the format).
  implicit val tapirCodec: Codec[String, ResourceName, CodecFormat.TextPlain] =
    Codec.string
      .mapDecode { raw =>
        apply(raw) match {
          case Right(rn) => DecodeResult.Value(rn)
          case Left(err) => DecodeResult.Error(raw, new IllegalArgumentException(err))
        }
      }(_.value)
      .schema(tapirSchema)

  implicit val circeEncoder: Encoder[ResourceName] = Encoder.encodeString.contramap(_.value)
  implicit val circeDecoder: Decoder[ResourceName] = Decoder.decodeString.emap(apply)
}
