package com.thatdot.api.v2.codec

import io.circe.{Decoder, Encoder}
import sttp.tapir.{Schema, Validator}

/** Codec helpers for sealed-trait "enum-like" types whose JSON wire format should be
  * SCREAMING_SNAKE_CASE per AIP-126. Scala identifiers stay in idiomatic PascalCase;
  * conversion to the wire format happens at the codec boundary.
  *
  * Usage:
  * {{{
  *   sealed abstract class MyEnum
  *   object MyEnum {
  *     case object FirstValue extends MyEnum
  *     case object SecondValue extends MyEnum
  *
  *     val values: Seq[MyEnum] = Seq(FirstValue, SecondValue)
  *
  *     implicit val encoder: Encoder[MyEnum] = ScreamingSnakeEnum.encoder
  *     implicit val decoder: Decoder[MyEnum] = ScreamingSnakeEnum.decoder(values)
  *     implicit lazy val schema: Schema[MyEnum] = ScreamingSnakeEnum.schema(values)
  *   }
  * }}}
  *
  * Wire values: `"FIRST_VALUE"`, `"SECOND_VALUE"`. Use this only for enums whose desired
  * wire format is the AIP-126 default; if the wire value must mirror an external system
  * (e.g. Kafka's `"PLAINTEXT"`/`"latest"`), use an explicit `Encoder.contramap` instead.
  */
object ScreamingSnakeEnum {

  /** Convert a `PascalCase` (or `camelCase`) identifier to `SCREAMING_SNAKE_CASE`.
    * Inserts an underscore before every uppercase letter that follows a lowercase letter.
    */
  def toScreamingSnake(s: String): String = {
    val sb = new StringBuilder(s.length + 4)
    var i = 0
    while (i < s.length) {
      val c = s.charAt(i)
      if (i > 0 && c.isUpper && s.charAt(i - 1).isLower) sb.append('_')
      sb.append(c.toUpper)
      i += 1
    }
    sb.toString
  }

  def encoder[T]: Encoder[T] =
    Encoder.encodeString.contramap(t => toScreamingSnake(t.toString))

  def decoder[T](values: Seq[T]): Decoder[T] = {
    val byWireValue: Map[String, T] = values.iterator.map(v => toScreamingSnake(v.toString) -> v).toMap
    Decoder.decodeString.emap(s => byWireValue.get(s).toRight(s"Unknown enum value: $s"))
  }

  def schema[T](values: Seq[T]): Schema[T] =
    Schema.string.validate(Validator.enumeration(values.toList, v => Some(toScreamingSnake(v.toString))))
}
