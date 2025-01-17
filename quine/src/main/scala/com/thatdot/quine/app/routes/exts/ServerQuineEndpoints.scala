package com.thatdot.quine.app.routes.exts

import scala.util.{Failure, Success}

import org.apache.pekko.util.ByteString

import endpoints4s.{Codec, Invalid, Valid, Validated}

import com.thatdot.common.logging.Log.LogConfig
import com.thatdot.common.quineid.QuineId
import com.thatdot.quine.graph.{NamespaceId, namespaceFromString}
import com.thatdot.quine.model.{EdgeDirection, Milliseconds, QuineIdProvider}
import com.thatdot.quine.routes.exts.{NamespaceParameter, QuineEndpoints}

/** Partial implementation of [[QuineEndpoints]] for schemas that are specific to the server,
  * for types that are defined in the `model` model (and therefore can't be part of [[QuineEndpoints]])
  */
trait ServerQuineEndpoints extends QuineEndpoints with endpoints4s.generic.JsonSchemas {

  implicit def idProvider: QuineIdProvider
  implicit protected def logConfig: LogConfig

  /** The server resolves all IDs straight into [[QuineId]] */
  type Id = QuineId

  /** Codec for QuineId. Uses the [[QuineIdProvider]] to parse/print the ID */
  lazy val idCodec: Codec[String, Id] = new Codec[String, Id] {
    def decode(str: String): Validated[Id] =
      idProvider.qidFromPrettyString(str) match {
        case Success(id) => endpoints4s.Valid(id)
        case Failure(_) => endpoints4s.Invalid(s"Invalid ID value '$str'")
      }

    def encode(id: Id): String = idProvider.qidToPrettyString(id)
  }

  def sampleId(): QuineId = idProvider.newQid()

  type AtTime = Option[Milliseconds]

  lazy val atTimeCodec: Codec[Option[Long], Option[Milliseconds]] =
    new Codec[Option[Long], Option[Milliseconds]] {
      def decode(atTime: Option[Long]): Validated[AtTime] = {
        val now = System.currentTimeMillis
        atTime match {
          case Some(at) if at > now => Invalid(s"Value $at must be less than system time $now")
          case _ => Valid(atTime.map(Milliseconds.apply))
        }
      }
      def encode(atTime: AtTime): Option[Long] = atTime.map(_.millis)
    }

  /** Efficient representation of byte array */
  type BStr = ByteString

  /** Never fails */
  lazy val byteStringCodec: Codec[Array[Byte], BStr] = new Codec[Array[Byte], BStr] {
    def decode(arr: Array[Byte]) = Valid(ByteString(arr))
    def encode(bstr: BStr) = bstr.toArray
  }

  /** Maps of symbols */
  implicit def mapSymbol[T: JsonSchema]: JsonSchema[Map[Symbol, T]] = mapJsonSchema[T]
    .xmap[Map[Symbol, T]](
      _.map { case (k, v) => Symbol(k) -> v },
    )(
      _.map { case (k, v) => k.name -> v },
    )

  /** Schema for symbol */
  implicit lazy val symbolSchema: JsonSchema[Symbol] =
    defaultStringJsonSchema.xmap(Symbol.apply)(_.name)

  /** Edge direction */
  implicit lazy val edgeDirectionsSchema: Enum[EdgeDirection] =
    stringEnumeration[EdgeDirection](EdgeDirection.values)(_.toString)

  implicit lazy val byteArraySchema: JsonSchema[Array[Byte]] =
    byteStringSchema.xmap[Array[Byte]](_.toArray)(ByteString.apply)

  def namespaceFromParam(namespaceParameter: NamespaceParameter): NamespaceId =
    namespaceFromString(namespaceParameter.namespaceId)
}
