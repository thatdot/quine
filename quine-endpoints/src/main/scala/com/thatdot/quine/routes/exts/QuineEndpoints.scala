package com.thatdot.quine.routes.exts

import scala.concurrent.duration.{DurationLong, FiniteDuration}

import endpoints4s._
import endpoints4s.algebra.Documentation

import com.thatdot.quine.routes.IngestRoutes
import com.thatdot.quine.routes.exts.NamespaceParameter.defaultNamespaceParameter

class NamespaceParameter private (val namespaceId: String) extends AnyVal
object NamespaceParameter {

  def apply(s: String): Option[NamespaceParameter] = {
    val normalized = s.toLowerCase
    Option.when(isValidNamespaceParameter(normalized))(new NamespaceParameter(normalized))
  }

  val defaultNamespaceParameter: NamespaceParameter = new NamespaceParameter("default")

  /** No more than 16 characters total, must start with a letter
    *
    * Note: we do not want to allow unicode characters in namespaces,
    * since they are illegal in cassandra table names.
    */
  def isValidNamespaceParameter(s: String): Boolean = {
    val validNamespacePattern = raw"""[a-zA-Z][a-zA-Z0-9]{0,15}+""".r
    validNamespacePattern.matches(s)
  }

  val namespaceCodec: Codec[String, NamespaceParameter] = new Codec[String, NamespaceParameter] {
    override def decode(s: String): Validated[NamespaceParameter] =
      Validated.fromOption(NamespaceParameter(s))(s"'$s' is not a valid namespace name")

    override def encode(from: NamespaceParameter): String = from.namespaceId
  }

}

class NamespaceNotFoundException(namespace: String) extends NoSuchElementException(s"Namespace $namespace not found")

trait NamespaceQueryString extends endpoints4s.algebra.Urls {

  /** This is overridden in active routes where the graph is availale (i.e [[QuineAppRoutes]]). The
    * no-op concrete implementation is only expressed here to avoid having to put the no-op
    * in other endpoints expressions (OpenApiDocs, QuineClient)
    */
  def namespaceExists(namespace: String): Boolean = true

  import NamespaceParameter.namespaceCodec
  private val optionalNamespaceCodec: Codec[Option[String], NamespaceParameter] =
    new Codec[Option[String], NamespaceParameter] {

      override def decode(from: Option[String]): Validated[NamespaceParameter] =
        from.fold[Validated[NamespaceParameter]](Valid(defaultNamespaceParameter))(namespaceCodec.decode)

      override def encode(from: NamespaceParameter): Option[String] =
        Option.when(from != defaultNamespaceParameter)(namespaceCodec.encode(from))
    }

  implicit lazy val namespaceQueryStringParam: QueryStringParam[NamespaceParameter] =
    optionalQueryStringParam(stringQueryString)
      .xmapWithCodec(optionalNamespaceCodec)
}

trait AtTimeQueryString extends endpoints4s.algebra.Urls {

  /** The decoded type of timestamps */
  protected type AtTime

  /** Since timestamps get encoded as milliseconds since 1970 in the REST API,
    * it is necessary to define the serialization/deserialization to/from a long.
    */
  protected def atTimeCodec: Codec[Option[Long], AtTime]

  /** Schema for an at time */
  implicit lazy val atTimeQueryStringParam: QueryStringParam[AtTime] =
    optionalQueryStringParam(longQueryString)
      .xmapWithCodec(atTimeCodec)
}

trait NoopAtTimeQueryString extends AtTimeQueryString {
  type AtTime = Option[Long]

  lazy val atTimeCodec: Codec[Option[Long], AtTime] = new Codec[Option[Long], AtTime] {
    def decode(atTime: Option[Long]) = endpoints4s.Valid(atTime)
    def encode(atTime: AtTime) = atTime
  }
}

trait IdSchema extends endpoints4s.algebra.JsonSchemas {

  /** The decoded type of graph node IDs */
  protected type Id

  /** Since IDs get encoded as strings in the REST API, it is necessary to
    * define the serialization/deserialization to/from strings.
    */
  protected def idCodec: Codec[String, Id]

  protected def sampleId(): Id

  /** Schema for an ID */
  implicit lazy val idSchema: JsonSchema[Id] =
    stringJsonSchema(format = Some("node-id"))
      .xmapWithCodec(idCodec)
      .withExample(sampleId())

  // TODO: find some other place for this to live?
  implicit lazy val namespaceSchema: JsonSchema[NamespaceParameter] =
    stringJsonSchema(format = Some("namespace-id")).xmapWithCodec(NamespaceParameter.namespaceCodec)
}

trait NoopIdSchema extends IdSchema {
  type Id = String

  lazy val idCodec: Codec[String, Id] = new Codec[String, Id] {
    def decode(str: String) = endpoints4s.Valid(str)
    def encode(id: Id) = id
  }

  def sampleId() = ""
}

/** Schemas, segments, parameters that the Quine API relies on
  *
  * This abstracts out some common JSON schemas, parameters, requests, and
  * responses into a simple trait that we can mix in to our various endpoint
  * classes.
  */
trait QuineEndpoints extends EntitiesWithExamples with IdSchema with AtTimeQueryString with NamespaceQueryString {

  /** Typeclass instance for using an ID as a query string parameter */
  implicit lazy val idParam: QueryStringParam[Id] =
    stringQueryString.xmapPartial(idCodec.decode)(idCodec.encode)

  /** Typeclass instance for using an ID as a URL segment */
  implicit lazy val idSegment: Segment[Id] =
    stringSegment.xmapPartial(idCodec.decode)(idCodec.encode)

  /** The decoded type of binary data */
  type BStr

  /** Since binary data gets encoded as Base64 strings, it is necessary to
    * define the serialization/deserialization to/from byte arrays
    */
  protected def byteStringCodec: Codec[Array[Byte], BStr]

  /** Schema for binary data encoding (uses base64 encoded strings) */
  implicit lazy val byteStringSchema: JsonSchema[BStr] = {
    val enc = java.util.Base64.getEncoder
    val dec = java.util.Base64.getDecoder
    val base64Codec: endpoints4s.Codec[String, Array[Byte]] =
      endpoints4s.Codec.parseStringCatchingExceptions(
        `type` = "base64 string",
        parse = dec.decode,
        print = enc.encodeToString
      )

    stringJsonSchema(format = Some("base64"))
      .withExample("Ym9veWFoIQ==")
      .xmapWithCodec(base64Codec)
      .xmapWithCodec(byteStringCodec)
  }

  final val nodeIdSegment: Path[Id] = segment[Id]("id", docs = Some("Node id"))

  final val namespace: QueryString[NamespaceParameter] = qs[NamespaceParameter](
    "namespace",
    docs = Some("""Namespace. If no namespace is provided, the default namespace will be used.
        |
        |Namespaces must be between 1-16 characters, consist of only letters or digits,
        |and must start with a letter.""".stripMargin)
  )

  final val atTime: QueryString[AtTime] = qs[AtTime](
    "at-time",
    docs = Some("An integer timestamp in milliseconds since the Unix epoch representing the historical moment to query")
  )

  final val reqTimeout: QueryString[Option[FiniteDuration]] = qs[Option[FiniteDuration]](
    "timeout",
    docs = Some("Milliseconds to wait before the HTTP request times out")
  )(
    optionalQueryStringParam(longQueryString.xmap(_.millis)(_.toMillis))
  )

  // NB this should be used for _write_ parallelism
  final val parallelism: QueryString[Int] = qs[Option[Int]](
    name = "parallelism",
    docs = Some(
      s"Operations to execute simultaneously. Default: `${IngestRoutes.defaultWriteParallelism}`"
    )
  ).xmap(_.getOrElse(IngestRoutes.defaultWriteParallelism))(Some(_))

  /** Schema for sets */
  implicit final def setSchema[T: JsonSchema]: JsonSchema[Set[T]] =
    implicitly[JsonSchema[Vector[T]]].xmap(_.toSet)(_.toVector)

  final def accepted[A, B, R](
    entity: ResponseEntity[A] = emptyResponse,
    docs: Documentation = None,
    headers: ResponseHeaders[B] = emptyResponseHeaders
  )(implicit tupler: Tupler.Aux[A, B, R]): Response[R] =
    response(Accepted, entity, docs, headers)

  final def noContent[B](
    docs: Documentation = None,
    headers: ResponseHeaders[B] = emptyResponseHeaders
  ): Response[B] =
    response(NoContent, emptyResponse, docs, headers)

  final def created[A, B, R](
    entity: ResponseEntity[A] = emptyResponse,
    docs: Documentation = None,
    headers: ResponseHeaders[B] = emptyResponseHeaders
  )(implicit tupler: Tupler.Aux[A, B, R]): Response[R] =
    response(Created, entity, docs, headers)

  final def serviceUnavailable[A, B, R](
    entity: ResponseEntity[A] = emptyResponse,
    docs: Documentation = None,
    headers: ResponseHeaders[B] = emptyResponseHeaders
  )(implicit tupler: Tupler.Aux[A, B, R]): Response[R] =
    response(ServiceUnavailable, entity, docs, headers)

  def ServiceUnavailable: StatusCode

}
