package com.thatdot.quine.routes.exts

import scala.scalajs.js

import endpoints4s.Codec

import collection.immutable.IndexedSeq

/** Browser implementation of [[QuineEndpoints]]
  */
trait ClientQuineEndpoints
    extends QuineEndpoints
    with NoopIdSchema
    with NoopAtTimeQueryString
    with endpoints4s.algebra.JsonEntities
    with endpoints4s.algebra.JsonSchemas
    with endpoints4s.algebra.Urls
    with endpoints4s.xhr.future.Endpoints {

  /** Simple immutable representation of byte array */
  type BStr = IndexedSeq[Byte]

  /** Never fails */
  lazy val byteStringCodec: Codec[Array[Byte], BStr] = new endpoints4s.Codec[Array[Byte], BStr] {
    def decode(arr: Array[Byte]) = endpoints4s.Valid(arr.toIndexedSeq)
    def encode(bstr: BStr) = bstr.toArray
  }

  /** RFC 3339 for the v2 API, rendered and parsed by the browser's own `Date`.
    *
    * `encode` throws `IllegalArgumentException` for a moment outside the range a JavaScript `Date`
    * can represent (±8.64e15 ms); dropping the parameter instead would silently query the present.
    */
  lazy val atTimeRfc3339Codec: Codec[Option[String], AtTime] = new endpoints4s.Codec[Option[String], AtTime] {
    def decode(atTime: Option[String]): endpoints4s.Validated[AtTime] = atTime match {
      case None => endpoints4s.Valid(None)
      case Some(str) =>
        val millis = js.Date.parse(str)
        if (millis.isNaN) endpoints4s.Invalid(s"Invalid RFC 3339 timestamp: $str")
        else endpoints4s.Valid(Some(millis.toLong))
    }
    def encode(atTime: AtTime): Option[String] = atTime.map { millis =>
      val date = new js.Date(millis.toDouble)
      if (date.getTime().isNaN)
        throw new IllegalArgumentException(s"atTime $millis ms is outside the range a JavaScript Date can represent")
      date.toISOString()
    }
  }

  val ServiceUnavailable: StatusCode = 503
}
