package com.thatdot.quine.routes.exts

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

  val ServiceUnavailable: StatusCode = 503
}
