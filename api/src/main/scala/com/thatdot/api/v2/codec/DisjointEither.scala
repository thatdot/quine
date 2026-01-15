package com.thatdot.api.v2.codec

import io.circe.{Decoder, Encoder}

/** Evidence that A and B are structurally disjoint in JSON (one is primitive,
  * one is object, etc.) enabling unambiguous Either encoding without a wrapper object.
  *
  * When two types serialize to structurally distinguishable JSON (e.g., a string vs
  * an object), we can encode `Either[A, B]` directly as either A's or B's JSON
  * representation, and decode by attempting A first, then B.
  *
  * Usage:
  * {{{
  * import com.thatdot.api.v2.codec.DisjointEither.syntax._
  * import com.thatdot.api.v2.codec.DisjointEvidence.JsonObjLike
  *
  * // Mark your case class as object-like
  * implicit val myTypeObjLike: JsonObjLike[MyType] = new JsonObjLike[MyType] {}
  *
  * // Now Either[String, MyType] has encoder/decoder automatically
  * val codec: Encoder[Either[String, MyType]] = implicitly
  * }}}
  */
sealed trait DisjointEvidence[A, B]

object DisjointEvidence {

  /** Marker for JSON primitive types (String, Int, Boolean, etc.) */
  trait JsonPrim[A]

  /** Marker for JSON array-like types (List, Set, etc.) */
  trait JsonListLike[A]

  /** Marker for JSON object-like types (case classes, Map, etc.) */
  trait JsonObjLike[A]

  // Built-in JsonPrim instances
  implicit val jsonPrimInt: JsonPrim[Int] = new JsonPrim[Int] {}
  implicit val jsonPrimString: JsonPrim[String] = new JsonPrim[String] {}
  implicit val jsonPrimBoolean: JsonPrim[Boolean] = new JsonPrim[Boolean] {}

  // Built-in JsonListLike instances
  implicit def jsonListLikeList[A]: JsonListLike[List[A]] = new JsonListLike[List[A]] {}
  implicit def jsonListLikeSet[A]: JsonListLike[Set[A]] = new JsonListLike[Set[A]] {}

  // Built-in JsonObjLike instances
  implicit def jsonObjLikeMap[K, V]: JsonObjLike[Map[K, V]] = new JsonObjLike[Map[K, V]] {}

  // Disjoint evidence derivations (6 combinations of Prim/List/Obj)
  implicit def primObj[A: JsonPrim, B: JsonObjLike]: DisjointEvidence[A, B] = new DisjointEvidence[A, B] {}
  implicit def objPrim[A: JsonObjLike, B: JsonPrim]: DisjointEvidence[A, B] = new DisjointEvidence[A, B] {}
  implicit def primList[A: JsonPrim, B: JsonListLike]: DisjointEvidence[A, B] = new DisjointEvidence[A, B] {}
  implicit def listPrim[A: JsonListLike, B: JsonPrim]: DisjointEvidence[A, B] = new DisjointEvidence[A, B] {}
  implicit def listObj[A: JsonListLike, B: JsonObjLike]: DisjointEvidence[A, B] = new DisjointEvidence[A, B] {}
  implicit def objList[A: JsonObjLike, B: JsonListLike]: DisjointEvidence[A, B] = new DisjointEvidence[A, B] {}
}

/** Provides Either codecs when disjointness evidence exists.
  *
  * Mix in `DisjointEitherOps` or import `DisjointEither.syntax._` to get
  * implicit `Encoder[Either[A, B]]` and `Decoder[Either[A, B]]` when
  * `DisjointEvidence[A, B]` is available.
  */
object DisjointEither {
  object syntax extends DisjointEitherOps
}

trait DisjointEitherOps {

  implicit def disjointEitherEncoder[A, B](implicit
    ev: DisjointEvidence[A, B],
    encodeA: Encoder[A],
    encodeB: Encoder[B],
  ): Encoder[Either[A, B]] = {
    case Left(value) => encodeA(value)
    case Right(value) => encodeB(value)
  }

  implicit def disjointEitherDecoder[A, B](implicit
    ev: DisjointEvidence[A, B],
    decodeA: Decoder[A],
    decodeB: Decoder[B],
  ): Decoder[Either[A, B]] =
    decodeA.map(Left(_)).or(decodeB.map(Right(_)))
}
