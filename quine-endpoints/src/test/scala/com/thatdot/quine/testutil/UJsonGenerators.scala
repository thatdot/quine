package com.thatdot.quine.testutil

import scala.collection.mutable

import org.scalacheck.{Arbitrary, Gen}
import ujson.Value

// Adapted from https://github.com/circe/circe/blob/master/modules/testing/shared/src/main/scala/io/circe/testing/ArbitraryInstances.scala

trait UJsonGenerators {

  /** The maximum depth of a generated JSON value.
    */
  protected def maxJsonDepth: Int = 5

  /** The maximum number of values in a generated JSON array.
    */
  protected def maxJsonArraySize: Int = 10

  /** The maximum number of key-value pairs in a generated JSON object.
    */
  protected def maxJsonObjectSize: Int = 10

  private[this] val genNull: Gen[Value] = Gen.const(ujson.Null)
  private[this] val genBool: Gen[Value] = Arbitrary.arbitrary[Boolean].map(ujson.Bool(_))
  private[this] val genString: Gen[Value] = Arbitrary.arbitrary[String].map(ujson.Str)
  private[this] val genNumber: Gen[Value] = Arbitrary.arbitrary[Double].map(ujson.Num)

  private[this] def genJsonArray(depth: Int): Gen[ujson.Arr] = Gen.choose(0, maxJsonArraySize).flatMap { size =>
    Gen
      .buildableOfN[mutable.ArrayBuffer[Value], Value](size, genJsonAtDepth(depth + 1))
      .map(ujson.Arr(_))
  }

  private[this] def genJsonObject(depth: Int): Gen[ujson.Obj] =
    Gen.choose(0, maxJsonObjectSize).flatMap { size =>
      Gen.listOfN(
        size,
        for {
          key <-
            Gen.alphaNumStr // Limit the keys to alphanumeric to keep them a little more readable.
          value <- genJsonAtDepth(depth + 1)
        } yield key -> value
      ) map ujson.Obj.from
    }

  private[this] def genJsonAtDepth(depth: Int): Gen[Value] = {
    val genJsons = genNumber :: genString :: (
      if (depth < maxJsonDepth) List(genJsonArray(depth), genJsonObject(depth)) else Nil
    )

    // Split out this way because `Gen.oneOf` requires two args and then varargs.
    Gen.oneOf(genNull, genBool, genJsons: _*)
  }

  implicit val arbitraryJson: Arbitrary[Value] = Arbitrary(genJsonAtDepth(0))
  implicit val arbitraryJsonObject: Arbitrary[ujson.Obj] = Arbitrary(genJsonObject(0))
  implicit val arbitraryJsonArray: Arbitrary[ujson.Arr] = Arbitrary(genJsonArray(0))

}

object UJsonGenerators extends UJsonGenerators
