package com.thatdot.quine

import cats.implicits._
import io.circe.Decoder.Result
import io.circe.syntax.EncoderOps
import io.circe.{Decoder, Encoder, Json}
import org.scalatest.Assertion
import org.scalatest.Assertions.assert

trait CirceCodecTestSupport {

  /** Test that a value can round-trip through JSON encoding/decoding. */
  def testJsonRoundtrip[V: Encoder: Decoder](v: V): Assertion = {
    val json = v.asJson
    val decoded: Result[V] = json.as[V]
    assert(decoded == Right(v), s"Roundtrip failed for: $v\nJSON: ${json.spaces2}\nError: $decoded")
  }

  /** Checks to see if a json encoding produces any "ugly" values.
    * Any time a "Left" or "Right" appears as a key, we probably have an Either that was encoded wrong.
    * Any class that encodes to an empty object is also probably wrong.
    *
    * @param json The json to recursively check
    * @param allowedToBeEmpty Since we cannot tell from the json alone whether an empty object came from
    *                         a case class or just a map, allowedToBeEmpty indicates that a value is allowed to be empty
    *                         (i.e. it came from a map rather than a case class)
    * @param path The current path in the JSON tree (for error messages)
    * @return Left(error) if there is an ugly value in the json otherwise Right(())
    */
  def checkForUglyJson(
    json: Json,
    allowedToBeEmpty: Vector[String] => Boolean,
    path: Vector[String] = Vector.empty,
  ): Either[String, Unit] =
    json.fold[Either[String, Unit]](
      Right(()),
      _ => Right(()),
      _ => Right(()),
      _ => Right(()),
      _.zipWithIndex
        .traverse { case (innerJson, index) =>
          checkForUglyJson(innerJson, allowedToBeEmpty, path.appended(index.toString))
        }
        .map(_ => ()),
      obj => {
        val map = obj.toMap
        for {
          _ <- if (map.contains("Left")) Left(s"Json contained a left value at ${path.mkString(".")}") else Right(())
          _ <- if (map.contains("Right")) Left(s"Json contained a right value at ${path.mkString(".")}") else Right(())
          _ <-
            if (map.isEmpty && !allowedToBeEmpty(path)) Left(s"Json object was empty at ${path.mkString(".")}")
            else Right(())
          _ <- map.toList.traverse { case (k, innerJson) =>
            checkForUglyJson(innerJson, allowedToBeEmpty, path.appended(k))
          }
        } yield ()
      },
    )
}
