package com.thatdot.quine.graph.messaging

import scala.concurrent.Future

import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.Source

sealed abstract class QuineResponse

object QuineResponse {
  final case class Success(a: QuineMessage) extends QuineResponse

  /** A wrapper for converting a Source[_] to something serializable (for sending to other cluster
    * hosts), and back again.
    * TODO: this is flawed in a couple ways:
    *
    *  - Network failures cause the source to fail or drop elements (not retry), see QU-68
    *  - The source must be begin to be run shortly after being received (else timeout)
    *
    * @param ref a source ref that can be sent to materialize a source on the destination JVM
    */
  final case class StreamRef(ref: String) extends QuineResponse

  /** A wrapper for sending a failure as something serializable. This gets used
    * to represent failures when remotely sending a [[Future]] or a [[Source]]
    */
  final case class Failure(err: Throwable) extends QuineResponse

  /** Not meant to be serialized - used for a [[Future]] sent within the JVM */
  final case class LocalFuture(future: Future[_]) extends QuineResponse

  /** Not meant to be serialized - used for a [[Source]] sent within the JVM */
  final case class LocalSource(source: Source[_, NotUsed]) extends QuineResponse
}
