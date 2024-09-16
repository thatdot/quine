package com.thatdot.quine.util

import scala.concurrent.Future

import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.{Flow, MergeHub, Sink, Source}

import com.thatdot.quine.util.Log._

object PekkoStreams extends LazySafeLogging {

  def count: Sink[Any, Future[Int]] = Sink.fold[Int, Any](0)((n, _) => n + 1)

  /** Used to perform a filter on a Stream via accumulating state
    * @param state The initial state
    * @param f The filtering function. Takes both state and the element. Returns (possibly modified) state and a boolean
    * @tparam S The type of the state
    * @tparam A The type of the elements
    * @return
    */
  def statefulFilter[S, A](state: S)(f: (S, A) => (S, Boolean)): Flow[A, A, NotUsed] = Flow[A]
    .statefulMap(() => state)(
      (s, elem) => {
        val (newState, cond) = f(s, elem)
        // Wrap the elements we want to keep with Some, return None otherwise
        newState -> Option.when(cond)(elem)
      },
      _ => None,
    )
    // Filter out all the `None`s from above, and unwrap the `Some`s
    .collect { case Some(s) => s }

  /** Remove consecutive duplicates
    */
  // See https://github.com/akka/akka/pull/19408 for upstream discussion (i.e. why doesn't this exist upstream?)
  def distinctConsecutive[A >: Null]: Flow[A, A, NotUsed] =
    // Don't have a previous element at the start of the stream, so just using null, which shouldn't be equal to anything
    statefulFilter(null: A)((previous, element) =>
      // "Emit"s when the element is not equal to the previous element
      element -> (element != previous),
    )

  /** Run a side-effect only on the first element in the stream.
    * @param runOnFirst A function to run on the first element of the stream
    * @tparam A The input type
    * @return
    */
  def wireTapFirst[A](runOnFirst: A => Unit): Flow[A, A, NotUsed] = Flow[A].statefulMap(() => false)(
    (ran, element) => (true, if (ran) element else { runOnFirst(element); element }),
    _ => None,
  )

  /** Create a MergeHub with the given name that, instead of propagating the pekko-streams error signal,
    * logs the error. This avoids an extra layer of stacktrace ("Upstream producer failed with exception")
    * in the logs when exception logging is enabled, and makes stream-killing errors respect the user's log settings.
    */
  def errorSuppressingMergeHub[T](
    mergeHubName: String,
  )(implicit logConfig: LogConfig): Source[T, Sink[T, NotUsed]] =
    MergeHub
      .source[T]
      .mapMaterializedValue(sink =>
        Flow[T]
          .recoverWith { err =>
            logger.error(
              log"""Detected stream-killing error (${Safe(err.getClass.getName)}) from pekko stream feeding
                 |into MergeHub ${Safe(mergeHubName)}""".cleanLines withException err,
            )
            Source.empty
          }
          .to(sink)
          .named(mergeHubName),
      )
}
