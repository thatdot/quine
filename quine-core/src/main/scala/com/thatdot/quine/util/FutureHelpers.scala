package com.thatdot.quine.util

import scala.concurrent.{ExecutionContext, Future}

import cats.Foldable
import cats.syntax.foldable._

/** Helper functions filling in some gaps in Scala's Futures library. */
object FutureHelpers {

  implicit class SequentialOps[F[_]: Foldable, A](self: F[A]) {

    /** Filters this foldable value by running a `Future[Boolean]` for each sequence element.
      * But unlike `self.filterA(f)`, the futures are run _sequentially_, not in parallel,
      * to help keep the number of threads down.
      */
    def filterSequentially(f: A => Future[Boolean])(implicit ec: ExecutionContext): Future[List[A]] =
      self.foldM(List.empty[A])((acc, a) => f(a).map(keep => if (keep) a :: acc else acc)).map(_.reverse)

    /** Maps over this foldable value by running a `Future[B]` for each element. But unlike
      * `self.traverse(f)`, the futures are run _sequentially_, not in parallel to help keep
      * the number of threads down.
      *
      * Example:
      * {{{
      *   // Takes ~1 second
      *   List(1,2,3).traverse            (x => Future{ Thread.sleep(1000); x })
      *
      *   // Takes ~3 seconds
      *   List(1,2,3).traverseSequentially(x => Future{ Thread.sleep(1000); x })
      * }}}
      */
    def traverseSequentially[B](f: A => Future[B])(implicit ec: ExecutionContext): Future[List[B]] =
      self.foldM(List.empty[B])((acc, a) => f(a).map(_ :: acc)).map(_.reverse)
  }

}
