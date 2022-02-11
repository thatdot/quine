package com.thatdot.quine.compiler.cypher

import scala.annotation.tailrec
import scala.language.higherKinds

import cats.{FlatMap, Functor, Monad}

import com.thatdot.quine.graph.cypher.{Location, Query}

/** Monad transformer for running [[WithQuery]] inside a bigger monadic context
  *
  * @param runWithQuery underlying monadic computation
  */
final case class WithQueryT[F[_], A](runWithQuery: F[WithQuery[A]]) {

  def map[B](f: A => B)(implicit functorF: Functor[F]): WithQueryT[F, B] =
    WithQueryT(functorF.map(runWithQuery)(_.map(f)))

  def flatMap[B](f: A => WithQueryT[F, B])(implicit flatMapF: FlatMap[F]): WithQueryT[F, B] =
    WithQueryT(
      flatMapF.flatMap(runWithQuery) { case WithQuery(a, queryA) =>
        flatMapF.map(f(a).runWithQuery) { case WithQuery(b, queryB) =>
          WithQuery(b, Query.apply(queryA, queryB))
        }
      }
    )
}
object WithQueryT {

  implicit def monad[F[_]](implicit monadF: Monad[F]): Monad[WithQueryT[F, *]] =
    new Monad[WithQueryT[F, *]] {

      def flatMap[A, B](fa: WithQueryT[F, A])(f: A => WithQueryT[F, B]): WithQueryT[F, B] =
        fa.flatMap(f)

      override def map[A, B](fa: WithQueryT[F, A])(f: A => B): WithQueryT[F, B] = fa.map(f)

      def pure[A](a: A): WithQueryT[F, A] = WithQueryT(monadF.pure(WithQuery(a)))

      def tailRecM[A, B](a: A)(f: A => WithQueryT[F, Either[A, B]]): WithQueryT[F, B] = {

        def step(wqa: WithQuery[A]): F[Either[WithQuery[A], WithQuery[B]]] = {
          val stepped: F[WithQuery[Either[A, B]]] = f(wqa.result).runWithQuery
          monadF.map(stepped) {
            case WithQuery(Left(a), query) => Left(WithQuery(a, Query.apply(wqa.query, query)))
            case WithQuery(Right(b), query) => Right(WithQuery(b, Query.apply(wqa.query, query)))
          }
        }

        WithQueryT.apply(monadF.tailRecM(WithQuery(a, Query.Unit()))(step))
      }
    }

  def pure[F[_]: Monad, A](a: A) = monad.pure(a)

  def apply[F[_], A](result: A, query: Query[Location.Anywhere])(implicit
    monadF: Monad[F]
  ): WithQueryT[F, A] =
    WithQueryT(monadF.pure(WithQuery(result, query)))

  def lift[F[_], A](fa: F[A])(implicit functorF: Functor[F]): WithQueryT[F, A] =
    WithQueryT[F, A](functorF.map(fa)(WithQuery(_)))
}

/** Some value that only makes sense if some query has run first
  *
  * This is useful for facilitating compilation of constructs which accumulate
  * side effects in the graph. For instance, consider the compilation of a
  * an expression like `GetDegree(n)` which returns the number of edges on `n`.
  * Since expressions can't touch the graph we compile it to (roughly)
  *
  * {{{
  * WithQuery(
  *   result = freshVariable,
  *   query = ArgumentEntry(
  *     entry = n,
  *     GetDegree(freshVariable)
  *   ),
  * )
  * }}}
  *
  * After the query part of that has run, `freshVariable` is now in the context,
  * so the compiled expression is simply a variable.
  *
  * @param result value produced
  * @param query side effecting query that is required by the value
  */
final case class WithQuery[+E](
  result: E,
  query: Query[Location.Anywhere] = Query.Unit()
) {

  /** Update only the result part of a [[WithQuery]]
    *
    * @param fn how to update the result part
    * @return an updated result with the same side effects
    */
  def map[T](fn: E => T): WithQuery[T] = WithQuery(fn(result), query)

  /** Update the result part of a [[WithQuery]] and accumulate more side-effects
    *
    * @param fn how to update the result and gather side effects
    * @return updated result and side-effects
    */
  def flatMap[T](fn: E => WithQuery[T]): WithQuery[T] = {
    val WithQuery(result2, query2) = fn(result)
    WithQuery(
      result = result2,
      query = Query.apply(query, query2)
    )
  }

  /** Make a node query
    *
    * Turns the result into a query and sequences it with existing effects
    *
    * @param elim how to transform the result
    * @return a full query to run on a node
    */
  def toNodeQuery(elim: E => Query[Location.OnNode]): Query[Location.OnNode] =
    Query.apply(
      query,
      elim(result)
    )

  /** Make a general query
    *
    * Turns the result into a query and sequences it with existing effects
    *
    * @param elim how to transform the result
    * @return a full query to run anywhere
    */
  def toQuery(elim: E => Query[Location.Anywhere]): Query[Location.Anywhere] =
    Query.apply(
      query,
      elim(result)
    )
}

object WithQuery {

  /** Monad instance for [[WithQuery]] */
  implicit val monad: Monad[WithQuery] = new Monad[WithQuery] {
    def flatMap[A, B](fa: WithQuery[A])(f: A => WithQuery[B]): WithQuery[B] = fa.flatMap(f)

    override def map[A, B](fa: WithQuery[A])(f: A => B): WithQuery[B] = fa.map(f)

    def pure[A](a: A): WithQuery[A] = WithQuery(a)

    def tailRecM[A, B](a: A)(f: A => WithQuery[Either[A, B]]): WithQuery[B] = {

      @tailrec
      def step(a1: A, query1: Query[Location.Anywhere]): WithQuery[B] = {
        val WithQuery(aOrB, query2) = f(a)
        val query3 = Query.apply(query1, query2)

        aOrB match {
          case Left(a2) => step(a2, query3)
          case Right(b) => WithQuery(b, query3)
        }
      }

      step(a, Query.Unit())
    }
  }
}
