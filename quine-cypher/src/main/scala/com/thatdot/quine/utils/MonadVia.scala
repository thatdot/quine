package com.thatdot.quine.utils

import cats.arrow.FunctionK
import cats.{Monad, MonadError}

/** Derive a monad instance from another
  *
  * @param instance monad instance for F
  * @param fToG fully faithful transformation from F to G
  * @param gToF fully faithful transformation from G to F
  *
  * `fToG` and `gToF` must be inverses such that
  *
  * {{{
  * fToG.compose(gToF) == FunctionK.identity[G]
  * gToF.compose(fToG) == FunctionK.identity[F]
  * }}}
  */
class MonadVia[F[_], G[_]](
  instance: Monad[F],
  fToG: FunctionK[F, G],
  gToF: FunctionK[G, F]
) extends Monad[G] {

  def flatMap[A, B](ga: G[A])(f: A => G[B]): G[B] =
    fToG.apply[B](instance.flatMap(gToF.apply[A](ga))(f.andThen(gToF.apply[B])))

  def pure[A](a: A): G[A] =
    fToG.apply[A](instance.pure(a))

  def tailRecM[A, B](a: A)(f: A => G[Either[A, B]]): G[B] =
    fToG.apply[B](instance.tailRecM(a)(f.andThen(gToF.apply[Either[A, B]])))
}

/** Derive a monad error instance from another
  *
  * @param instance monad error instance for F
  * @param fToG fully faithful transformation from F to G
  * @param gToF fully faithful transformation from G to F
  * @see MonadVia
  */
class MonadErrorVia[F[_], G[_], E](
  instance: MonadError[F, E],
  fToG: FunctionK[F, G],
  gToF: FunctionK[G, F]
) extends MonadVia[F, G](instance, fToG, gToF)
    with MonadError[G, E] {

  def handleErrorWith[A](ga: G[A])(f: E => G[A]): G[A] =
    fToG.apply[A](instance.handleErrorWith(gToF.apply[A](ga))(f.andThen(gToF.apply[A])))

  def raiseError[A](e: E): G[A] =
    fToG.apply[A](instance.raiseError(e))
}
