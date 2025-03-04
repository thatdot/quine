package com.thatdot.quine.util

import scala.concurrent.Future
import scala.reflect.ClassTag
import scala.util.Either

import org.apache.pekko.stream.scaladsl.{Sink, Source}
import org.apache.pekko.stream.{FlowShape, Graph, Materializer, SourceRef}

import cats.Applicative

import com.thatdot.quine.util.InterpM.liftUnsafe

/** Interpreter monad for concurrent operations . This is a monad that represents a computation that can fail with an error.
  * The Source wrapped in the Future should be assumed to be side-effecting, and combining ConcurrentMs should never cause
  * their Sources to be run.
  *
  * This monad exists primarily to allow for the composition of effects in a way that lends itself to
  * structured error handling. Namely, the error argument `E` is exposed as an explicit type parameter so that
  * consumers of this monad can choose an error type that is serializable across a network boundary.
  *
  * NB the primary constructor is private -- this is to enforce consistent style in construction.
  *
  * Note: Any errors that are encountered during the execution of this InterpM that are not of type `E`
  * will be treated as exceptions, in the same way that `Source` handles exceptions.
  *
  * @param source the underlying computation, which is represented by a source. Internally, this uses the exception
  *               handling from `Source`, but the external interface for this class forces you to handle errors of
  *               type E when using this monad.
  * @tparam E the type of error that can be raised
  * @tparam S probably QueryContext, but occasionally an equivalent representation like Vector[Value]
  */
class InterpM[E <: AnyError: ClassTag, S] private (private val source: Source[S, _]) {

  /** A safe version of this `InterpM` that will not produce errors other than errors of type `E`
    */
  private def makeSafe(cleanError: Throwable => E): InterpM[E, S] =
    mapSource(_.mapError { case e =>
      cleanError(e)
    })

  /** The underlying source that you can use without forcing you to handle errors
    */
  @inline
  def unsafeSource: Source[S, _] = source

  /** @see [[Source.runWith]]
    */
  def runWith[A](sink: Sink[S, Future[A]])(implicit mat: Materializer): Future[Either[E, A]] =
    source
      .runWith(sink)
      .map(Right.apply)(mat.executionContext)
      .recover { case e: E =>
        Left(e)
      }(mat.executionContext)

  /** @see [[Source.runWith]]
    */
  def runWith(sink: Sink[Either[E, S], SourceRef[Either[E, S]]])(implicit mat: Materializer): SourceRef[Either[E, S]] =
    source
      .map(Right.apply)
      .recover { case e: E =>
        Left(e)
      }
      .runWith(sink)

  /** Like `runWith`, but accepts a function that maps errors to `S` so the resulting SourceRef will not contain an either
    */
  def runWith(sink: Sink[S, SourceRef[S]], handler: E => S)(implicit mat: Materializer): SourceRef[S] = source
    .recover { case e: E =>
      handler(e)
    }
    .runWith(sink)

  /** @see [[Source.via]]
    */
  def via[T](flow: Graph[FlowShape[S, T], _]): InterpM[E, T] = mapSource(_.via(flow))

  /** map over the stream
    * @see [[Source.map]]
    */
  def map[S2](f: S => S2): InterpM[E, S2] = mapSource(_.map(f))

  def traverse[S2](f: S => Either[E, S2]): InterpM[E, S2] = flatMap {
    f(_) match {
      case Left(err) => InterpM.error(err)
      case Right(value) => InterpM.single(value)
    }
  }

  /** flatmap over the stream
    * @see [[Source.flatMapConcat]]
    */
  def flatMap[S2](f: S => InterpM[E, S2]): InterpM[E, S2] =
    mapSource(_.flatMapConcat(r => f(r).source))

  private def mapSource[S2](f: Source[S, _] => Source[S2, _]): InterpM[E, S2] = liftUnsafe(f(source))

  /** orElse over the stream
    * @see [[Source.orElse]]
    */
  def orElse[S2 >: S](useThisIfEmpty: InterpM[E, S2]): InterpM[E, S2] =
    mapSource(_.orElse(useThisIfEmpty.source))

  /** Converts this InterpM to an interpM with types `E2` and `S2`
    * Note, this should only be used when the specific type of this is known to be an `InterpM[E2,S2]`
    * as it will throw an exception if it is not the correct type
    * @throws java.lang.ClassCastException
    */
  def collectType[E2 <: AnyError, S2]: InterpM[E2, S2] = asInstanceOf[InterpM[E2, S2]]

  /** @see [[Source.++]]
    */
  def ++[S2 <: S](other: InterpM[E, S2]): InterpM[E, S] = new InterpM[E, S](source ++ other.source)

  /** @see [[Source.take]]
    */
  def take(n: Long): InterpM[E, S] = mapSource(_.take(n))

  /** @see [[Source.fold]]
    */
  def fold[T](acc: T)(f: (T, S) => T): InterpM[E, T] = mapSource(_.fold(acc)(f))

  /** Like fold, but takes a function that results in an Either. Will short circuit if `f` ever returns a left value
    */
  def foldM[T](acc: T)(f: (T, S) => Either[E, T]): InterpM[E, T] = fold(acc) {
    f(_, _) match {
      case Left(err) => throw err
      case Right(value) => value
    }
  }

  /** @see [[Source.filter]]
    */
  def filter(f: S => Boolean): InterpM[E, S] = mapSource(_.filter(f))

  /** @see [[Source.mapConcat]]
    */
  def mapConcat[S2](f: S => IterableOnce[S2]): InterpM[E, S2] = mapSource(_.mapConcat(f))

  /** @see [[Source.drop]]
    */
  def drop(n: Long): InterpM[E, S] = mapSource(_.drop(n))

  /** @see [[Source.named]]
    */
  def named(name: String): InterpM[E, S] = mapSource(_.named(name))

  /** @see [[Source.concat]]
    */
  def concat[S2 <: S](other: InterpM[E, S2]): InterpM[E, S] = new InterpM[E, S](source.concat(other.source))

  def filterEither(f: S => Either[E, Boolean]): InterpM[E, S] = new InterpM[E, S](source.filter { s =>
    f(s) match {
      case Left(err) => throw err
      case Right(b) => b
    }
  })

}

object InterpM {

  implicit def applicativeForInterpM[E <: AnyError: ClassTag]: Applicative[InterpM[E, *]] =
    new Applicative[InterpM[E, *]] {
      override def product[A, B](fa: InterpM[E, A], fb: InterpM[E, B]): InterpM[E, (A, B)] = fa.flatMap { a =>
        fb.map(b => (a, b))
      }

      override def pure[A](a: A): InterpM[E, A] = InterpM.single(a)

      override def map[A, B](fa: InterpM[E, A])(f: A => B): InterpM[E, B] = fa.map(f)

      override def ap[A, B](ff: InterpM[E, A => B])(fa: InterpM[E, A]): InterpM[E, B] = fa.flatMap { a =>
        ff.map(_(a))
      }
    }

  /** @see [[Source.empty]]
    */
  def empty[E <: AnyError: ClassTag, S]: InterpM[E, S] = liftUnsafe(Source.empty)

  /** Helper to create [[InterpM]] from `Iterable`.
    * Example usage: `Source(Seq(1,2,3))`
    *
    * Starts a new `InterpM` from the given `Iterable`. This is like starting from an
    * Iterator, but every Subscriber directly attached to the Publisher of this
    * stream will see an individual flow of elements (always starting from the
    * beginning) regardless of when they subscribed.
    */
  def apply[E <: AnyError: ClassTag, S](it: scala.collection.immutable.Iterable[S]): InterpM[E, S] = liftUnsafe(
    Source(it),
  )

  /** @see [[Source.fromIterator]]
    */
  def fromIterator[E <: AnyError: ClassTag, S](f: () => Iterator[S]): InterpM[E, S] = liftUnsafe(Source.fromIterator(f))

  /** @see [[Source.single]]
    */
  def single[E <: AnyError: ClassTag, S](s: S): InterpM[E, S] = liftUnsafe(Source.single(s))

  /** Lifts a Future[S] into an InterpM[E,S] without checking for errors
    * This should only be used for sources that are known to not throw exceptions other than `E`
    */
  def liftFutureUnsafe[E <: AnyError: ClassTag, S](f: Future[S]): InterpM[E, S] = liftUnsafe(Source.future(f))

  /** Lifts a Either[E, S] into an InterpM[E,S]
    */
  def lift[E <: AnyError: ClassTag, S](r: Either[E, S]): InterpM[E, S] = r match {
    case Left(err) => error(err)
    case Right(value) => single(value)
  }

  /** Safely lifts a future into an exception, using cleanErrors to handle all errors
    */
  def liftFuture[E <: AnyError: ClassTag, S](f: Future[S], cleanError: Throwable => E): InterpM[E, S] =
    liftFutureUnsafe[E, S](f).makeSafe(cleanError)

  /** Lifts a Future[InterpM[S]] into an InterpM[E,S] without checking for errors
    * This should only be used for sources that are known to not throw exceptions other than `E`
    */
  def futureInterpMUnsafe[E <: AnyError: ClassTag, S](f: Future[InterpM[E, S]]): InterpM[E, S] =
    InterpM.liftFutureUnsafe(f).flatMap(identity)

  /** Lifts a Future[Source[S]] into an InterpM[E,S] without checking for errors
    * This should only be used for sources that are known to not throw exceptions other than `E`
    */
  def futureSourceUnsafe[E <: AnyError: ClassTag, S](f: Future[Source[S, _]]): InterpM[E, S] =
    InterpM.liftUnsafe(Source.futureSource(f))

  /** Safely lifts a FutureResult[E,S] into an InterpM
    */
  def future[E <: AnyError: ClassTag, S](f: FutureResult[E, S]): InterpM[E, S] = liftUnsafe(
    Source.future(f.unsafeFuture),
  )

  /** @see [Source.lazySource]
    */
  def lazyInterpM[E <: AnyError: ClassTag, S](interp: () => InterpM[E, S]): InterpM[E, S] = new InterpM(
    Source.lazySource { () =>
      interp().source
    },
  )

  /** @see [Source.lazyFuture]
    */
  def lazyFutureInterpMUnsafe[E <: AnyError: ClassTag, S](interp: () => Future[InterpM[E, S]]): InterpM[E, S] =
    new InterpM(Source.lazyFuture { () =>
      interp()
    }).flatMap(identity)

  /** @see [Source.lazyFutureSource]
    */
  def lazyFutureSourceUnsafe[E <: AnyError: ClassTag, S](interp: () => Future[Source[S, _]]): InterpM[E, S] =
    new InterpM(Source.lazyFutureSource(interp))

  /** Lifts a Source[S, _] into an InterpM[E,S] without checking for errors
    * This should only be used for sources that are known to not throw exceptions other than `E`
    */

  def liftUnsafe[E <: AnyError: ClassTag, S](source: Source[S, _]): InterpM[E, S] = new InterpM(source)

  /** Lifts a Source[S, _] into a InterpM[E,S] using cleanErrors to safely handle exceptions
    */
  def lift[E <: AnyError: ClassTag, S](s: Source[S, _], cleanError: Throwable => E): InterpM[E, S] = new InterpM[E, S](
    s.mapError { case e =>
      cleanError(e)
    },
  )

  /** Constructs a InterpM using a thunk that can raise an exception
    */
  def liftUnsafeThunk[E <: AnyError: ClassTag, S](s: => InterpM[E, S]): InterpM[E, S] = try s
  catch {
    case e: E => InterpM.error(e)
  }

  /** Returns an InterpM that failed with error `e`
    */
  def error[E <: AnyError: ClassTag, S](e: E): InterpM[E, S] = new InterpM[E, S](Source.failed(e))

  /** Returns an InterpM that contains an error that may be treated as an exception
    * This should only be used for errors that are truly considered exceptional and not an expected possible result
    * If `e` is of type `E` then this is equivalent to `Source.error(e)`
    */
  def raise[E <: AnyError: ClassTag, S](e: Throwable): InterpM[E, S] = new InterpM[E, S](Source.failed(e))
}
