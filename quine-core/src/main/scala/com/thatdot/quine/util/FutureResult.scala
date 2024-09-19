package com.thatdot.quine.util
import scala.concurrent.duration.Duration
import scala.concurrent.{Awaitable, CanAwait, ExecutionContext, Future}
import scala.reflect.ClassTag
import scala.util.{Failure, Success}

/** A monad for futures with explicit error handling. This is for futures where errors are an expected, valid result in
  * some cases. For example, if the future must parse user input, then a parse error is a valid result and the code
  * that handles the result should be explicitly expected to handle that error case.
  *
  * If `Await.result` is called on this an the the underlying future throws an exception that is not of type `E`,
  * then `Await.result` will throw that exception
  *
  * @tparam E the error type that this FutureResult may return. This type represents errors that may occur when the code is
  *           behaving correctly as expected
  * @tparam A the result of this future if it returns a valid result
  */
final class FutureResult[E <: AnyError: ClassTag, A](private val future: Future[A]) extends Awaitable[Either[E, A]] {

  /** @see [[future.flatMap]]
    */
  def flatMap[B](f: A => FutureResult[E, B])(implicit ec: ExecutionContext): FutureResult[E, B] =
    new FutureResult[E, B](
      future.flatMap(a => f(a).future),
    )

  /** @see [[future.map]]
    */
  def map[B](f: A => B)(implicit ec: ExecutionContext): FutureResult[E, B] = new FutureResult(future.map(f))

  /** Should only be used when we know this contains a future of type `B`
    * @see [[future.mapTo]]
    * @throws ClassCastException
    */
  def mapTo[E2 <: AnyError: ClassTag, B: ClassTag](): FutureResult[E2, B] = new FutureResult(future.mapTo[B])

  /** The unsafe, underlying future.
    */
  def unsafeFuture: Future[A] = future

  /** @see [[Future.ready]] */
  override def ready(atMost: Duration)(implicit permit: CanAwait): this.type = {
    future.ready(atMost);
    this
  }

  /** @see [[Future.result]] */
  override def result(atMost: Duration)(implicit permit: CanAwait): Either[E, A] = try Right(future.result(atMost))
  catch {
    case e: E => Left(e)
  }

  /** @see [Future.onComplete]
    */
  def onComplete[U](f: FutureResult.Result[E, A] => U)(implicit ec: ExecutionContext): Unit =
    future.onComplete {
      case Failure(err: E) => f(FutureResult.Failure(err))
      case Failure(err) => f(FutureResult.ExceptionalFailure(err))
      case Success(result) => f(FutureResult.Success(result))
    }
}

object FutureResult {

  /** Represents the possible results of an execution of a FutureResult
    * Similar to [[Future.Result]], except that it seperates failures into `Failures` that represent an
    * expected possible failure of type `E` and a true exception as `ExceptionalFailure`
    */
  sealed trait Result[E, A]
  case class Success[E, A](result: A) extends Result[E, A]
  case class Failure[E, A](err: E) extends Result[E, A]
  case class ExceptionalFailure[E, A](err: Throwable) extends Result[E, A]

  /** Lifts a future[A] into an FutureResult[E,A] without checking for errors
    * This should only be used for futures that are known to not throw exceptions other than `E`
    */
  def liftUnsafe[E <: AnyError: ClassTag, A](f: Future[A]): FutureResult[E, A] = new FutureResult[E, A](f)
  def lift[E <: AnyError: ClassTag, A](result: Either[E, A]): FutureResult[E, A] = result match {
    case Left(err) => failed(err)
    case Right(value) => successful(value)
  }

  /** @see [[Future.successful]]
    */
  def successful[E <: AnyError: ClassTag, A](result: A): FutureResult[E, A] = liftUnsafe(Future.successful(result))

  /** @see [[Future.failed]]
    */
  def failed[E <: AnyError: ClassTag, A](err: E): FutureResult[E, A] = liftUnsafe(Future.failed(err))
}
