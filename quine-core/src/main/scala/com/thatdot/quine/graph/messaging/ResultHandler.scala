package com.thatdot.quine.graph.messaging

import scala.concurrent.{Future, Promise}
import scala.reflect.{ClassTag, classTag}
import scala.util.control.NonFatal
import scala.util.{Failure, Success}

import org.apache.pekko.NotUsed
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.scaladsl.{Flow, Source, StreamRefs}
import org.apache.pekko.stream.{Materializer, StreamRefResolver}

import com.thatdot.quine.graph.BaseGraph
import com.thatdot.quine.util.{AnyError, BaseError, FutureResult, InterpM}

trait ResultHandler[Response] {

  /** Respond to an ask by sending a reply back to `to`
    *
    * @param to destination of message
    * @param response message to send
    * @param graph current graph
    * @param responseStaysWithinJvm responses within the JVM, can send references directly
    * @param ec
    */
  def respond(
    to: QuineRef,
    response: Response,
    graph: BaseGraph,
    responseStaysWithinJvm: Boolean,
  )(implicit
    mat: Materializer,
  ): Unit

  /** Receive a response to an ask
    *
    * @param response received response message
    * @param promise promise to succeed
    * @param system
    */
  def receiveResponse(response: QuineResponse, promise: Promise[Response])(implicit
    system: ActorSystem,
  ): Unit
}

object ResultHandler {
  implicit def forId[A <: QuineMessage: ClassTag]: ResultHandler[A] = new ResultHandler[A] {
    def respond(
      to: QuineRef,
      response: A,
      graph: BaseGraph,
      responseStaysWithinJvm: Boolean,
    )(implicit
      mat: Materializer,
    ): Unit =
      graph.relayTell(to, BaseMessage.Response(QuineResponse.Success(response)))

    def receiveResponse(qr: QuineResponse, promise: Promise[A])(implicit
      system: ActorSystem,
    ): Unit = qr match {
      case QuineResponse.Success(a: A) => promise.success(a)
      case other =>
        val e = new IllegalArgumentException(s"Expected a single value, not $other")
        promise.failure(e)
    }
  }

  implicit def forFuture[A <: QuineMessage: ClassTag]: ResultHandler[Future[A]] =
    new ResultHandler[Future[A]] {
      def respond(
        to: QuineRef,
        response: Future[A],
        graph: BaseGraph,
        responseStaysWithinJvm: Boolean,
      )(implicit
        mat: Materializer,
      ): Unit =
        if (responseStaysWithinJvm) {
          graph.relayTell(to, BaseMessage.Response(QuineResponse.LocalFuture(response)))
        } else {
          response.onComplete { r =>
            val message = r match {
              case Success(v) => QuineResponse.Success(v)
              case Failure(e) => QuineResponse.ExceptionalFailure(AnyError.fromThrowable(e))
            }
            graph.relayTell(to, BaseMessage.Response(message))
          }(mat.executionContext)
        }

      def receiveResponse(qr: QuineResponse, promise: Promise[Future[A]])(implicit
        system: ActorSystem,
      ): Unit = qr match {
        case QuineResponse.LocalFuture(future) => promise.success(future.mapTo[A])
        case QuineResponse.Success(a: A) => promise.success(Future.successful(a))
        case QuineResponse.Failure(f) => promise.success(Future.failed(f)) //Eventually we should handle this case
        case QuineResponse.ExceptionalFailure(f) => promise.success(Future.failed(f))
        case other =>
          val e = new IllegalStateException(s"Expected a future value, not $other")
          promise.failure(e)
      }
    }

  implicit def forSource[A <: QuineMessage: ClassTag]: ResultHandler[Source[A, NotUsed]] =
    new ResultHandler[Source[A, NotUsed]] {
      def respond(
        to: QuineRef,
        response: Source[A, NotUsed],
        graph: BaseGraph,
        responseStaysWithinJvm: Boolean,
      )(implicit
        mat: Materializer,
      ): Unit =
        if (responseStaysWithinJvm) {
          graph.relayTell(to, BaseMessage.Response(QuineResponse.LocalSource(response)))
        } else {
          val mapped = response.via( // `.via` a named, nested flow (instead of directly `.map`ing) for better errors
            Flow[A]
              .map(r => BaseMessage.Response(QuineResponse.Success(r)))
              .recover { case NonFatal(e) =>
                BaseMessage.Response(
                  QuineResponse.ExceptionalFailure(AnyError.fromThrowable(e)),
                ) //Eventually we should try to go back and prevent this case from happening
              }
              .named(s"result-handler-source-of-${classTag[A].runtimeClass.getSimpleName}"),
          )
          val ref = mapped.runWith(StreamRefs.sourceRef())
          val serialized = StreamRefResolver.get(graph.system).toSerializationFormat(ref)
          graph.relayTell(to, BaseMessage.Response(QuineResponse.StreamRef(serialized)))
        }

      def receiveResponse(qr: QuineResponse, promise: Promise[Source[A, NotUsed]])(implicit
        system: ActorSystem,
      ): Unit = qr match {
        case QuineResponse.LocalSource(source) => promise.success(source.collectType[A])
        case QuineResponse.StreamRef(s) =>
          val ss = StreamRefResolver.get(system).resolveSourceRef[BaseMessage.Response](s).map {
            _.response match {
              case QuineResponse.Success(v: A) => v
              case QuineResponse.Failure(e) => throw e //Eventually we should handle this case
              case QuineResponse.ExceptionalFailure(e) => throw e
              case other => throw new IllegalStateException(s"Expected a success or failure value, not $other")
            }
          }
          promise.success(ss)
        case other =>
          val e = new IllegalStateException(s"Expected a stream value but got $other")
          promise.failure(e)
      }
    }

  implicit def forFutureResult[E <: BaseError: ClassTag, A <: QuineMessage: ClassTag]
    : ResultHandler[FutureResult[E, A]] = new ResultHandler[FutureResult[E, A]] {
    override def respond(to: QuineRef, response: FutureResult[E, A], graph: BaseGraph, responseStaysWithinJvm: Boolean)(
      implicit mat: Materializer,
    ): Unit =
      if (responseStaysWithinJvm) {
        graph.relayTell(to, BaseMessage.Response(QuineResponse.LocalFutureResult(response)))
      } else {
        response.onComplete { r =>
          val message = r match {
            case FutureResult.Success(v) => QuineResponse.Success(v)
            case FutureResult.Failure(e) => QuineResponse.Failure(e)
            case FutureResult.ExceptionalFailure(e) => QuineResponse.ExceptionalFailure(AnyError.fromThrowable(e))
          }
          graph.relayTell(to, BaseMessage.Response(message))
        }(mat.executionContext)
      }
    override def receiveResponse(response: QuineResponse, promise: Promise[FutureResult[E, A]])(implicit
      system: ActorSystem,
    ): Unit = response match {
      case QuineResponse.LocalFutureResult(future) => promise.success(future.mapTo[E, A]())
      case QuineResponse.Success(a: A) => promise.success(FutureResult.successful(a))
      case QuineResponse.Failure(f: E) =>
        promise.success(FutureResult.failed(f)) //Eventually we should handle this case
      case QuineResponse.ExceptionalFailure(f) => promise.failure(f)
      case other =>
        val e = new IllegalStateException(s"Expected a future result value, not $other")
        promise.failure(e)
    }
  }

  implicit def forInterpM[E <: BaseError: ClassTag, A <: QuineMessage: ClassTag]: ResultHandler[InterpM[E, A]] =
    new ResultHandler[InterpM[E, A]] {
      def respond(
        to: QuineRef,
        response: InterpM[E, A],
        graph: BaseGraph,
        responseStaysWithinJvm: Boolean,
      )(implicit
        mat: Materializer,
      ): Unit =
        if (responseStaysWithinJvm) {
          graph.relayTell(to, BaseMessage.Response(QuineResponse.LocalInterpM(response)))
        } else {
          val mapped = response.via( // `.via` a named, nested flow (instead of directly `.map`ing) for better errors
            Flow[A]
              .map(r => BaseMessage.Response(QuineResponse.Success(r)))
              .recover { case NonFatal(e) =>
                BaseMessage.Response(
                  QuineResponse.ExceptionalFailure(AnyError.fromThrowable(e)),
                ) //Eventually we should try to go back and prevent this case from happening
              }
              .named(s"result-handler-source-of-${classTag[A].runtimeClass.getSimpleName}"),
          )
          val ref = mapped.runWith(StreamRefs.sourceRef(), e => BaseMessage.Response(QuineResponse.Failure(e)))
          val serialized = StreamRefResolver.get(graph.system).toSerializationFormat(ref)
          graph.relayTell(to, BaseMessage.Response(QuineResponse.StreamRef(serialized)))
        }

      def receiveResponse(qr: QuineResponse, promise: Promise[InterpM[E, A]])(implicit
        system: ActorSystem,
      ): Unit =
        qr match {
          case QuineResponse.LocalInterpM(c) => promise.success(c.collectType[E, A])
          case QuineResponse.LocalSource(source) => promise.success(InterpM.liftUnsafe(source.collectType[A]))
          case QuineResponse.StreamRef(s) =>
            val ss =
              InterpM.liftUnsafe(StreamRefResolver.get(system).resolveSourceRef[BaseMessage.Response](s)).flatMap {
                _.response match {
                  case QuineResponse.Success(v: A) => InterpM.single(v): InterpM[E, A]
                  case QuineResponse.Failure(e: E) => InterpM.error(e): InterpM[E, A]
                  case QuineResponse.Failure(e) =>
                    throw e //We are passing around an error that this handler is not capable of handling
                  case QuineResponse.ExceptionalFailure(e) => throw e
                  case other => throw new IllegalStateException(s"Expected a success or failure value, not $other")
                }
              }
            promise.success(ss)
          case other =>
            val e = new IllegalStateException(s"Expected a stream value but got $other")
            promise.failure(e)
        }
    }
}
