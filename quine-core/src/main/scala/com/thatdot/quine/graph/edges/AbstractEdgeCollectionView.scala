package com.thatdot.quine.graph.edges

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.jdk.CollectionConverters._

import org.apache.pekko.NotUsed
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.{Source, StreamConverters}

import com.thatdot.common.quineid.QuineId
import com.thatdot.quine.model.{DomainEdge, EdgeDirection, GenericEdge, HalfEdge}

trait AbstractEdgeCollectionView {

  type F[A]
  type S[A]

  def thisQid: QuineId
  def all: S[HalfEdge]

  def size: F[Int]

  def nonEmpty: F[Boolean]

  def edgesByType(edgeType: Symbol): S[HalfEdge]

  def edgesByDirection(direction: EdgeDirection): S[HalfEdge]

  def edgesByQid(thatId: QuineId): S[GenericEdge]

  def qidsByTypeAndDirection(edgeType: Symbol, direction: EdgeDirection): S[QuineId]

  def directionsByTypeAndQid(edgeType: Symbol, thatId: QuineId): S[EdgeDirection]

  def typesByDirectionAndQid(direction: EdgeDirection, thatId: QuineId): S[Symbol]

  def contains(halfEdge: HalfEdge): F[Boolean]

  def hasUniqueGenEdges(requiredEdges: Iterable[DomainEdge]): F[Boolean]

  protected[graph] def toSerialize: Iterable[HalfEdge]

  // Temporary helpers to block for Futures / Streams in async cases.
  def toSyncFuture[A](f: F[A]): A

  def toSyncStream[A](s: S[A]): Iterator[A]

  protected def sMap[A, B](stream: S[A])(f: A => B): S[B]

  protected def sFilter[A](stream: S[A])(pred: A => Boolean): S[A]

  protected def mkStream[A](elem: F[A]): S[A]

  def byComponents(edgeType: Option[Symbol], direction: Option[EdgeDirection], thatId: Option[QuineId]): S[HalfEdge] =
    (edgeType, direction, thatId) match {
      case (None, None, None) => all
      case (Some(t), None, None) => edgesByType(t)
      case (Some(t), Some(d), None) => sMap(qidsByTypeAndDirection(t, d))(HalfEdge(t, d, _))
      case (Some(t), Some(d), Some(id)) =>
        // Turn the Boolean response of `contains` into a Stream[Boolean] of a single element, filter out that
        // element if it's false, and then replace the remaining element (if present) with the HalfEdge in question
        val edge = HalfEdge(t, d, id)
        sMap(sFilter(mkStream(contains(edge)))(identity))(_ => edge)
      case (None, Some(d), None) => edgesByDirection(d)
      case (None, Some(d), Some(id)) => sMap(typesByDirectionAndQid(d, id))(HalfEdge(_, d, id))
      case (None, None, Some(id)) => sMap(edgesByQid(id))(_.toHalfEdge(id))
      case (Some(t), None, Some(id)) => sMap(directionsByTypeAndQid(t, id))(HalfEdge(t, _, id))
    }
}

trait AbstractEdgeCollection extends AbstractEdgeCollectionView {
  def addEdge(edge: HalfEdge): F[Unit]

  def removeEdge(edge: HalfEdge): F[Unit]

}
object AbstractEdgeCollection {
  // A way to expose the type members as type parameters to use from tests
  type Aux[F0[_], S0[_]] = AbstractEdgeCollection {
    type F[A] = F0[A]
    type S[A] = S0[A]
  }
}

trait SyncEdgeCollectionView extends AbstractEdgeCollectionView {
  type F[A] = A
  type S[A] = Iterator[A]
}
trait SyncEdgeCollection extends SyncEdgeCollectionView with AbstractEdgeCollection {
  def toSyncFuture[A](f: A): A = f
  def toSyncStream[A](s: Iterator[A]): Iterator[A] = s
  protected def sMap[A, B](stream: Iterator[A])(f: A => B): Iterator[B] = stream.map(f)
  protected def sFilter[A](stream: Iterator[A])(pred: A => Boolean): Iterator[A] = stream.filter(pred)
  protected def mkStream[A](elem: A): Iterator[A] = Iterator.single(elem)
}

trait AsyncEdgeCollectionView extends AbstractEdgeCollectionView {
  type F[A] = Future[A]
  type S[A] = Source[A, NotUsed]
}

abstract class AsyncEdgeCollection(materializer: Materializer, awaitDuration: Duration = 5.seconds)
    extends AsyncEdgeCollectionView
    with AbstractEdgeCollection {
  def toSyncFuture[A](f: Future[A]): A = Await.result(f, awaitDuration)

  def toSyncStream[A](s: Source[A, NotUsed]): Iterator[A] =
    s.runWith(StreamConverters.asJavaStream[A]())(materializer).iterator.asScala

  protected def sMap[A, B](stream: Source[A, NotUsed])(f: A => B): Source[B, NotUsed] = stream.map(f)
  protected def sFilter[A](stream: Source[A, NotUsed])(pred: A => Boolean): Source[A, NotUsed] = stream.filter(pred)

  protected def mkStream[A](elem: Future[A]): Source[A, NotUsed] = Source.future(elem)
  protected[graph] def toSerialize: Iterable[HalfEdge] = Iterable.empty
}
