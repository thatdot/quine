package com.thatdot.quine.graph.edges

import scala.concurrent.Future
import scala.language.higherKinds

import com.thatdot.quine.model.{DomainEdge, EdgeDirection, GenericEdge, HalfEdge, QuineId}

abstract class AbstractEdgeCollectionView[F[_], S[_]] {

  def all: S[HalfEdge]

  def size: F[Long]

  def nonEmpty: F[Boolean]

  def edgesByType(edgeType: Symbol): S[HalfEdge]

  def edgesByDirection(direction: EdgeDirection): S[HalfEdge]

  def edgesByQid(thatId: QuineId): S[GenericEdge]

  def qidsByTypeAndDirection(edgeType: Symbol, direction: EdgeDirection): S[QuineId]

  def directionsByTypeAndQid(edgeType: Symbol, thatId: QuineId): S[EdgeDirection]

  def typesByDirectionAndQid(direction: EdgeDirection, thatId: QuineId): S[Symbol]

  def contains(edgeType: Symbol, direction: EdgeDirection, thatId: QuineId): F[Boolean]

  def hasUniqueGenEdges(requiredEdges: Iterable[DomainEdge]): F[Boolean]
}

abstract class AbstractEdgeCollection[F[_], S[_]] extends AbstractEdgeCollectionView[F, S] {
  def addEdge(edge: HalfEdge): F[Unit]

  def removeEdge(edge: HalfEdge): F[Unit]

}

trait SyncEdgeCollectionView extends AbstractEdgeCollectionView[Identity, Iterator]
trait AsyncEdgeCollectionView extends AbstractEdgeCollectionView[Future, NoMatSource]

trait AsyncEdgeCollection extends AbstractEdgeCollection[Future, NoMatSource]
