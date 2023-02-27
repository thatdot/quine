package com.thatdot.quine.graph.edgecollection

import scala.concurrent.Future
import scala.language.higherKinds

import com.thatdot.quine.model.{DomainEdge, EdgeDirection, GenericEdge, HalfEdge, QuineId}

abstract class AbstractEdgeCollection[F[_], S[_]] {

  def addEdge(edge: HalfEdge): F[Unit]

  def deleteEdge(edge: HalfEdge): F[Unit]

  def all: S[HalfEdge]

  def nonEmpty: F[Boolean]

  def edgesByType(edgeType: Symbol): S[HalfEdge]

  def edgesByDirection(direction: EdgeDirection): S[HalfEdge]

  def edgesByQid(thatId: QuineId): S[GenericEdge]

  def qidsByTypeAndDirection(edgeType: Symbol, direction: EdgeDirection): S[QuineId]

  def directionsByTypeAndQid(edgeType: Symbol, thatId: QuineId): S[EdgeDirection]

  def typesByDirectionAndQid(direction: EdgeDirection, thatId: QuineId): S[Symbol]

  def contains(edgeType: Symbol, direction: EdgeDirection, thatId: QuineId): F[Boolean]

  def hasUniqueGenEdges(thisQid: QuineId, requiredEdges: Iterable[DomainEdge]): F[Boolean]
}

abstract class SyncEdgeCollection extends AbstractEdgeCollection[Identity, Iterator]
abstract class AsyncEdgeCollection extends AbstractEdgeCollection[Future, NoMatSource]
