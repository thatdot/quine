package com.thatdot.quine.util

import java.util.UUID

import scala.jdk.CollectionConverters.IterableHasAsJava

import com.google.common.hash.{Funnel, PrimitiveSink}
import shapeless.Lazy

import com.thatdot.quine.graph.MultipleValuesStandingQueryPartId
import com.thatdot.quine.graph.cypher.MultipleValuesStandingQuery.LocalProperty
import com.thatdot.quine.graph.cypher.{Columns, Expr, MultipleValuesStandingQuery}
import com.thatdot.quine.model.{EdgeDirection, HalfEdge, QuineId}

/** Guava Funnel instances. These are grouped into traits to allow for easy import of only the funnels related to
  * a specific domain. Each trait is accompanied by an object mixing in that trait, so funnels can be imported as
  * `with SomeFunnels` or `import SomeFunnels._`.
  * Additionally, the `all` object is provided to import all funnels at once, and the `syntax` object provides
  * syntax extensions to make it easier to manipulate Guava Funnels.
  */
//noinspection UnstableApiUsage
object Funnels {
  object syntax {

    /** "Chainable" sink -- allows single abstract method (SAM) syntax to work on functions returning the PrimitiveSink
      */
    trait CFunnel[A] extends com.google.common.hash.Funnel[A] {
      final def funnel(from: A, into: PrimitiveSink): Unit = {
        val _ = cfunnel(from, into)
      }
      def cfunnel(from: A, into: PrimitiveSink): PrimitiveSink
    }

    /** "Flipped" sink -- reverses the argument order to make SAM and chaining syntax more conveniently-accessible
      */
    trait FFunnel[A] extends CFunnel[A] {
      final def cfunnel(from: A, into: PrimitiveSink): PrimitiveSink = ffunnel(into, from)
      def ffunnel(into: PrimitiveSink, from: A): PrimitiveSink
    }

    /** Syntax extension adding a chainable `put` to `PrimitiveSink`, additionally retaining the type specificity
      * of the sink (eg, `(h: Hasher).put(x)` returns a Hasher, not just a PrimitiveSink).
      */
    implicit class PrimitiveSinkOps[S <: PrimitiveSink](val sink: S) {
      def put[T: Funnel](t: T): S = {
        implicitly[Funnel[T]].funnel(t, sink)
        sink
      }
      def putAll[T: Funnel](nameLiteral: String, iter: Iterable[T]): S = {
        sink
          .putInt(nameLiteral.hashCode)
          .putInt(iter.size)
          .put(iter) { (iter, sink) =>
            com.google.common.hash.Funnels.sequentialFunnel(implicitly[Funnel[T]]).funnel(iter.asJava, sink)
          }
        sink
      }
    }

    /** Like [[PrimitiveSinkOps]] but supports [[Lazy]] funnels -- this can be useful when defining recursive funnels
      */
    implicit class RecursivePrimitiveSinkOps[S <: PrimitiveSink](val sink: S) {
      def putLazy[T](t: T)(implicit lazyFunnel: Lazy[Funnel[T]]): S =
        sink.put(t)(lazyFunnel.value)
      def putAllLazy[T](nameLiteral: String, iter: Iterable[T])(implicit lazyFunnel: Lazy[Funnel[T]]): S =
        sink.putAll(nameLiteral, iter)(lazyFunnel.value)
    }
  }

  /** Funnel instances for domain-agnostic types. Assumes that [[String.hashCode]] is stable.
    */
  trait BasicFunnels {
    import com.thatdot.quine.util.Funnels.syntax._

    implicit final val funnelSymbol: FFunnel[Symbol] = (sink, symbol) =>
      sink.putInt("Symbol".hashCode).putUnencodedChars(symbol.name)

    implicit final val funnelUuid: FFunnel[UUID] = (sink, value) => {
      sink
        .putInt("UUID".hashCode)
        .putLong(value.getMostSignificantBits)
        .putLong(value.getLeastSignificantBits)
    }

  }
  object BasicFunnels extends BasicFunnels

  /** Funnels which process multiple values without any additional marking. In particular, these do not
    * indicate that they are collections, and so are particularly prone to collisions. When using these,
    * either make sure you are in a context where there is already a negligible risk of collision, or else
    * prefix these with a tag that is unique to the context. Funnels defined here are left explicit.
    */
  trait UntaggedCompositeFunnels {
    import com.thatdot.quine.util.Funnels.syntax._
    protected[this] def funnelUntaggedTuple[A: Funnel, B: Funnel]: FFunnel[(A, B)] = (sink, tuple) => {
      val (a, b) = tuple
      sink.put(a).put(b)
    }
  }
  object UntaggedCompositeFunnels extends UntaggedCompositeFunnels

  /** Funnels for core Quine model types. Assumes that [[String.hashCode]] is stable.
    */
  trait QuineFunnels extends BasicFunnels {
    import com.thatdot.quine.util.Funnels.syntax._
    implicit final val funnelId: FFunnel[QuineId] = (sink, id) => sink.putInt("QuineId".hashCode).putBytes(id.array)

    implicit final val funnelDirection: FFunnel[EdgeDirection] = (sink, direction) => {
      sink.putInt("EdgeDirection".hashCode)
      direction match {
        case EdgeDirection.Outgoing => sink.putInt("Outgoing".hashCode)
        case EdgeDirection.Incoming => sink.putInt("Incoming".hashCode)
        case EdgeDirection.Undirected => sink.putInt("Undirected".hashCode)
      }
    }
    implicit final val funnelHalfEdge: FFunnel[HalfEdge] = (sink, edge) => {
      sink.putInt("HalfEdge".hashCode).put(edge.direction).put(edge.edgeType).put(edge.other)
    }
  }
  object QuineFunnels extends QuineFunnels

  /** Funnels for Cypher values, queries, expressions, etc. Assumes that [[String.hashCode]] and
    * [[java.time.ZoneId.hashCode]] are stable across JVM instances and versions.
    */
  trait CypherFunnels extends BasicFunnels {
    import com.thatdot.quine.util.Funnels.syntax._
    // Explicit type bound so that this can also resolve to eg `Funnel[Value]`
    implicit def funnelCypherExpr[E <: Expr]: CFunnel[E] = _.addToHasher(_)
    implicit final val funnelColumns: FFunnel[Columns] = (sink, columns) => {
      columns match {
        case Columns.Omitted => sink.putInt("Omitted".hashCode)
        case Columns.Specified(variables) => sink.putAll("Specified", variables)
      }
    }
  }
  object CypherFunnels extends CypherFunnels

  /** Funnels for MultipleValues standing queries and related types. Assumes that [[String.hashCode]] is stable.
    */
  trait MultipleValuesFunnels extends QuineFunnels with CypherFunnels with UntaggedCompositeFunnels {
    import com.thatdot.quine.util.Funnels.syntax._

    implicit final val funnelValueConstraint: FFunnel[LocalProperty.ValueConstraint] = (sink, constraint) => {
      sink.putInt("ValueConstraint".hashCode)
      constraint match {
        case LocalProperty.Equal(equalTo) =>
          sink
            .putInt("Equal".hashCode)
            .put(equalTo)
        case LocalProperty.NotEqual(notEqualTo) =>
          sink
            .putInt("NotEqual".hashCode)
            .put(notEqualTo)
        case LocalProperty.Unconditional =>
          sink.putInt("Unconditional".hashCode)
        case LocalProperty.Any =>
          sink.putInt("Any".hashCode)
        case LocalProperty.None =>
          sink.putInt("None".hashCode)
        case LocalProperty.Regex(pattern) =>
          sink
            .putInt("Regex".hashCode)
            .putUnencodedChars(pattern)
        case LocalProperty.ListContains(mustContain) =>
          sink
            .putInt("ListContains".hashCode)
            .putAll("MustContain", mustContain)
      }
    }

    implicit final val funnelPartId: FFunnel[MultipleValuesStandingQueryPartId] = (sink, partId) =>
      sink.putInt("MultipleValuesStandingQueryPartId".hashCode).put(partId.uuid)

    implicit final val funnelMvsq: FFunnel[MultipleValuesStandingQuery] = (sink, mvsq) =>
      mvsq match {
        case MultipleValuesStandingQuery.UnitSq() => sink.putInt("UnitSq".hashCode)
        case MultipleValuesStandingQuery.Cross(queries, emitSubscriptionsLazily, columns) =>
          sink
            .putInt("Cross".hashCode)
            .putAllLazy("Queries", queries)
            .putBoolean(emitSubscriptionsLazily)
            .put(columns)
        case MultipleValuesStandingQuery.AllProperties(aliasedAs, columns) =>
          sink
            .putInt("AllProperties".hashCode)
            .put(aliasedAs)
            .put(columns)
        case LocalProperty(propKey, propConstraint, aliasedAs, columns) =>
          sink
            .putInt("LocalProperty".hashCode)
            .put(propKey)
            .put(propConstraint)
            .putAll("AliasedAs", aliasedAs)
            .put(columns)
        case MultipleValuesStandingQuery.LocalId(aliasedAs, formatAsString, columns) =>
          sink
            .putInt("LocalId".hashCode)
            .put(aliasedAs)
            .putBoolean(formatAsString)
            .put(columns)
        case MultipleValuesStandingQuery.SubscribeAcrossEdge(edgeName, edgeDirection, andThen, columns) =>
          sink
            .putInt("SubscribeAcrossEdge".hashCode)
            .putAll("Name", edgeName)
            .putAll("Direction", edgeDirection)
            .putLazy(andThen)
            .put(columns)
        case MultipleValuesStandingQuery.EdgeSubscriptionReciprocal(halfEdge, andThenId, columns) =>
          sink
            .putInt("EdgeSubscriptionReciprocal".hashCode)
            .put(halfEdge)
            .put(andThenId)
            .put(columns)
        case MultipleValuesStandingQuery.FilterMap(condition, toFilter, dropExisting, toAdd, columns) =>
          sink
            .putInt("FilterMap".hashCode)
            .putAll("Condition", condition)
            .putLazy(toFilter)
            .putBoolean(dropExisting)
            .putAll("ToAdd", toAdd)(funnelUntaggedTuple)
            .put(columns)
      }
  }
  object MultipleValuesFunnels extends MultipleValuesFunnels

  object all extends QuineFunnels with CypherFunnels with MultipleValuesFunnels
}
