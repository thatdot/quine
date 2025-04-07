package com.thatdot.quine.util

import java.net.InetSocketAddress
import java.nio.charset.Charset
import java.nio.file.Path
import java.time.temporal.TemporalUnit

import scala.annotation.unused

import org.apache.pekko.NotUsed
import org.apache.pekko.actor.ActorRef
import org.apache.pekko.stream.scaladsl.Source

import cats.data.NonEmptyList
import com.typesafe.config.ConfigOrigin

import com.thatdot.common.logging.Pretty.PrettyHelper
import com.thatdot.common.quineid.QuineId
import com.thatdot.common.util.ByteConversions
import com.thatdot.quine.graph.behavior.DomainNodeIndexBehavior.SubscribersToThisNodeUtil.DistinctIdSubscription
import com.thatdot.quine.graph.cypher.{
  AllPropertiesState,
  CrossState,
  EdgeSubscriptionReciprocalState,
  Expr,
  LocalIdState,
  LocalPropertyState,
  UnitState,
  Value,
}
import com.thatdot.quine.graph.messaging.StandingQueryMessage.SqResultLike
import com.thatdot.quine.graph.messaging._
import com.thatdot.quine.graph.{
  EventTime,
  GraphQueryPattern,
  MultipleValuesStandingQueryPartId,
  StandingQueryId,
  StandingQueryResult,
  namespaceToString,
}
import com.thatdot.quine.model.{DomainGraphBranch, DomainGraphNode, QuineIdProvider, QuineValue}

object Log {

  import com.thatdot.common.logging.Log._

  trait LowPriorityImplicits {
    implicit def loggableOption[A](implicit loggable: Loggable[A]): Loggable[Option[A]] = new Loggable[Option[A]] {
      override def safe(a: Option[A]): String = a match {
        case None => "None"
        case Some(value) => s"Some(${loggable.safe(value)})"
      }
      override def unsafe(a: Option[A], redactor: String => String): String = a match {
        case None => redactor("None")
        case Some(value) => loggable.unsafe(value, redactor)
      }
    }
    // fallback QuineId log instance for when an idProvider is not in implicit scope
    implicit val LogQuineIdRaw: AlwaysSafeLoggable[QuineId] = _.toString
    // fallback SpaceTimeQuineId log instance for when an idProvider is not in implicit scope
    implicit val LogSpaceTimeQuineId: AlwaysSafeLoggable[com.thatdot.quine.graph.messaging.SpaceTimeQuineId] =
      _.toString

  }

  // General case for compositional Loggables: given a loggable for each component of a composite value,
  // we can make a loggable for that composite value
  trait MediumPriorityStructuralLoggables extends LowPriorityImplicits {

    implicit def loggableMap[K, V, MapT[X, Y] <: scala.collection.Map[X, Y]](implicit
      loggableKey: Loggable[K],
      loggableVal: Loggable[V],
    ): Loggable[MapT[K, V]] = CollectionLoggableImplicits.loggableMap

    implicit def loggableIterable[A, ItT[X] <: scala.collection.Iterable[X]](implicit
      loggableElems: Loggable[A],
    ): Loggable[ItT[A]] = CollectionLoggableImplicits.loggableIterable

    implicit def loggableSet[A, SetT[X] <: scala.collection.Set[X]](implicit
      loggableElems: Loggable[A],
    ): Loggable[SetT[A]] = CollectionLoggableImplicits.loggableSet

    implicit def loggableNonEmptyList[A](implicit loggableElems: Loggable[A]): Loggable[NonEmptyList[A]] =
      CollectionLoggableImplicits.loggableNonEmptyList

    implicit def loggableConcurrentLinkedDeque[A](implicit
      loggableElems: Loggable[A],
    ): Loggable[java.util.concurrent.ConcurrentLinkedDeque[A]] =
      CollectionLoggableImplicits.loggableConcurrentLinkedDeque
  }

  // Special case for compositional Loggables: If all their components are AlwaysSafe, we can make an AlwaysSafe
  trait MediumPrioritySafeLoggables extends MediumPriorityStructuralLoggables {

    implicit def alwaysSafeMap[K, V, MapT[X, Y] <: scala.collection.Map[X, Y]](implicit
      loggableKey: AlwaysSafeLoggable[K],
      loggableVal: AlwaysSafeLoggable[V],
    ): AlwaysSafeLoggable[MapT[K, V]] = AlwaysSafeCollectionImplicits.alwaysSafeMap

    implicit def alwaysSafeIterable[A, ItT[X] <: scala.collection.Iterable[X]](implicit
      loggableElems: AlwaysSafeLoggable[A],
    ): AlwaysSafeLoggable[ItT[A]] = AlwaysSafeCollectionImplicits.alwaysSafeIterable

    implicit def alwaysSafeSet[A, SetT[X] <: scala.collection.Set[X]](implicit
      loggableElems: AlwaysSafeLoggable[A],
    ): AlwaysSafeLoggable[SetT[A]] = AlwaysSafeCollectionImplicits.alwaysSafeSet

    implicit def alwaysSafeNonEmptyList[A](implicit
      loggableElems: AlwaysSafeLoggable[A],
    ): AlwaysSafeLoggable[NonEmptyList[A]] = AlwaysSafeCollectionImplicits.alwaysSafeNonEmptyList
    implicit def alwaysSafeConcurrentLinkedDeque[A](implicit
      loggableElems: AlwaysSafeLoggable[A],
    ): AlwaysSafeLoggable[java.util.concurrent.ConcurrentLinkedDeque[A]] =
      AlwaysSafeCollectionImplicits.alwaysSafeConcurrentLinkedDeque
  }
  // All of the implicit instances of Loggable for primitives and Quine Values.
  // This is put inside of another object so you aren't given all of the implicits every time you import Loggable._
  object implicits extends MediumPrioritySafeLoggables {
    implicit def logExpr(implicit qidLoggable: Loggable[QuineId]): Loggable[com.thatdot.quine.graph.cypher.Expr] = {
      def logExpr(a: Expr, redactor: String => String): String = {
        @inline def prefix = a.getClass.getSimpleName
        @inline def recurse(e: Expr): String = logExpr(e, redactor)

        a match {
          case Expr.Variable(_) =>
            // variable names are safe
            a.toString
          case Expr.Property(expr, key) =>
            s"$prefix(${recurse(expr)}, $key)" // static property keys are safe
          case Expr.Parameter(_) =>
            // parameter indices are safe
            a.toString
          case Expr.ListLiteral(expressions) => s"$prefix(${expressions.map(recurse).mkString(", ")})"
          case Expr.MapLiteral(entries) =>
            // static keys in a map literal are safe
            s"$prefix(${entries.map { case (k, v) => s"$k -> ${recurse(v)}" }.mkString(", ")})"
          case Expr.MapProjection(original, items, includeAllProps) =>
            // static keys in a map projection are safe
            s"$prefix(${recurse(original)}, [${items.map { case (k, v) => s"$k -> ${recurse(v)}" }.mkString(", ")}], includeAllProps=$includeAllProps)"
          case Expr.Function(function, arguments) =>
            // function name is safe
            s"$prefix(${function.name}, Arguments(${arguments.map(recurse).mkString(", ")}))"
          case Expr.ListComprehension(variable, list, filterPredicate, extract) =>
            // static variable name is safe
            s"$prefix($variable, ${recurse(list)}, ${recurse(filterPredicate)}, ${recurse(extract)})"
          case Expr.AllInList(variable, list, filterPredicate) =>
            // static variable name is safe
            s"$prefix($variable, ${recurse(list)}, ${recurse(filterPredicate)})"
          case Expr.AnyInList(variable, list, filterPredicate) =>
            // static variable name is safe
            s"$prefix($variable, ${recurse(list)}, ${recurse(filterPredicate)})"
          case Expr.SingleInList(variable, list, filterPredicate) =>
            // static variable name is safe
            s"$prefix($variable, ${recurse(list)}, ${recurse(filterPredicate)})"
          case Expr.ReduceList(accumulator, initial, variable, list, reducer) =>
            // static variable name and function name are safe
            s"$prefix($accumulator, ${recurse(initial)}, $variable, ${recurse(list)}, ${recurse(reducer)})"
          case Expr.FreshNodeId =>
            // singleton value is safe
            a.toString
          // For all other cases, the type of the AST node is safe, but the child ASTs may not be
          case Expr.DynamicProperty(expr, keyExpr) =>
            s"$prefix(${recurse(expr)}, ${recurse(keyExpr)})"
          case Expr.ListSlice(list, from, to) =>
            s"$prefix(${recurse(list)}, ${from.map(recurse)}, ${to.map(recurse)})"
          case Expr.PathExpression(nodeEdges) =>
            s"$prefix(${nodeEdges.map(recurse).mkString(", ")})"
          case Expr.RelationshipStart(relationship) =>
            s"$prefix(${recurse(relationship)})"
          case Expr.RelationshipEnd(relationship) =>
            s"$prefix(${recurse(relationship)})"
          case Expr.Equal(lhs, rhs) =>
            s"$prefix(${recurse(lhs)}, ${recurse(rhs)})"
          case Expr.Subtract(lhs, rhs) =>
            s"$prefix(${recurse(lhs)}, ${recurse(rhs)})"
          case Expr.Add(lhs, rhs) =>
            s"$prefix(${recurse(lhs)}, ${recurse(rhs)})"
          case Expr.Multiply(lhs, rhs) =>
            s"$prefix(${recurse(lhs)}, ${recurse(rhs)})"
          case Expr.Divide(lhs, rhs) =>
            s"$prefix(${recurse(lhs)}, ${recurse(rhs)})"
          case Expr.Modulo(lhs, rhs) =>
            s"$prefix(${recurse(lhs)}, ${recurse(rhs)})"
          case Expr.Exponentiate(lhs, rhs) =>
            s"$prefix(${recurse(lhs)}, ${recurse(rhs)})"
          case Expr.UnaryAdd(argument) =>
            s"$prefix(${recurse(argument)})"
          case Expr.UnarySubtract(argument) =>
            s"$prefix(${recurse(argument)})"
          case Expr.GreaterEqual(lhs, rhs) =>
            s"$prefix(${recurse(lhs)}, ${recurse(rhs)})"
          case Expr.LessEqual(lhs, rhs) =>
            s"$prefix(${recurse(lhs)}, ${recurse(rhs)})"
          case Expr.Greater(lhs, rhs) =>
            s"$prefix(${recurse(lhs)}, ${recurse(rhs)})"
          case Expr.Less(lhs, rhs) =>
            s"$prefix(${recurse(lhs)}, ${recurse(rhs)})"
          case Expr.InList(element, list) =>
            s"$prefix(${recurse(element)}, ${recurse(list)})"
          case Expr.StartsWith(scrutinee, startsWith) =>
            s"$prefix(${recurse(scrutinee)}, ${recurse(startsWith)})"
          case Expr.EndsWith(scrutinee, endsWith) =>
            s"$prefix(${recurse(scrutinee)}, ${recurse(endsWith)})"
          case Expr.Contains(scrutinee, contained) =>
            s"$prefix(${recurse(scrutinee)}, ${recurse(contained)})"
          case Expr.Regex(scrutinee, regex) =>
            s"$prefix(${recurse(scrutinee)}, ${recurse(regex)})"
          case Expr.IsNotNull(notNull) =>
            s"$prefix(${recurse(notNull)})"
          case Expr.IsNull(isNull) =>
            s"$prefix(${recurse(isNull)})"
          case Expr.Not(negated) =>
            s"$prefix(${recurse(negated)})"
          case Expr.And(conjuncts) =>
            s"$prefix(${conjuncts.map(recurse).mkString(", ")})"
          case Expr.Or(disjuncts) =>
            s"$prefix(${disjuncts.map(recurse).mkString(", ")})"
          case Expr.Case(scrutinee, branches, default) =>
            s"$prefix(${scrutinee.map(recurse)}, {${branches
              .map { case (cond, action) => s"${recurse(cond)} -> ${recurse(action)}" }
              .mkString(", ")}}, ${default.map(recurse)})"
          case value: Value =>
            value match {
              case Expr.True | Expr.False =>
                // In conjunction with variable names and property keys being safe, non-null boolean is UNSAFE.
                // consider: "is_married": true/false
                s"Bool(${redactor(a.toString)})"
              case Expr.Null =>
                // singleton "Null" is safe
                a.toString
              case Expr.Bytes(b, representsId) if representsId =>
                // ID bytes are delegated to LogQuineId
                s"IdBytes(${qidLoggable.unsafe(QuineId(b), redactor)})"
              case Expr.Bytes(b, _) =>
                // non-ID bytes are unsafe, but in case the redactor is a no-op, format them.
                s"$prefix(${redactor(ByteConversions.formatHexBinary(b))})"
              case Expr.List(list) =>
                // NB this exposes the number of elements in the list, but not their values
                s"$prefix(${list.map(recurse).mkString(", ")})"
              case Expr.Map(map) =>
                // map keys may be dynamic/based on PII, so we redact them
                // NB this exposes the number of elements in the map
                s"$prefix(${map.map { case (k, v) => s"${redactor(k)} -> ${recurse(v)}" }.mkString(", ")})"
              case Expr.Node(id, labels, properties) =>
                // ID is delegated to LogQuineId, labels are stringified before redaction, properties are redacted
                s"$prefix(${qidLoggable.unsafe(id, redactor)}, Labels(${redactor(labels.map(_.name).mkString(", "))}), {${properties
                  .map { case (k, v) => s"${redactor(k.name)} -> ${recurse(v)}" }
                  .mkString(", ")}})"
              case Expr.Relationship(start, name, properties, end) =>
                // IDs are delegated to LogQuineId, label is stringified redacted, properties are redacted
                s"$prefix(${qidLoggable.unsafe(start, redactor)}, ${redactor(name.name)}, ${properties
                  .map { case (k, v) => s"${redactor(k.name)} -> ${recurse(v)}" }
                  .mkString(", ")}, ${qidLoggable.unsafe(end, redactor)})"
              case Expr.Path(head, tails) =>
                // flatten the path into an alternating Path(node, edge, node, edge, node...) sequence, redacting all.
                // NB this exposes the number of nodes and edges in the path
                s"$prefix(${(recurse(head) +: tails.flatMap { case (edge, node) => Seq(recurse(edge), recurse(node)) }).mkString(", ")})"
              // For the rest, the type name is safe but the contents are unsafe
              case Expr.Str(string) =>
                s"$prefix(${redactor("\"" + string + "\"")})"
              case Expr.Integer(long) =>
                s"$prefix(${redactor(long.toString)})"
              case Expr.Floating(double) =>
                s"$prefix(${redactor(double.toString)})"
              case Expr.LocalDateTime(localDateTime) =>
                s"$prefix(${redactor(localDateTime.toString)})"
              case Expr.Date(date) =>
                s"$prefix(${redactor(date.toString)})"
              case Expr.Time(time) =>
                s"$prefix(${redactor(time.toString)})"
              case Expr.LocalTime(localTime) =>
                s"$prefix(${redactor(localTime.toString)})"
              case Expr.DateTime(zonedDateTime) =>
                s"$prefix(${redactor(zonedDateTime.toString)})"
              case Expr.Duration(duration) =>
                s"$prefix(${redactor(duration.toString)})"
            }
        }
      }
      Loggable(logExpr)
    }
    implicit val LogValue: Loggable[com.thatdot.quine.graph.cypher.Value] = Loggable(logExpr.unsafe(_, _))
    implicit val LogInt: Loggable[Int] = toStringLoggable[Int]
    implicit val LogBoolean: Loggable[Boolean] = toStringLoggable[Boolean]
    implicit val LogLong: Loggable[Long] = toStringLoggable[Long]
    implicit val LogConfigOrigin: Loggable[ConfigOrigin] = toStringLoggable[ConfigOrigin]
    implicit def SafeLoggable[A]: AlwaysSafeLoggable[Safe[A]] = com.thatdot.common.logging.Log.SafeLoggable

    implicit def logStandingQueryResult(implicit qidLoggable: Loggable[QuineId]): Loggable[StandingQueryResult] =
      Loggable { (result, redactor) =>
        val sanitizedData: Map[Safe[String], Safe[String]] = result.data.view
          .mapValues(logQuineValue(qidLoggable).unsafe(_, redactor))
          .map { case (k, v) =>
            Safe(k) -> Safe(v)
          }
          .toMap

        val sanitizedDataStr: String =
          loggableMap(SafeLoggable[String], SafeLoggable[String]).unsafe(sanitizedData, redactor)

        s"${result.getClass.getSimpleName}(${result.meta}, Data($sanitizedDataStr))"
      }
    implicit val LogPath: Loggable[Path] = toStringLoggable[Path]
    implicit val LogDate: AlwaysSafeLoggable[java.util.Date] = _.toString
    implicit val LogUrl: Loggable[java.net.URL] = toStringLoggable[java.net.URL]
    implicit val LogInetSocketAddress: Loggable[InetSocketAddress] = toStringLoggable[InetSocketAddress]
    implicit val LogEventTime: AlwaysSafeLoggable[EventTime] = _.toString
    implicit val LogTemporalUnit: AlwaysSafeLoggable[TemporalUnit] = Loggable.alwaysSafe[TemporalUnit](_.toString)
    implicit val LogStandingQueryId: AlwaysSafeLoggable[StandingQueryId] =
      Loggable.alwaysSafe[StandingQueryId](_.toString)
    implicit val LogCharset: AlwaysSafeLoggable[Charset] = Loggable.alwaysSafe[Charset](_.toString)
    implicit val LogDistinctIdSubscription: Loggable[DistinctIdSubscription] = toStringLoggable[DistinctIdSubscription]
    implicit val LogUnitState: AlwaysSafeLoggable[UnitState] = _.toString
    implicit val LogCrossState: Loggable[CrossState] = toStringLoggable[com.thatdot.quine.graph.cypher.CrossState]
    implicit val LogAllPropertiesState: Loggable[AllPropertiesState] = toStringLoggable[AllPropertiesState]
    implicit val LogLocalPropertyState: Loggable[LocalPropertyState] = toStringLoggable[LocalPropertyState]
    implicit val LogEdgeSubscriptionReciprocalState: Loggable[EdgeSubscriptionReciprocalState] =
      toStringLoggable[EdgeSubscriptionReciprocalState]
    implicit val LogLocalIdState: AlwaysSafeLoggable[LocalIdState] = _.toString
    implicit val LogSqResultLike: Loggable[SqResultLike] = toStringLoggable[SqResultLike]
    implicit val LogMultipleValuesStandingQueryPartId: AlwaysSafeLoggable[MultipleValuesStandingQueryPartId] =
      Loggable.alwaysSafe[MultipleValuesStandingQueryPartId](_.toString)
    implicit val LogMultipleValuesCompositeId
      : AlwaysSafeLoggable[(StandingQueryId, MultipleValuesStandingQueryPartId)] =
      _.toString
    implicit val LogActorRef: AlwaysSafeLoggable[ActorRef] =
      // not just _.toString because ActorRefs can be null (notably, ActorRef.noSender)
      String.valueOf(_)
    implicit val LogSymbol: Loggable[Symbol] = Loggable((sym, redactor) => redactor(sym.name))
    implicit val LogVersion: AlwaysSafeLoggable[com.thatdot.quine.persistor.Version] =
      Loggable.alwaysSafe[com.thatdot.quine.persistor.Version](_.toString)
    implicit def logQuineIdPretty(implicit idProvider: QuineIdProvider): AlwaysSafeLoggable[QuineId] = _.pretty
    implicit val LogEdgeEvent: Loggable[com.thatdot.quine.graph.EdgeEvent] =
      toStringLoggable[com.thatdot.quine.graph.EdgeEvent]
    implicit val LogFile: Loggable[java.io.File] = toStringLoggable[java.io.File]
    implicit val LogShardRef: Loggable[ShardRef] = toStringLoggable[ShardRef]
    implicit def logSpaceTimeQuineIdPretty(implicit
      idProvider: QuineIdProvider,
    ): AlwaysSafeLoggable[com.thatdot.quine.graph.messaging.SpaceTimeQuineId] =
      _.pretty
    implicit def LogWakefulState[W <: com.thatdot.quine.graph.WakefulState]: AlwaysSafeLoggable[W] =
      _.toString
    implicit val LogActorSelection: Loggable[org.apache.pekko.actor.ActorSelection] =
      toStringLoggable[org.apache.pekko.actor.ActorSelection]

    // Option[Symbol] is too generic a type for which to confidently have an implicit instance
    @unused val LogNamespaceId: AlwaysSafeLoggable[Option[Symbol]] =
      Loggable.alwaysSafe[com.thatdot.quine.graph.NamespaceId](namespaceToString)

    // NB Milliseconds is
    implicit val LogMilliseconds: AlwaysSafeLoggable[com.thatdot.quine.model.Milliseconds] =
      _.toString
    implicit val LogAtTime: AlwaysSafeLoggable[Option[com.thatdot.quine.model.Milliseconds]] =
      _.toString

    implicit def logQuineValue(implicit
      qidLoggable: Loggable[QuineId],
    ): Loggable[com.thatdot.quine.model.QuineValue] = {

      def logQuineValue(qv: QuineValue, redactor: String => String): String = {
        @inline def recurse(qv: QuineValue): String = logQuineValue(qv, redactor)
        val prefix = qv.getClass.getSimpleName
        qv match {
          case QuineValue.Str(string) => s"$prefix(${redactor("\"" + string + "\"")})"
          case QuineValue.Integer(long) => s"$prefix(${redactor(long.toString)})"
          case QuineValue.Floating(double) => s"$prefix(${redactor(double.toString)})"
          case QuineValue.True | QuineValue.False =>
            // In conjunction with variable names and property keys being safe, non-null boolean is UNSAFE.
            // consider: "is_married": true/false
            s"Bool(${redactor(prefix)})"
          case QuineValue.Null =>
            // singleton "null" is safe
            qv.toString
          case QuineValue.Bytes(bytes) => s"$prefix(${redactor(ByteConversions.formatHexBinary(bytes))})"
          case QuineValue.List(list) =>
            // NB this exposes the number of elements in the list
            s"$prefix(${list.map(recurse).mkString(", ")})"
          case QuineValue.Map(map) =>
            // NB this exposes the number of elements in the map
            s"$prefix(${map.map { case (k, v) => s"${redactor(k)} -> ${recurse(v)}" }.mkString(", ")})"
          case QuineValue.DateTime(instant) =>
            s"$prefix(${redactor(instant.toString)})"
          case QuineValue.Duration(duration) =>
            s"$prefix(${redactor(duration.toString)})"
          case QuineValue.Date(date) =>
            s"$prefix(${redactor(date.toString)})"
          case QuineValue.LocalTime(time) =>
            s"$prefix(${redactor(time.toString)})"
          case QuineValue.Time(time) =>
            s"$prefix(${redactor(time.toString)})"
          case QuineValue.LocalDateTime(localDateTime) =>
            s"$prefix(${redactor(localDateTime.toString)})"
          case QuineValue.Id(id) => s"$prefix(${qidLoggable.unsafe(id, redactor)})"
        }
      }

      Loggable(logQuineValue)
    }
    implicit def logQuineType[QT <: com.thatdot.quine.model.QuineType]: AlwaysSafeLoggable[QT] =
      _.toString
    implicit val LogHalfEdge: Loggable[com.thatdot.quine.model.HalfEdge] =
      toStringLoggable[com.thatdot.quine.model.HalfEdge]
    implicit val LogPropertyValue: Loggable[com.thatdot.quine.model.PropertyValue] =
      toStringLoggable[com.thatdot.quine.model.PropertyValue]
    implicit val LogRange: Loggable[Range] = toStringLoggable[Range]
    implicit val LogFiniteDuration: Loggable[scala.concurrent.duration.FiniteDuration] =
      toStringLoggable[scala.concurrent.duration.FiniteDuration]
    implicit val LogNewMultipleValuesStateResult
      : Loggable[com.thatdot.quine.graph.messaging.StandingQueryMessage.NewMultipleValuesStateResult] =
      toStringLoggable[com.thatdot.quine.graph.messaging.StandingQueryMessage.NewMultipleValuesStateResult]
    implicit def logMultipleValuesStandingQuery[
      StandingQueryT <: com.thatdot.quine.graph.cypher.MultipleValuesStandingQuery,
    ]: AlwaysSafeLoggable[StandingQueryT] =
      _.toString
    implicit def logAdHocCypherQuery[
      QueryT <: com.thatdot.quine.graph.cypher.Query[_],
    ]: AlwaysSafeLoggable[QueryT] = _.toString
    implicit val LogGraphQueryPattern: AlwaysSafeLoggable[GraphQueryPattern] = _.toString
    implicit val LogDomainGraphBranch: AlwaysSafeLoggable[DomainGraphBranch] = _.toString
    implicit val LogDomainGraphNode: AlwaysSafeLoggable[DomainGraphNode] = _.toString
    implicit val LogStandingQueryInfo: AlwaysSafeLoggable[com.thatdot.quine.graph.StandingQueryInfo] =
      _.toString
    implicit val LogNotUsed: AlwaysSafeLoggable[NotUsed] = Loggable.alwaysSafe(_.toString)

    implicit def logJson[Json <: io.circe.Json]: Loggable[Json] = toStringLoggable[Json]
    implicit def LogSource[A, B]: AlwaysSafeLoggable[Source[A, B]] = _.toString
  }
}
