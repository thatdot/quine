package com.thatdot.quine.util

import java.net.InetSocketAddress
import java.nio.charset.Charset
import java.nio.file.Path
import java.time.temporal.TemporalUnit

import scala.jdk.CollectionConverters._

import org.apache.pekko.NotUsed
import org.apache.pekko.actor.ActorRef
import org.apache.pekko.stream.scaladsl.Source

import cats.data.NonEmptyList
import cats.implicits._
import com.typesafe.config.ConfigOrigin
import com.typesafe.scalalogging
import org.slf4j

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
  MultipleValuesStandingQueryPartId,
  StandingQueryId,
  StandingQueryResult,
  namespaceToString,
}
import com.thatdot.quine.model.{QuineId, QuineValue}

object Log {

  sealed trait SafeString {
    def interpolateSafe(): String
    def interpolateUnsafe(redactor: String => String): String
  }
  //A class that holds the result of interpolating a PII safe string using the macro below
  //We must store the interpolated string lazily because we do not know yet if we will
  //  be creating a `safe` or `unsafe` string as it depends on the settings in the logger
  class SafeInterpolator private (
    val safeString: () => String,
    val unsafeString: (String => String) => String,
    val exception: Option[Throwable] = None,
  ) extends SafeString {
    private def copy(
      safeString: () => String = safeString,
      unsafeString: (String => String) => String = unsafeString,
      exception: Option[Throwable] = exception,
    ) = new SafeInterpolator(safeString, unsafeString, exception)
    private def map(f: String => String) = copy(
      safeString = () => f(safeString()),
      unsafeString = (redactor: String => String) => f(unsafeString(redactor)),
    )
    def interpolateSafe(): String = safeString()
    def interpolateUnsafe(redactor: String => String): String = unsafeString(redactor)
    def withException(e: Throwable): SafeInterpolator = copy(exception = Some(e))
    def +(other: SafeInterpolator): SafeInterpolator = copy(
      safeString = () => safeString() + other.safeString(),
      unsafeString = (redactor: String => String) => unsafeString(redactor) + unsafeString(redactor),
      exception = exception.handleErrorWith(_ => (other.exception)),
    )
    def stripMargin: SafeInterpolator = map(_.stripMargin)
    def trim: SafeInterpolator = map(_.trim)
    def replaceNewline(r: Char): SafeInterpolator = map(_.replace('\n', r))
    def cleanLines: SafeInterpolator = stripMargin.replaceNewline(' ').trim
  }
  object SafeInterpolator {
    //Private so this object can only be created from within the "log" interpolator
    private[Log] def apply(
      ss: Seq[String],
      safeArgs: Seq[() => String],
      unsafeArgs: Seq[(String => String) => String],
    ) = new SafeInterpolator(
      () => StringContext.standardInterpolator(StringContext.processEscapes, safeArgs.map(_()), ss),
      (redactor: String => String) =>
        StringContext.standardInterpolator(StringContext.processEscapes, unsafeArgs.map(_(redactor)), ss),
    )
  }

  //This is like SafeInterpolator, but it represents strings that can only be safe
  //This is alos only constructed below, but in the "safe" interpolator function rather than the "log" interpolator
  class OnlySafeStringInterpolator private[Log] (
    val safeString: () => String,
  ) extends SafeString {
    def copy(
      safeString: () => String = safeString,
    ): OnlySafeStringInterpolator = new OnlySafeStringInterpolator(safeString)
    def interpolateSafe(): String = safeString()
    def interpolateUnsafe(redactor: String => String): String = interpolateSafe()
    def +(other: OnlySafeStringInterpolator): OnlySafeStringInterpolator =
      copy(safeString = () => safeString() + other.safeString())
    private def map(f: String => String): OnlySafeStringInterpolator = copy(safeString = () => f(safeString()))
    def stripMargin: OnlySafeStringInterpolator = map(_.stripMargin)
    def trim: OnlySafeStringInterpolator = map(_.trim)
    def replaceNewline(r: Char): OnlySafeStringInterpolator = map(_.replace('\n', r))
    def cleanLines: OnlySafeStringInterpolator = stripMargin.replaceNewline(' ').trim
  }
  object OnlySafeStringInterpolator {
    private[Log] def apply(ss: Seq[String], safeArgs: Seq[() => String]) = new OnlySafeStringInterpolator(() =>
      StringContext.standardInterpolator(StringContext.processEscapes, safeArgs.map(_()), ss),
    )
  }

  //A typeclass that represents a datatype that can be converted into a string for the purpose of logging
  trait Loggable[A] {
    //Will be called if we are treating this data as "safe"
    // i.e. we have enabled unsafe logging in the config or this has been marked as "Safe" (see the "Safe" class)
    def safe(a: A): String
    //Will be called if this data could be unsafe. Should replace all fields that could contain
    //  PII with `redacted(unsafeInfo)`
    def unsafe(a: A, redactor: String => String): String
  }

  //A typeclass that represents a type that can always be considered safe. These should never be redacted
  // and always be considered safe to pass to the "safe" interpolator below
  trait AlwaysSafeLoggable[A] extends Loggable[A] {
    def safe(a: A): String
    final def unsafe(a: A, redactor: String => String): String = safe(a)
  }

  //Declares the data in a loggable is safe.
  //When the "unsafe" method is called on a Safe[A] we just call the "safe" method of A, since we have
  // declared that the data in this instance of "A" is safe
  case class Safe[A](a: A)(implicit val loggable: Loggable[A])
  //The AlwaysSafeLoggable instance for variables marked "Safe"
  implicit def SafeLoggable[A]: AlwaysSafeLoggable[Safe[A]] = new AlwaysSafeLoggable[Safe[A]] {
    def safe(a: Safe[A]): String = a.loggable.safe(a.a)
  }

  //Helper class that is needed for how we construct StringInterpolators
  //When the log function declares that it takes a variable number of arguments, it declares their type must be LogObj
  // so that we can have a implicit Loggable instance for each argument.
  // (We cannot have a variable number of implicit arguments whose type depends on the first variable argument list)
  implicit class LogObj[A](val value: A)(implicit val loggable: Loggable[A]) {
    def safe: () => String = () => loggable.safe(value)
    def unsafe: (String => String) => String = f => loggable.unsafe(value, f)
  }
  //Works like the LogObj class above, except it is for the AlwaysSafeLoggable typeclass
  implicit class AlwaysSafeLogObj[A](val value: A)(implicit val loggable: AlwaysSafeLoggable[A]) {
    def safe: () => String = () => loggable.safe(value)
  }

  //An implicit class that extends AnyVal and has a StringContext is how we add new string interpolation methods
  implicit class SafeLoggableInterpolator(private val sc: StringContext) extends AnyVal {

    //Preforms a string interpolation, but the arguments must have an implicit SafeLoggable instance
    //The actual interpolation is lazy and is not performed until the string is actually logged
    //Note: Actually logging this object (calling "warn", "error", "info", etc) requires an implicit LogConfig in context
    def log(args: LogObj[_]*): SafeInterpolator = SafeInterpolator(
      sc.parts,
      args.map(_.safe),
      args.map(_.unsafe),
    )

    //Preforms a string interpolation like the "log" interpolator, but this can only accept variables that are marked as "safe"
    //This is functionally equivalent to "log" (safe does not mark its arguments as safe, it just requires that they must already marked as safe)
    //The method exists because the result of preforming the "safe" interpolation does not rely on a LogConfig
    def safe(args: AlwaysSafeLogObj[_]*): OnlySafeStringInterpolator =
      OnlySafeStringInterpolator(sc.parts, args.map(_.safe))
  }

  //Declares the keys in a map are safe but the values may not be
  case class SafeKeys[K, V](m: Map[K, V])(implicit val safeLoggable: Loggable[Map[Safe[K], V]])
  implicit def SafeKeysLoggable[K, V](implicit loggableKeys: Loggable[K]): Loggable[SafeKeys[K, V]] =
    new Loggable[SafeKeys[K, V]] {
      def safe(m: SafeKeys[K, V]): String = m.safeLoggable.safe(m.m map { case (k, v) =>
        (Safe(k), v)
      })
      def unsafe(m: SafeKeys[K, V], redactor: String => String): String = m.safeLoggable.unsafe(
        m.m map { case (k, v) =>
          (Safe(k), v)
        },
        redactor,
      )
    }

  private[Log] trait SafeToStringLoggable[A] extends Loggable[A] {
    def safe(a: A) = a.toString
  }
  //Helper function for generating a "Loggable" for primitive types
  // that encoding them is as simple as calling .toString
  def toStringLoggable[A]: SafeToStringLoggable[A] = (a: A, redactor: String => String) => redactor(a.toString)

  implicit val LogString: Loggable[String] = toStringLoggable[String]
  //All of the implicit instances of Loggable for primitives and Quine Values.
  // This is put inside of another object so you aren't given all of the implicits every time you import Loggable._
  object implicits {
    implicit val LogExpr: Loggable[com.thatdot.quine.graph.cypher.Expr] = {
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
                s"IdBytes(${LogQuineId.unsafe(QuineId(b), redactor)})"
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
                s"$prefix(${LogQuineId.unsafe(id, redactor)}, Labels(${redactor(labels.map(_.name).mkString(", "))}), {${properties
                  .map { case (k, v) => s"${redactor(k.name)} -> ${recurse(v)}" }
                  .mkString(", ")}})"
              case Expr.Relationship(start, name, properties, end) =>
                // IDs are delegated to LogQuineId, label is stringified redacted, properties are redacted
                s"$prefix(${LogQuineId.unsafe(start, redactor)}, ${redactor(name.name)}, ${properties
                  .map { case (k, v) => s"${redactor(k.name)} -> ${recurse(v)}" }
                  .mkString(", ")}, ${LogQuineId.unsafe(end, redactor)})"
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
    implicit val LogValue: Loggable[com.thatdot.quine.graph.cypher.Value] = Loggable(LogExpr.unsafe(_, _))
    implicit val LogInt: Loggable[Int] = toStringLoggable[Int]
    implicit val LogBoolean: Loggable[Boolean] = toStringLoggable[Boolean]
    implicit val LogLong: Loggable[Long] = toStringLoggable[Long]
    implicit val LogConfigOrigin: Loggable[ConfigOrigin] = toStringLoggable[ConfigOrigin]
    implicit val LogStandingQueryResult: Loggable[StandingQueryResult] = toStringLoggable[StandingQueryResult]
    implicit val LogPath: Loggable[Path] = toStringLoggable[Path]
    implicit val LogUrl: Loggable[java.net.URL] = toStringLoggable[java.net.URL]
    implicit val LogInetSocketAddress: Loggable[InetSocketAddress] = toStringLoggable[InetSocketAddress]
    implicit val LogEventTime: Loggable[EventTime] = toStringLoggable[EventTime]
    implicit val LogTemporalUnit: AlwaysSafeLoggable[TemporalUnit] = Loggable.alwaysSafe[TemporalUnit](_.toString)
    implicit val LogStandingQueryId: AlwaysSafeLoggable[StandingQueryId] =
      Loggable.alwaysSafe[StandingQueryId](_.toString)
    implicit val LogCharset: AlwaysSafeLoggable[Charset] = Loggable.alwaysSafe[Charset](_.toString)
    implicit val LogUnitState: Loggable[UnitState] = toStringLoggable[com.thatdot.quine.graph.cypher.UnitState]
    implicit val LogCrossState: Loggable[CrossState] = toStringLoggable[com.thatdot.quine.graph.cypher.CrossState]
    implicit val LogAllPropertiesState: Loggable[AllPropertiesState] = toStringLoggable[AllPropertiesState]
    implicit val LogLocalPropertyState: Loggable[LocalPropertyState] = toStringLoggable[LocalPropertyState]
    implicit val LogEdgeSubscriptionReciprocalState: Loggable[EdgeSubscriptionReciprocalState] =
      toStringLoggable[EdgeSubscriptionReciprocalState]
    implicit val LogLocalIdState: Loggable[LocalIdState] = toStringLoggable[LocalIdState]
    implicit val LogSqResultLike: Loggable[SqResultLike] = toStringLoggable[SqResultLike]
    implicit val LogMultipleValuesStandingQueryPartId: AlwaysSafeLoggable[MultipleValuesStandingQueryPartId] =
      Loggable.alwaysSafe[MultipleValuesStandingQueryPartId](_.toString)
    implicit val LogActorRef: AlwaysSafeLoggable[ActorRef] =
      // not just _.toString because ActorRefs can be null (notably, ActorRef.noSender)
      String.valueOf(_)
    implicit val LogSymbol: Loggable[Symbol] = toStringLoggable[Symbol]
    implicit val LogVersion: AlwaysSafeLoggable[com.thatdot.quine.persistor.Version] =
      Loggable.alwaysSafe[com.thatdot.quine.persistor.Version](_.toString)
    implicit val LogQuineId: AlwaysSafeLoggable[QuineId] = Loggable.alwaysSafe[QuineId](_.toString)
    implicit val LogEdgeEvent: Loggable[com.thatdot.quine.graph.EdgeEvent] =
      toStringLoggable[com.thatdot.quine.graph.EdgeEvent]
    implicit val LogFile: Loggable[java.io.File] = toStringLoggable[java.io.File]
    implicit val LogShardRef: Loggable[ShardRef] = toStringLoggable[ShardRef]
    implicit val LogSpaceTimeQuineId: AlwaysSafeLoggable[com.thatdot.quine.graph.messaging.SpaceTimeQuineId] =
      Loggable.alwaysSafe[com.thatdot.quine.graph.messaging.SpaceTimeQuineId](_.toString)
    implicit def LogWakefulState[W <: com.thatdot.quine.graph.WakefulState]: AlwaysSafeLoggable[W] =
      _.toString
    implicit val LogActorSelection: Loggable[org.apache.pekko.actor.ActorSelection] =
      toStringLoggable[org.apache.pekko.actor.ActorSelection]
    // Option[Symbol] is too generic a type for which to confidently have an implicit instance
    val LogNamespaceId: AlwaysSafeLoggable[Option[Symbol]] =
      Loggable.alwaysSafe[com.thatdot.quine.graph.NamespaceId](namespaceToString)
    implicit val LogMilliseconds: Loggable[com.thatdot.quine.model.Milliseconds] =
      toStringLoggable[com.thatdot.quine.model.Milliseconds]
    implicit val LogQuineValue: Loggable[com.thatdot.quine.model.QuineValue] = {

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
          case QuineValue.Id(id) => s"$prefix(${LogQuineId.unsafe(id, redactor)})"
        }
      }

      Loggable(logQuineValue)
    }
    implicit val LogQuineType: AlwaysSafeLoggable[com.thatdot.quine.model.QuineType] =
      Loggable.alwaysSafe[com.thatdot.quine.model.QuineType](_.toString)
    implicit val LogHalfEdge: Loggable[com.thatdot.quine.model.HalfEdge] =
      toStringLoggable[com.thatdot.quine.model.HalfEdge]
    implicit val LogPropertyValue: Loggable[com.thatdot.quine.model.PropertyValue] =
      toStringLoggable[com.thatdot.quine.model.PropertyValue]
    implicit val LogQuineIntegerType: Loggable[com.thatdot.quine.model.QuineType.Integer.type] =
      toStringLoggable[com.thatdot.quine.model.QuineType.Integer.type]
    implicit val LogRange: Loggable[Range] = toStringLoggable[Range]
    implicit val LogFiniteDuration: Loggable[scala.concurrent.duration.FiniteDuration] =
      toStringLoggable[scala.concurrent.duration.FiniteDuration]
    implicit val LogNewMultipleValuesStateResult
      : Loggable[com.thatdot.quine.graph.messaging.StandingQueryMessage.NewMultipleValuesStateResult] =
      toStringLoggable[com.thatdot.quine.graph.messaging.StandingQueryMessage.NewMultipleValuesStateResult]
    implicit val LogMultipleValuesStandingQuery: Loggable[com.thatdot.quine.graph.cypher.MultipleValuesStandingQuery] =
      toStringLoggable[com.thatdot.quine.graph.cypher.MultipleValuesStandingQuery]
    implicit val LogStandingQueryInfo: Loggable[com.thatdot.quine.graph.StandingQueryInfo] =
      toStringLoggable[com.thatdot.quine.graph.StandingQueryInfo]
    implicit val LogNotUsed: AlwaysSafeLoggable[NotUsed] = Loggable.alwaysSafe(_.toString)
    implicit val LogJson: Loggable[io.circe.Json] = toStringLoggable[io.circe.Json]
    implicit def LogSource[A, B](implicit
      loggableA: Loggable[A],
      loggableB: Loggable[B],
    ): Loggable[org.apache.pekko.stream.scaladsl.Source[A, B]] =
      new Loggable[org.apache.pekko.stream.scaladsl.Source[A, B]] {
        override def safe(a: Source[A, B]): String = a.toString

        override def unsafe(src: Source[A, B], redactor: String => String): String =
          src.map(a => loggableA.unsafe(a, redactor)).mapMaterializedValue(b => loggableB.unsafe(b, redactor)).toString

      }
    implicit def LoggableMap[K, V](implicit loggableKey: Loggable[K], loggableVal: Loggable[V]): Loggable[Map[K, V]] =
      new Loggable[Map[K, V]] {
        override def safe(a: Map[K, V]): String = a.map { case (k, v) =>
          (loggableKey.safe(k), loggableVal.safe(v))
        }.toString

        override def unsafe(a: Map[K, V], redactor: String => String): String = a.map { case (k, v) =>
          (loggableKey.unsafe(k, redactor), loggableVal.unsafe(v, redactor))
        }.toString
      }
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
    implicit def LogConcurrentMap[K, V](implicit
      loggableKey: Loggable[K],
      loggableVal: Loggable[V],
    ): Loggable[scala.collection.concurrent.Map[K, V]] = new Loggable[scala.collection.concurrent.Map[K, V]] {
      override def safe(a: scala.collection.concurrent.Map[K, V]): String = a.map { case (k, v) =>
        (loggableKey.safe(k), loggableVal.safe(v))
      }.toString
      override def unsafe(a: scala.collection.concurrent.Map[K, V], redactor: String => String): String = a.map {
        case (k, v) => (loggableKey.unsafe(k, redactor), loggableVal.unsafe(v, redactor))
      }.toString
    }
    implicit def loggableIterable[A](implicit loggableElems: Loggable[A]): Loggable[Iterable[A]] =
      new Loggable[Iterable[A]] {
        override def safe(l: Iterable[A]): String = "[" + l
          .map { case e =>
            loggableElems.safe(e)
          }
          .mkString(", ") + "]"
        override def unsafe(l: Iterable[A], redactor: String => String): String = "[" + l
          .map { case e =>
            loggableElems.unsafe(e, redactor)
          }
          .mkString(",") + "]"
      }
    implicit def loggableList[A](implicit loggableElems: Loggable[A]): Loggable[List[A]] = new Loggable[List[A]] {
      override def safe(l: List[A]): String = "[" + l
        .map { case e =>
          loggableElems.safe(e)
        }
        .mkString(", ") + "]"
      override def unsafe(l: List[A], redactor: String => String): String = "[" + l
        .map { case e =>
          loggableElems.unsafe(e, redactor)
        }
        .mkString(",") + "]"
    }
    implicit def loggableCollectionsSet[A](implicit loggableElems: Loggable[A]): Loggable[scala.collection.Set[A]] =
      new Loggable[scala.collection.Set[A]] {
        override def safe(l: scala.collection.Set[A]): String = "{" + l
          .map { case e =>
            loggableElems.safe(e)
          }
          .mkString(", ") + "}"
        override def unsafe(l: scala.collection.Set[A], redactor: String => String): String = "{" + l
          .map { case e =>
            loggableElems.unsafe(e, redactor)
          }
          .mkString(",") + "}"
      }
    implicit def loggableSet[A](implicit loggableElems: Loggable[A]): Loggable[Set[A]] = new Loggable[Set[A]] {
      override def safe(l: Set[A]): String = "{" + l
        .map { case e =>
          loggableElems.safe(e)
        }
        .mkString(", ") + "}"
      override def unsafe(l: Set[A], redactor: String => String): String = "{" + l
        .map { case e =>
          loggableElems.unsafe(e, redactor)
        }
        .mkString(",") + "}"
    }
    implicit def loggableNonEmptyList[A](implicit loggableElems: Loggable[A]): Loggable[NonEmptyList[A]] =
      new Loggable[NonEmptyList[A]] {
        override def safe(l: NonEmptyList[A]): String = loggableIterable(loggableElems).safe(l.toList)
        override def unsafe(l: NonEmptyList[A], redactor: String => String): String =
          loggableIterable(loggableElems).unsafe(l.toList, redactor)
      }
    implicit def loggableConcurrentLinkedDeque[A](implicit
      loggableElems: Loggable[A],
    ): Loggable[java.util.concurrent.ConcurrentLinkedDeque[A]] =
      new Loggable[java.util.concurrent.ConcurrentLinkedDeque[A]] {
        override def safe(l: java.util.concurrent.ConcurrentLinkedDeque[A]): String =
          loggableList(loggableElems).safe(l.iterator.asScala.toList)
        override def unsafe(l: java.util.concurrent.ConcurrentLinkedDeque[A], redactor: String => String): String =
          loggableList(loggableElems).unsafe(l.iterator.asScala.toList, redactor)
      }
  }

  object Loggable {
    //Helper function for creating a `Loggable`
    //Since the `safe` function is often just the same as the `unsafe` function but without
    //  obfuscation of potentially unsafe value, you can just supply the `unsafe` function
    //  and have this function derive the `safe` function
    def apply[A](f: (A, String => String) => String): Loggable[A] = new Loggable[A] {
      def safe(a: A) = f(a, identity)
      def unsafe(a: A, redactor: (String => String)) = f(a, redactor)
    }
    def alwaysSafe[A](f: A => String): AlwaysSafeLoggable[A] = new AlwaysSafeLoggable[A] {
      def safe(a: A) = f(a)
    }
  }

  //The method for actually Redacting potential PII
  //Currently the only option for this is RedactHide, which replaces the PII with "**REDACTED**"
  sealed trait RedactMethod {
    def redactor(s: String): String
  }
  case object RedactHide extends RedactMethod {
    override def redactor(s: String): String = "**REDACTED**"
  }

  case class LogConfig(
    showUnsafe: Boolean = false,
    showExceptions: Boolean = false,
    redactor: RedactMethod = RedactHide,
  )
  object LogConfig {
    //The most permissive log config. Useful for testing environments
    val testing: LogConfig = LogConfig(showUnsafe = true, showExceptions = true, redactor = RedactHide)
    val strictest: LogConfig = LogConfig(showUnsafe = false, showExceptions = false, redactor = RedactHide)
  }

  //Unifies scalalogging loggers with pekko LoggingAdapters
  class SafeLogger(
    private val logger: Either[
      scalalogging.Logger,
      org.apache.pekko.event.LoggingAdapter,
    ],
  ) {

    def whenDebugEnabled(body: => Unit): Unit = logger match {
      case Left(logger) => logger.whenDebugEnabled(body)
      case Right(logger) => if (logger.isDebugEnabled) body
    }
    def whenWarnEnabled(body: => Unit): Unit = logger match {
      case Left(logger) => logger.whenWarnEnabled(body)
      case Right(logger) => if (logger.isWarningEnabled) body
    }
    def whenInfoEnabled(body: => Unit): Unit = logger match {
      case Left(logger) => logger.whenInfoEnabled(body)
      case Right(logger) => if (logger.isInfoEnabled) body
    }
    def whenErrorEnabled(body: => Unit): Unit = logger match {
      case Left(logger) => logger.whenErrorEnabled(body)
      case Right(logger) => if (logger.isErrorEnabled) body
    }
    def whenTraceEnabled(body: => Unit): Unit = logger match {
      case Left(logger) => logger.whenTraceEnabled(body)
      case Right(_) => ()
    }

    private def warn(s: String): Unit = logger match {
      case Left(logger) => logger.warn(s)
      case Right(logger) => logger.warning(s)
    }
    private def info[A](s: String): Unit = logger match {
      case Left(logger) => logger.info(s)
      case Right(logger) => logger.info(s)
    }
    private def error[A](s: String): Unit = logger match {
      case Left(logger) => logger.error(s)
      case Right(logger) => logger.error(s)
    }
    private def debug[A](s: String): Unit = logger match {
      case Left(logger) => logger.debug(s)
      case Right(logger) => logger.debug(s)
    }
    private def trace[A](s: String): Unit = logger match {
      case Left(logger) => logger.trace(s)
      case Right(_) => () //Pekko logging does not have trace, so we must do nothing here
    }

    def warn[A](s: => OnlySafeStringInterpolator): Unit = whenWarnEnabled(warn(s.interpolateSafe()))
    def info[A](s: => OnlySafeStringInterpolator): Unit = whenInfoEnabled(info(s.interpolateSafe()))
    def error[A](s: => OnlySafeStringInterpolator): Unit = whenErrorEnabled(error(s.interpolateSafe()))
    def debug[A](s: => OnlySafeStringInterpolator): Unit = whenDebugEnabled(debug(s.interpolateSafe()))
    def trace[A](s: => OnlySafeStringInterpolator): Unit = whenTraceEnabled(trace(s.interpolateSafe()))

    private def warn(s: String, throws: Throwable): Unit = logger match {
      case Left(logger) => logger.warn(s, throws)
      case Right(logger) => logger.warning(s)
    }
    private def info[A](s: String, throws: Throwable): Unit = logger match {
      case Left(logger) => logger.info(s, throws)
      case Right(logger) => logger.info(s)
    }
    private def error[A](s: String, throws: Throwable): Unit = logger match {
      case Left(logger) => logger.error(s, throws)
      case Right(logger) => logger.error(s)
    }
    private def debug[A](s: String, throws: Throwable): Unit = logger match {
      case Left(logger) => logger.debug(s, throws)
      case Right(logger) => logger.debug(s)
    }
    private def trace[A](s: String, throws: Throwable): Unit = logger match {
      case Left(logger) => logger.trace(s, throws)
      case Right(_) => ()
    }

    def warn[A](si: => SafeInterpolator)(implicit config: LogConfig): Unit = whenWarnEnabled {
      val msg = if (config.showUnsafe) si.interpolateSafe() else si.interpolateUnsafe(config.redactor.redactor)
      (config.showExceptions, si.exception) match {
        case (true, Some(e)) => warn(msg, e)
        case _ => warn(msg)
      }
    }
    def info[A](si: => SafeInterpolator)(implicit config: LogConfig): Unit = whenInfoEnabled {
      val msg = if (config.showUnsafe) si.interpolateSafe() else si.interpolateUnsafe(config.redactor.redactor)
      (config.showExceptions, si.exception) match {
        case (true, Some(e)) => info(msg, e)
        case _ => info(msg)
      }
    }
    def error[A](si: => SafeInterpolator)(implicit config: LogConfig): Unit = whenErrorEnabled {
      val msg = if (config.showUnsafe) si.interpolateSafe() else si.interpolateUnsafe(config.redactor.redactor)
      (config.showExceptions, si.exception) match {
        case (true, Some(e)) => error(msg, e)
        case _ => error(msg)
      }
    }
    def debug[A](si: => SafeInterpolator)(implicit config: LogConfig): Unit = whenDebugEnabled {
      val msg = if (config.showUnsafe) si.interpolateSafe() else si.interpolateUnsafe(config.redactor.redactor)
      (config.showExceptions, si.exception) match {
        case (true, Some(e)) => debug(msg, e)
        case _ => debug(msg)
      }
    }
    def trace[A](si: => SafeInterpolator)(implicit config: LogConfig): Unit = whenTraceEnabled {
      val msg = if (config.showUnsafe) si.interpolateSafe() else si.interpolateUnsafe(config.redactor.redactor)
      (config.showExceptions, si.exception) match {
        case (true, Some(e)) => trace(msg, e)
        case _ => trace(msg)
      }
    }
  }

  //Constructor for creating a SafeLogger outside of this file
  object SafeLogger {
    def apply(name: String) = new SafeLogger(Left(scalalogging.Logger(name)))
  }

  //Works Like scalalogging LazyLogging, but LazyLogging creates a scalalogging.Logger while LazySafeLogging wraps it in a SafeLogger
  trait LazySafeLogging {
    @transient
    protected lazy val logger: SafeLogger = new SafeLogger(
      Left(scalalogging.Logger(slf4j.LoggerFactory.getLogger(getClass.getName))),
    )
  }
  //Works Like scalalogging StrictLogging, but StrictLogging creates a scalalogging.Logger while StrictSafeLogging wraps it in a SafeLogger
  trait StrictSafeLogging {
    protected val logger: SafeLogger = new SafeLogger(
      Left(scalalogging.Logger(slf4j.LoggerFactory.getLogger(getClass.getName))),
    )
  }
  //Works Like Pekko ActorLogging, but Pekko ActorLogging creates a pekko logger while ActorSafeLogging wraps it in a SafeLogger
  trait ActorSafeLogging { this: org.apache.pekko.actor.Actor =>
    protected lazy val log: SafeLogger = new SafeLogger(Right(org.apache.pekko.event.Logging(context.system, this)))
  }
}
