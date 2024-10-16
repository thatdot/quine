package com.thatdot.quine.graph.cypher

import java.lang.{Double => JavaDouble, Integer => JavaInteger, Long => JavaLong}
import java.nio.charset.StandardCharsets
import java.time.temporal._
import java.time.{Duration => JavaDuration, LocalDateTime => JavaLocalDateTime, ZonedDateTime => JavaZonedDateTime}
import java.util.Base64

import scala.annotation.nowarn
import scala.collection.immutable.{Map => ScalaMap, SortedMap}
import scala.util.Try
import scala.util.hashing.MurmurHash3

import cats.implicits._
import com.google.common.hash.{HashCode, Hasher, Hashing}
import io.circe.{Json, JsonNumber, JsonObject}
import org.apache.commons.text.StringEscapeUtils

import com.thatdot.quine.model.{QuineId, QuineIdProvider, QuineValue}
import com.thatdot.quine.util.Log._
import com.thatdot.quine.util.MonadHelpers._
import com.thatdot.quine.util.{ByteConversions, TypeclassInstances}

/** Maps directly onto Cypher's expressions
  *
  * @see [[https://neo4j.com/docs/cypher-manual/current/syntax/expressions/]]
  */
sealed abstract class Expr {

  /** Is this a pure expression? A pure expression satisfies all of:
    *
    * - Returns a value that is fully computed from the expression arguments
    *   (therefore evaluating with the same variable context and parameters always
    *        produces the same result)
    *
    * - Does not cause side effects
    */
  def isPure: Boolean

  /** Barring unbound variable or parameter exceptions, is it impossible for
    * the expression to return an error when evaluated?
    */
  def cannotFail: Boolean

  /** Evaluate an expression under a current context and with parameters
    *
    * @param context    variables in scope and their values
    * @param parameters constant parameters (constant across a query)
    * @param idProvider ID provider
    */
  def eval(
    context: QueryContext,
  )(implicit
    idProvider: QuineIdProvider,
    parameters: Parameters,
    logConfig: LogConfig,
  ): Either[CypherException, Value]

  def evalUnsafe(
    context: QueryContext,
  )(implicit
    idProvider: QuineIdProvider,
    parameters: Parameters,
    logConfig: LogConfig,
  ): Value = eval(context).getOrThrow

  /** substitute all parameters in this expression and all descendants
    * @param parameters a [[Parameters]] providing parameters used by [[Expr.Parameter]]s within this expression.
    * @return a copy of this expression with all provided parameters substituted
    * INV: If all parameters used by [[Expr.Parameter]] AST nodes are provided, the returned
    * expression will have no [[Expr.Parameter]] AST nodes remaining in the tree
    */
  def substitute(parameters: ScalaMap[Expr.Parameter, Value]): Expr
}

/** TODO: missing values supported by Neo4j (but not required by openCypher)
  *
  *    - Point
  *    - Date, Time, LocalTime
  */
object Expr {

  //Helper function for evaluating multiple expressions at once
  implicit class ExprPair(val p: (Expr, Expr)) {
    def eval(
      context: QueryContext,
    )(implicit
      idProvider: QuineIdProvider,
      parameters: Parameters,
      logConfig: LogConfig,
    ): Either[CypherException, (Value, Value)] = for {
      l <- p._1.eval(context)
      r <- p._2.eval(context)
    } yield (l, r)
  }

  /** Helpful marker trait for values that have a property type. These are:
    * integers, floating, string, and booleans.
    *
    * @note this does not include [[Null]] or graph objects like [[Node]]
    *
    * TODO: spatial types
    */
  sealed trait PropertyValue extends Value

  /** Convert a Quine value into a cypher one */
  def fromQuineValue(value: QuineValue): Value = value match {
    case QuineValue.Str(str) => Str(str)
    case QuineValue.Integer(lng) => Integer(lng)
    case QuineValue.Floating(flt) => Floating(flt)
    case QuineValue.True => True
    case QuineValue.False => False
    case QuineValue.Null => Null
    case QuineValue.Bytes(arr) => Bytes(arr)
    case QuineValue.List(vec) => List(vec.map(fromQuineValue))
    case QuineValue.Map(map) => Map(map.fmap(fromQuineValue))
    case QuineValue.DateTime(datetime) => DateTime(datetime.toZonedDateTime)
    case QuineValue.Duration(duration) => Duration(duration)
    case QuineValue.Date(date) => Date(date)
    case QuineValue.Time(t) => Time(t)
    case QuineValue.LocalTime(t) => LocalTime(t)
    case QuineValue.LocalDateTime(ldt) => LocalDateTime(ldt)
    case QuineValue.Id(id) => Bytes(id)
  }

  def toQuineValue(value: Value): Either[CypherException, QuineValue] = value match {
    case Str(str) => QuineValue.Str(str).asRight
    case Integer(lng) => QuineValue.Integer(lng).asRight
    case Floating(flt) => QuineValue.Floating(flt).asRight
    case True => QuineValue.True.asRight
    case False => QuineValue.False.asRight
    case Null => QuineValue.Null.asRight
    case Bytes(arr, false) => QuineValue.Bytes(arr).asRight
    case Bytes(arr, true) => QuineValue.Id(QuineId(arr)).asRight
    case List(vec) => vec.traverse(toQuineValue).map(QuineValue.List.apply)
    case Map(map) => map.traverse(toQuineValue).map(QuineValue.Map.apply)
    case DateTime(zonedDateTime) => QuineValue.DateTime(zonedDateTime.toOffsetDateTime).asRight
    case Duration(duration) => QuineValue.Duration(duration).asRight
    case Date(d) => QuineValue.Date(d).asRight
    case Time(t) => QuineValue.Time(t).asRight
    case LocalTime(t) => QuineValue.LocalTime(t).asRight
    case LocalDateTime(ldt) => QuineValue.LocalDateTime(ldt).asRight

    case other => CypherException.TypeMismatch(Seq.empty, other, "converting to a quine value").asLeft
  }

  /** A cypher string value
    *
    * @param str underlying Java string
    */
  final case class Str(string: String) extends PropertyValue {
    override def typ = Type.Str

    def addToHasher(hasher: Hasher): Hasher =
      hasher
        .putInt("Str".hashCode)
        .putString(string, StandardCharsets.UTF_8)
  }

  /** A cypher number value */
  sealed trait Number extends Value {
    def +(other: Number): Either[CypherException.Arithmetic, Number]
    def -(other: Number): Either[CypherException.Arithmetic, Number]
    def *(other: Number): Either[CypherException.Arithmetic, Number]
    def /(other: Number): Either[CypherException.Arithmetic, Number]
    def %(other: Number): Either[CypherException.Arithmetic, Number]
    def unary_- : Either[CypherException.Arithmetic, Number]
    def ^(other: Number): Either[CypherException.Arithmetic, Number]
    def string: String
  }
  object Number {
    def unapply(v: Value): Option[Double] = v match {
      case Expr.Floating(dbl) => Some(dbl)
      case Expr.Integer(lng) => Some(lng.toDouble)
      case _ => None
    }
  }

  /** A cypher integer number value
    *
    * @param long underlying Java 64-bit integral value
    */
  final case class Integer(long: Long) extends Number with PropertyValue {

    /** Java API: extract underlying long value */
    def getLong: Long = long

    def +(other: Number) = other match {
      case Floating(f2) => Floating(long + f2).asRight
      case Integer(i2) =>
        try Integer(Math.addExact(long, i2)).asRight
        catch {
          case a: ArithmeticException =>
            CypherException
              .Arithmetic(
                wrapping = a.getMessage,
                operands = Seq(Integer(long), Integer(i2)),
              )
              .asLeft
        }
      case Null => Null.asRight
    }

    def -(other: Number) = other match {
      case Floating(f2) => Floating(long - f2).asRight
      case Integer(i2) =>
        try Integer(Math.subtractExact(long, i2)).asRight
        catch {
          case a: ArithmeticException =>
            CypherException
              .Arithmetic(
                wrapping = a.getMessage,
                operands = Seq(Integer(long), Integer(i2)),
              )
              .asLeft
        }
      case Null => Null.asRight
    }

    def *(other: Number) = other match {
      case Floating(f2) => Floating(long * f2).asRight
      case Integer(i2) =>
        try Integer(Math.multiplyExact(long, i2)).asRight
        catch {
          case a: ArithmeticException =>
            CypherException
              .Arithmetic(
                wrapping = a.getMessage,
                operands = Seq(Integer(long), Integer(i2)),
              )
              .asLeft
        }
      case Null => Null.asRight
    }

    def /(other: Number) = other match {
      case Floating(f2) => Floating(long / f2).asRight
      case Integer(i2) =>
        try Integer(long / i2).asRight
        catch {
          case a: ArithmeticException =>
            CypherException
              .Arithmetic(
                wrapping = a.getMessage,
                operands = Seq(Integer(long), Integer(i2)),
              )
              .asLeft
        }
      case Null => Null.asRight
    }

    def %(other: Number) = other match {
      case Floating(f2) => Floating(long % f2).asRight
      case Integer(i2) =>
        try Integer(long % i2).asRight
        catch {
          case a: ArithmeticException =>
            CypherException
              .Arithmetic(
                wrapping = a.getMessage,
                operands = Seq(Integer(long), Integer(i2)),
              )
              .asLeft
        }
      case Null => Null.asRight
    }

    def ^(other: Number) = other match {
      case Floating(f2) => Floating(Math.pow(long.toDouble, f2)).asRight
      case Integer(i2) =>
        try Floating(Math.pow(long.toDouble, i2.toDouble)).asRight
        catch {
          case a: ArithmeticException =>
            CypherException
              .Arithmetic(
                wrapping = a.getMessage,
                operands = Seq(Integer(long), Integer(i2)),
              )
              .asLeft
        }
      case Null => Null.asRight
    }

    def unary_- = try Integer(Math.negateExact(long)).asRight
    catch {
      case a: ArithmeticException =>
        CypherException
          .Arithmetic(
            wrapping = a.getMessage,
            operands = Seq(Integer(long)),
          )
          .asLeft
    }

    def string = long.toString

    def typ = Type.Integer

    def addToHasher(hasher: Hasher): Hasher =
      hasher
        .putInt("Long".hashCode)
        .putLong(long)
  }
  object Integer {

    /* Cache of small integers from -128 to 127 inclusive, to share references
     * whenever possible (less allocations + faster comparisons)
     */
    private val integerCacheMin = -128L
    private val integerCacheMax = 127L
    private val integerCache: Array[Integer] =
      Array.tabulate((integerCacheMax - integerCacheMin + 1).toInt) { (i: Int) =>
        new Integer(i.toLong + integerCacheMin)
      }

    def apply(long: Long): Integer =
      if (long >= integerCacheMin && long <= integerCacheMax) {
        integerCache((long - integerCacheMin).toInt)
      } else {
        new Integer(long)
      }
  }

  /** A cypher IEEE-754 floating point number value
    *
    * @param double underlying Java 64-bit floating point value
    */
  final case class Floating(double: Double) extends Number with PropertyValue {

    /** Java API: extract underlying double value */
    def getDouble: Double = double

    def +(other: Number) = other match {
      case Floating(f2) => Floating(double + f2).asRight
      case Integer(i2) => Floating(double + i2).asRight
      case Null => Null.asRight
    }

    def -(other: Number) = other match {
      case Floating(f2) => Floating(double - f2).asRight
      case Integer(i2) => Floating(double - i2).asRight
      case Null => Null.asRight
    }

    def *(other: Number) = other match {
      case Floating(f2) => Floating(double * f2).asRight
      case Integer(i2) => Floating(double * i2).asRight
      case Null => Null.asRight
    }

    def /(other: Number) = other match {
      case Floating(f2) => Floating(double / f2).asRight
      case Integer(i2) => Floating(double / i2).asRight
      case Null => Null.asRight
    }

    def %(other: Number) = other match {
      case Floating(f2) => Floating(double % f2).asRight
      case Integer(i2) => Floating(double % i2).asRight
      case Null => Null.asRight
    }

    def ^(other: Number) = other match {
      case Floating(f2) => Floating(Math.pow(double, f2)).asRight
      case Integer(i2) => Floating(Math.pow(double, i2.toDouble)).asRight
      case Null => Null.asRight
    }

    def unary_- = Floating(-double).asRight

    def string = double.toString

    def typ = Type.Floating

    def addToHasher(hasher: Hasher): Hasher =
      hasher
        .putInt("Double".hashCode)
        .putDouble(double)
  }

  /** A cypher boolean value */
  sealed trait Bool extends Value {

    /** Negation in Kleene's strong three-valued logic
      *
      * @return the ternary negation
      */
    def negate: Bool

    /** Conjunction in Kleene's strong three-valued logic
      *
      * @param other the conjunct
      * @return the ternary conjunction
      */
    def and(other: Bool): Bool

    /** Disjunction in Kleene's string three-valued logic
      *
      * @param other the disjunct
      * @return the ternary disjunction
      */
    def or(other: Bool): Bool
  }
  object Bool {
    def apply(value: Boolean): Bool = if (value) True else False
    def unapply(value: Value): Option[Boolean] = value match {
      case True => Some(true)
      case False => Some(false)
      case _ => None
    }
  }

  /** A cypher `true` boolean value */
  case object True extends Bool with PropertyValue {

    override val typ = Type.Bool

    override val hash: HashCode = super.hash

    def addToHasher(hasher: Hasher): Hasher =
      hasher.putInt("True".hashCode)

    override def negate = False
    override def and(other: Bool) = other
    override def or(other: Bool) = True
  }

  /** A cypher `false` boolean value */
  case object False extends Bool with PropertyValue {

    override val typ = Type.Bool

    override val hash: HashCode = super.hash

    def addToHasher(hasher: Hasher): Hasher =
      hasher.putInt("False".hashCode)

    override def negate = True
    override def and(other: Bool) = False
    override def or(other: Bool) = other
  }

  /** Java AIP: get the null singleton */
  final def nullValue() = Null

  /** A cypher value which indicates the absence of a value
    *
    * @see [[https://neo4j.com/docs/cypher-manual/current/syntax/working-with-null]]
    */
  case object Null extends Value with Bool with Number {

    override val typ = Type.Null

    override val hash: HashCode = super.hash

    def addToHasher(hasher: Hasher): Hasher =
      hasher.putInt("Null".hashCode)

    override def +(_other: Number) = Null.asRight

    override def -(_other: Number) = Null.asRight

    override def *(_other: Number) = Null.asRight

    override def /(_other: Number) = Null.asRight

    override def %(_other: Number) = Null.asRight
    override val unary_- = Null.asRight
    override def ^(_other: Number) = Null.asRight
    override val string: String = "null"

    override def negate = Null
    override def and(other: Bool): Bool = if (other == False) False else Null
    override def or(other: Bool): Bool = if (other == True) True else Null
  }

  /** A cypher value representing an array of bytes
    *
    * @note there is no way to directly write a literal for this in Cypher
    * @param b array of bytes (do not mutate this!)
    * @param representsId do these bytes represent an ID? (just a hint, not part of `hashCode` or `equals`)
    */
  final case class Bytes(b: Array[Byte], representsId: Boolean = false) extends PropertyValue {
    override def hashCode: Int =
      MurmurHash3.bytesHash(b, 0x54321) // 12345 would make QuineValue.Bytes hash the same as
    override def equals(other: Any): Boolean =
      other match {
        case Bytes(bytesOther, _) => b.toSeq == bytesOther.toSeq
        case _ => false
      }

    override def toString(): String =
      if (representsId) QuineId(b).toString
      else s"Bytes(${ByteConversions.formatHexBinary(b)})"

    def typ = Type.Bytes

    def addToHasher(hasher: Hasher): Hasher =
      hasher
        .putInt("Bytes".hashCode)
        .putBytes(b)
  }
  object Bytes {
    def apply(qid: QuineId): Bytes = Bytes(qid.array, representsId = true)
  }

  /** A cypher value representing a node
    *
    * @param id primary ID of the node
    * @param labels labels of the node
    * @param properties properties on the node
    */
  final case class Node(
    id: QuineId,
    labels: Set[Symbol],
    properties: ScalaMap[Symbol, Value],
  ) extends Value {

    def typ = Type.Node

    // TODO: should we hash the labels/properties?
    def addToHasher(hasher: Hasher): Hasher =
      hasher
        .putInt("Node".hashCode)
        .putBytes(id.array)
  }

  /** A cypher value representing an edge
    *
    * TODO: this needs to store properties
    *
    * @param start node at which the edge starts
    * @param name label on the edge
    * @param end node at which the edge ends
    */
  final case class Relationship(
    start: QuineId,
    name: Symbol,
    properties: ScalaMap[Symbol, Value],
    end: QuineId,
  ) extends Value {

    def typ = Type.Relationship

    // TODO: should we hash the properties? re-visit this with edge properties
    def addToHasher(hasher: Hasher): Hasher =
      hasher
        .putInt("Relationship".hashCode)
        .putBytes(start.array)
        .putString(name.name, StandardCharsets.UTF_8)
        .putBytes(end.array)

    def reflect: Relationship = Relationship(end, name, properties, start)
  }

  /** A list of cypher values
    *
    * Values can be heterogeneous.
    *
    * A list of integers can be coerced to list of floats. TODO: figure out
    * where this coercion can possibly matter/occur besides just making a list
    * homogeneous when putting it on a node.
    *
    * @param list underlying Scala vector of values
    */
  final case class List(list: Vector[Value]) extends PropertyValue {

    def typ = Type.ListOfAnything

    def addToHasher(hasher: Hasher): Hasher = {
      hasher
        .putInt("List".hashCode)
        .putInt(list.length)
      for (value <- list)
        value.addToHasher(hasher)
      hasher
    }
  }
  object List {
    def apply(vs: Value*): List = List(Vector(vs: _*))
    val empty: List = List(Vector.empty)
  }

  /** A map of cypher values with string keys
    *
    * Values can be heterogeneous.
    *
    * @param map underlying Scala map of values
    */
  final case class Map private (map: SortedMap[String, Value]) extends PropertyValue {
    def typ = Type.Map

    def addToHasher(hasher: Hasher): Hasher = {
      hasher
        .putInt("Map".hashCode)
        .putInt(map.size)
      for ((key, value) <- map) {
        hasher.putString(key, StandardCharsets.UTF_8)
        value.addToHasher(hasher)
      }
      hasher
    }
  }
  object Map {
    def apply(entries: IterableOnce[(String, Value)]): Map = new Map(SortedMap.from(entries))
    def apply(entries: (String, Value)*): Map = new Map(SortedMap.from(entries))
    val empty: Map = new Map(SortedMap.empty)
  }

  /** A cypher path - a linear sequence of alternating nodes and edges
    *
    * This cannot be constructed directly via literals: path values come
    * from path expressions (and 'only' from there).
    *
    * @param head first node in the path
    * @param tails sequence of edges and nodes following the head
    */
  final case class Path(head: Node, tails: Vector[(Relationship, Node)]) extends Value {

    def typ = Type.Path

    override def isPure: Boolean = head.isPure && tails.forall { (rn: (Relationship, Node)) =>
      val (r, n) = rn
      r.isPure && n.isPure
    }

    def addToHasher(hasher: Hasher): Hasher = {
      hasher
        .putInt("Path".hashCode)
        .putInt(tails.length)
      head.addToHasher(hasher)
      for ((rel, node) <- tails) {
        rel.addToHasher(hasher)
        node.addToHasher(hasher)
      }
      hasher
    }

    def toList: List = List(
      head +: tails.flatMap { case (r, n) => Vector[Value](r, n) },
    )
  }

  /** A cypher local date time
    *
    * @note this time is relative - it is missing a timezone to be absolute
    * @param localDateTime underlying Java local date time
    */
  final case class LocalDateTime(localDateTime: JavaLocalDateTime) extends PropertyValue {

    def typ = Type.LocalDateTime

    def addToHasher(hasher: Hasher): Hasher =
      hasher
        .putInt("LocalDateTime".hashCode)
        .putLong(localDateTime.toLocalDate.toEpochDay)
        .putLong(localDateTime.toLocalTime.toNanoOfDay)
  }

  /** A cypher date
    *
    * @note this time represents a date without time or timezone information.
    * @param date underlying Java LocalDate
    */
  final case class Date(date: java.time.LocalDate) extends PropertyValue {

    def typ = Type.Date

    def addToHasher(hasher: Hasher): Hasher =
      hasher
        .putInt("Date".hashCode)
        .putLong(date.toEpochDay)
  }

  /** A cypher time
    *
    * @note this time represents a time and UTC offset without date information.
    * @param time underlying Java time
    */
  final case class Time(time: java.time.OffsetTime) extends PropertyValue {

    def typ = Type.Time

    def addToHasher(hasher: Hasher): Hasher =
      hasher
        .putInt("Time".hashCode)
        .putLong(time.toLocalTime.toNanoOfDay)
        .putInt(time.getOffset.getTotalSeconds)
  }

  /** A cypher local time
    *
    * @note this time represents a local time without date information.
    * @param localTime underlying Java local time
    */
  final case class LocalTime(localTime: java.time.LocalTime) extends PropertyValue {

    def typ = Type.LocalTime

    def addToHasher(hasher: Hasher): Hasher =
      hasher
        .putInt("LocalTime".hashCode)
        .putLong(localTime.toNanoOfDay)
  }

  /** A cypher date time
    *
    * @note this time is absolute (the timezone was an input, implicit or explicit)
    * @param zonedDateTime underlying Java local date time
    */
  final case class DateTime(zonedDateTime: JavaZonedDateTime) extends PropertyValue {

    def typ = Type.DateTime

    def addToHasher(hasher: Hasher): Hasher = {
      val instant = zonedDateTime.toInstant
      hasher
        .putInt("DateTime".hashCode)
        .putInt(instant.getNano)
        .putLong(instant.getEpochSecond)
        .putInt(zonedDateTime.getZone.hashCode)
    }

    def timezoneFields(fieldName: String): Option[Expr.Str] = fieldName match {
      case "timezone" => Some(Expr.Str(zonedDateTime.getZone.toString))
      case "offset" => Some(Expr.Str(zonedDateTime.getOffset.toString))
      case _ => None
    }
  }

  private[this] val InstantMillis: TemporalField = new TemporalField {
    override def isTimeBased = true
    override def isDateBased = false
    override def getBaseUnit: TemporalUnit = ChronoUnit.MILLIS
    override def getRangeUnit: TemporalUnit = ChronoUnit.FOREVER
    override def range: ValueRange = ValueRange.of(Long.MinValue, Long.MaxValue)
    override def getFrom(temporal: TemporalAccessor): Long = {
      val seconds = temporal.getLong(ChronoField.INSTANT_SECONDS)
      val millis = temporal.getLong(ChronoField.MILLI_OF_SECOND)
      seconds * 1000L + millis
    }
    override def adjustInto[R <: Temporal](temporal: R, newValue: Long) =
      ChronoField.MILLI_OF_SECOND.adjustInto(
        ChronoField.INSTANT_SECONDS.adjustInto(temporal, newValue / 1000),
        newValue % 1000,
      )
    override def isSupportedBy(temporal: TemporalAccessor) =
      temporal.isSupported(ChronoField.INSTANT_SECONDS) &&
      temporal.isSupported(ChronoField.MILLI_OF_SECOND)
    override def rangeRefinedBy(temporal: TemporalAccessor) =
      if (isSupportedBy(temporal)) range
      else throw new UnsupportedTemporalTypeException("Unsupported field: " + toString)
    override def toString = "InstantMillis"
  }

  /** Time units and the names they use */
  val temporalFields: ScalaMap[String, TemporalField] = ScalaMap(
    "year" -> ChronoField.YEAR,
    "quarter" -> IsoFields.QUARTER_OF_YEAR,
    "month" -> ChronoField.MONTH_OF_YEAR,
    "week" -> IsoFields.WEEK_OF_WEEK_BASED_YEAR,
    "dayOfQuarter" -> IsoFields.DAY_OF_QUARTER,
    "day" -> ChronoField.DAY_OF_MONTH,
    "ordinalDay" -> ChronoField.DAY_OF_YEAR,
    "dayOfWeek" -> ChronoField.DAY_OF_WEEK,
    "hour" -> ChronoField.HOUR_OF_DAY,
    "minute" -> ChronoField.MINUTE_OF_HOUR,
    "second" -> ChronoField.SECOND_OF_MINUTE,
    "millisecond" -> ChronoField.MILLI_OF_SECOND,
    "microsecond" -> ChronoField.MICRO_OF_SECOND,
    "nanosecond" -> ChronoField.NANO_OF_SECOND,
    // "offsetMinutes" -> ???, TODO
    "offsetSeconds" -> ChronoField.OFFSET_SECONDS,
    "epochMillis" -> InstantMillis,
    "epochSeconds" -> ChronoField.INSTANT_SECONDS,
  )

  // The set of temporal units we allow in a duration constructor (eg `WITH duration({years: 2}) AS d`)
  // or in a duration dot-dereference (eg `d.years`)
  val temporalUnits: ScalaMap[String, TemporalUnit] = ScalaMap(
    "years" -> ChronoUnit.YEARS,
    "quarters" -> IsoFields.QUARTER_YEARS,
    "months" -> ChronoUnit.MONTHS,
    "weeks" -> ChronoUnit.WEEKS,
    "days" -> ChronoUnit.DAYS,
    "hours" -> ChronoUnit.HOURS,
    "minutes" -> ChronoUnit.MINUTES,
    "seconds" -> ChronoUnit.SECONDS,
    "milliseconds" -> ChronoUnit.MILLIS,
    "microseconds" -> ChronoUnit.MICROS,
    "nanoseconds" -> ChronoUnit.NANOS,
  )

  /** A cypher duration
    *
    * @note this is not like Neo4j's duration!
    *
    * @param duration seconds/nanoseconds between two times
    */
  final case class Duration(duration: JavaDuration) extends PropertyValue {

    def typ = Type.Duration

    def addToHasher(hasher: Hasher): Hasher =
      hasher
        .putInt("Duration".hashCode)
        .putInt(duration.getNano)
        .putLong(duration.getSeconds)

    // Returns the number of [unit] in this duration, rounded down.
    def as(unit: TemporalUnit): Either[ArithmeticException, Expr.Integer] = {
      // It's tempting to just take the duration as a nanoseconds measure then convert to the desired unit, but
      // that would overflow on durations longer than 293 years (ie, MAX_LONG nanoseconds), regardless
      // of the target unit. Instead, we consider the seconds and nanoseconds parts separately, then add them.
      // Additionally, we choose not to worry about estimated vs precise durations. For example, we'll say a day
      // is 86400 seconds (`ChronoUnit.DAYS.getDuration`), even though some days have a leap second.
      val unitDuration = unit.getDuration
      Try(duration.dividedBy(unitDuration)).fold(
        {
          case e: ArithmeticException => Left(e)
          case unexpectedError => throw unexpectedError
        },
        result => Right(Expr.Integer(result)),
      )
    }
  }

  /** A cypher variable
    *
    * TODO: replace this with an [[scala.Int]] index into a [[scala.Vector]] context (as
    *       opposed to a [[scala.Symbol]] index into a [[scala.collection.immutable.Map]])
    * TODO: along with the above TODO, remove the or-else-Null case
    */
  final case class Variable(id: Symbol) extends Expr {

    def isPure: Boolean = true

    def cannotFail: Boolean = true

    def substitute(parameters: ScalaMap[Parameter, Value]): Variable = this

    override def eval(
      qc: QueryContext,
    )(implicit idp: QuineIdProvider, p: Parameters, logConfig: LogConfig): Either[CypherException, Value] =
      qc.getOrElse(id, Null).asRight
  }

  /** A cypher property access
    *
    * {{{
    * RETURN foo.prop
    * }}}
    *
    * TODO: properties on relationships, point
    *
    * @param expr expression whose property is being access
    * @param key name of the property
    */
  final case class Property(expr: Expr, key: Symbol) extends Expr with LazySafeLogging {

    def isPure: Boolean = expr.isPure

    // Argument is not map-like
    def cannotFail: Boolean = false

    def substitute(parameters: ScalaMap[Parameter, Value]): Property = copy(expr = expr.substitute(parameters))

    override def eval(
      qc: QueryContext,
    )(implicit idp: QuineIdProvider, p: Parameters, logConfig: LogConfig): Either[CypherException, Value] =
      expr.eval(qc).flatMap {
        case Node(_, _, props) => props.getOrElse(key, Null).asRight
        case Relationship(_, _, props, _) => props.getOrElse(key, Null).asRight
        case Map(props) => props.getOrElse(key.name, Null).asRight
        case Null => Right(Null)
        case LocalDateTime(t) =>
          temporalFields
            .get(key.name)
            .fold[Value](Null)(u => Expr.Integer(t.getLong(u)))
            .asRight
        case dt @ DateTime(t) =>
          temporalFields
            .get(key.name)
            .map(u => Expr.Integer(t.getLong(u)))
            .orElse(dt.timezoneFields(key.name))
            .getOrElse(Null)
            .asRight
        case d @ Duration(_) =>
          temporalUnits
            .get(key.name)
            .toRight[Value](Null)
            .flatMap(units =>
              d.as(units).leftMap { e =>
                // If this dereference caused an overflow, we log a warning and return Null.
                // This is a deliberate deviation from Cypher idioms, which would have the exception wrapped
                // as a CypherException. We choose an error handling path here that avoids
                // terminating the query, because this is the kind of functionality that is likely present
                // in a stream (ingest or standing query output), and which is likely to work for some records
                // of that stream but fail on others. It is (arguably) a better user experience to have the
                // partially-processed stream and warnings indicating why the stream was only partially processed
                // than to require manual intervention. Ideally, however, this would be configured by the
                // stream's error handling mode.
                logger.warn(
                  log"""Duration property access failed on duration value: ${d.toString} due to arithmetic
                       |exception. Current row: ${qc.pretty} Returning Null.""".cleanLines withException e,
                )
                Null
              },
            )
            .merge
            .asRight

        case other =>
          CypherException
            .TypeMismatch(
              expected = Seq(
                Type.Map,
                Type.Node,
                Type.Relationship,
                Type.LocalDateTime,
                Type.DateTime,
                Type.Duration,
              ),
              actualValue = other,
              context = "property access",
            )
            .asLeft
      }
  }

  /** A dynamic property access
    *
    * {{{
    * WITH [1,2,3,4] AS list
    * WITH { a: 1, b: 2.0 } AS map
    * RETURN list[2], map["a"]
    * }}}
    *
    * TODO: properties on relationships, point
    *
    * @param expr expression whose property is being access
    * @param keyExpr expression for the name of the property
    */
  final case class DynamicProperty(expr: Expr, keyExpr: Expr) extends Expr with LazySafeLogging {

    def isPure: Boolean = expr.isPure && keyExpr.isPure

    // Key is not string or object is not map-like
    def cannotFail: Boolean = false

    def substitute(parameters: ScalaMap[Parameter, Value]): DynamicProperty = copy(
      expr = expr.substitute(parameters),
      keyExpr = keyExpr.substitute(parameters),
    )

    override def eval(
      qc: QueryContext,
    )(implicit idp: QuineIdProvider, p: Parameters, logConfig: LogConfig): Either[CypherException, Value] =
      expr.eval(qc) flatMap {
        case Node(_, _, props) =>
          for {
            k <- keyExpr.eval(qc)
            key <- k.asString("dynamic property on node")
          } yield props.getOrElse(Symbol(key), Null)

        case Relationship(_, _, props, _) =>
          for {
            k <- keyExpr.eval(qc)
            key <- k.asString("dynamic property on relationship")
          } yield props.getOrElse(Symbol(key), Null)

        case Map(props) =>
          for {
            k <- keyExpr.eval(qc)
            key <- k.asString("dynamic property on map")
          } yield props.getOrElse(key, Null)

        case LocalDateTime(t) =>
          for {
            k <- keyExpr.eval(qc)
            key <- k.asString("dynamic property on local date time")
          } yield temporalFields.get(key).fold[Value](Null)(u => Expr.Integer(t.getLong(u)))

        case dt @ DateTime(t) =>
          for {
            k <- keyExpr.eval(qc)
            key <- k.asString("dynamic property on local date time")
          } yield temporalFields
            .get(key)
            .map(u => Expr.Integer(t.getLong(u)))
            .orElse(dt.timezoneFields(key))
            .getOrElse(Null)

        case d @ Duration(_) =>
          for {
            k <- keyExpr.eval(qc)
            key <- k.asString("dynamic property on local date time")
          } yield temporalUnits
            .get(key)
            .toRight[Value](Null)
            .flatMap(units =>
              d.as(units).leftMap { e =>
                // If this dereference caused an overflow, we log a warning and return Null.
                // This is a deliberate deviation from Cypher idioms, which would have the exception wrapped
                // as a CypherException. We choose an error handling path here that avoids
                // terminating the query, because this is the kind of functionality that is likely present
                // in a stream (ingest or standing query output), and which is likely to work for some records
                // of that stream but fail on others. It is (arguably) a better user experience to have the
                // partially-processed stream and warnings indicating why the stream was only partially processed
                // than to require manual intervention. Ideally, however, this would be configured by the
                // stream's error handling mode.
                logger.warn(
                  log"""Duration property access failed on duration value: ${d.toString} due to arithmetic
                       |exception. Current row: ${qc.pretty} Returning Null.""".cleanLines withException e,
                )
                Null
              },
            )
            .merge
        case List(elems) =>
          (for {
            keyVal <- keyExpr.eval(qc)
            key <- keyVal.asLong("index into list")
            keyMod = if (key < 0) elems.length + key else key
            result <-
              if (!keyMod.isValidInt)
                CypherException.InvalidIndex(keyVal).asLeft
              else
                elems.applyOrElse(keyMod.toInt, (_: Int) => Null).asRight
          } yield result): @nowarn("cat=unused-pat-vars") // suppress false positive on keyMod from 2.13.15 bug

        case Null => Null.asRight

        case other =>
          CypherException
            .TypeMismatch(
              expected = Seq(
                Type.Map,
                Type.Node,
                Type.Relationship,
                Type.LocalDateTime,
                Type.DateTime,
                Type.Duration,
              ),
              actualValue = other,
              context = "dynamic property access",
            )
            .asLeft
      }
  }

  /** List slice
    *
    * {{{
    * RETURN range(0, 10)[0..3]
    * }}}
    *
    * @param list list that is being sliced
    * @param from lower bound of the slice
    * @param to upper bound of the slice
    */
  final case class ListSlice(list: Expr, from: Option[Expr], to: Option[Expr]) extends Expr {

    def isPure: Boolean = list.isPure && from.forall(_.isPure) && to.forall(_.isPure)

    // Non-list argument
    def cannotFail: Boolean = false

    def substitute(parameters: ScalaMap[Parameter, Value]): ListSlice = copy(
      list = list.substitute(parameters),
      from = from.map(_.substitute(parameters)),
      to = to.map(_.substitute(parameters)),
    )

    override def eval(
      qc: QueryContext,
    )(implicit idp: QuineIdProvider, p: Parameters, logConfig: LogConfig): Either[CypherException, Value] =
      list.eval(qc) flatMap {
        case List(elems) =>
          for {
            fromVal <- from.map { (fromExpr: Expr) =>
              (for {
                idx <- fromExpr.eval(qc)
                key <- idx.asLong("index into list")
                keyMod = if (key < 0) elems.length + key else key
                result <-
                  if (!keyMod.isValidInt)
                    CypherException.InvalidIndex(idx).asLeft
                  else keyMod.toInt.asRight
              } yield result): @nowarn("cat=unused-pat-vars") // suppress false positive on keyMod from 2.13.15 bug
            }.sequence

            toVal <- to.map { (toExpr: Expr) =>
              (for {
                idx <- toExpr.eval(qc)
                key <- idx.asLong("index into list")
                keyMod = if (key < 0) elems.length + key else key
                result <-
                  if (!keyMod.isValidInt)
                    CypherException.InvalidIndex(idx).asLeft
                  else keyMod.toInt.asRight
              } yield result): @nowarn("cat=unused-pat-vars") // suppress false positive on keyMod from 2.13.15 bug
            }.sequence

          } yield List(elems.slice(fromVal.getOrElse(0), toVal.getOrElse(elems.length)))

        case Null => Null.asRight

        case other =>
          CypherException
            .TypeMismatch(
              expected = Seq(Type.ListOfAnything),
              actualValue = other,
              context = "list slice",
            )
            .asLeft
      }
  }

  /** A constant parameter
    *
    * {{{
    * RETURN \$param.foo
    * }}}
    *
    * @param name name of the parameter
    */
  final case class Parameter(name: Int) extends Expr {

    val isPure: Boolean = true

    def cannotFail: Boolean = true

    def substitute(parameters: ScalaMap[Parameter, Value]): Expr = parameters.getOrElse(this, this)

    override def eval(
      qc: QueryContext,
    )(implicit idp: QuineIdProvider, p: Parameters, logConfig: LogConfig): Either[CypherException, Value] =
      p.params.apply(name).asRight
  }

  /** A list literal
    *
    * {{{
    * RETURN [1 + 2, "hello" ~= ".*lo", 2.0 ^ 4]
    * }}}
    *
    * @param expressions elements in the list literal
    */
  final case class ListLiteral(expressions: Vector[Expr]) extends Expr {

    def isPure: Boolean = expressions.forall(_.isPure)

    def cannotFail: Boolean = expressions.forall(_.cannotFail)

    def substitute(parameters: ScalaMap[Parameter, Value]): ListLiteral =
      copy(expressions = expressions.map(_.substitute(parameters)))

    override def eval(
      qc: QueryContext,
    )(implicit idp: QuineIdProvider, p: Parameters, logConfig: LogConfig): Either[CypherException, Value] =
      expressions.traverse(_.eval(qc)) map (List(_))
  }

  /** A map literal
    *
    * {{{
    * RETURN { name: "Joe " + "Blo", age: 40 + 2 }
    * }}}
    *
    * @param entries elements in the map literal
    */
  final case class MapLiteral(entries: ScalaMap[String, Expr]) extends Expr {

    def isPure: Boolean = entries.values.forall(_.isPure)

    def cannotFail: Boolean = entries.values.forall(_.cannotFail)

    def substitute(parameters: ScalaMap[Parameter, Value]): MapLiteral = copy(
      entries = entries.fmap(_.substitute(parameters)),
    )

    override def eval(
      qc: QueryContext,
    )(implicit idp: QuineIdProvider, p: Parameters, logConfig: LogConfig): Either[CypherException, Expr.Map] =
      (entries.to(collection.immutable.TreeMap): SortedMap[String, Expr]).traverse(_.eval(qc)).map(Map(_))
  }

  /** A map projection
    *
    * {{{
    * WITH { foo: 1, bar: 2 } AS M
    * RETURN m { .age, baz: "hello", .* }
    * }}}
    *
    * @param original value to project (node or map)
    * @param items new entries to add
    * @param includeAllProps keep all old entries
    */
  final case class MapProjection(
    original: Expr,
    items: Seq[(String, Expr)],
    includeAllProps: Boolean,
  ) extends Expr {

    def isPure: Boolean = original.isPure && items.forall(_._2.isPure)

    // Original value is not map-like
    def cannotFail: Boolean = false

    def substitute(parameters: ScalaMap[Parameter, Value]): MapProjection = copy(
      original = original.substitute(parameters),
      items = items.map { case (str, expr) => str -> expr.substitute(parameters) },
    )

    override def eval(
      qc: QueryContext,
    )(implicit idp: QuineIdProvider, p: Parameters, logConfig: LogConfig): Either[CypherException, Value] = for {
      newItems <- items.traverse { case (s, e) => e.eval(qc).map((s, _)) }
      baseMap <- original.eval(qc) flatMap {
        case Null => Null.asLeft.asRight
        case Map(theMap) => theMap.asRight.asRight
        case Node(_, _, theMap) => theMap.map { case (k, v) => k.name -> v }.asRight.asRight
        case Relationship(_, _, theMap, _) => theMap.map { case (k, v) => k.name -> v }.asRight.asRight
        case other =>
          CypherException
            .TypeMismatch(
              expected = Seq(Type.Map, Type.Node, Type.Relationship),
              actualValue = other,
              context = "map projection",
            )
            .asLeft
      }
      result = baseMap match {
        case Left(n) => n //Original evaluated to Null. Just return Null.
        case Right(m) if includeAllProps => Map(m ++ newItems.toMap)
        case _ => Map(newItems.toMap)
      }
    } yield result
  }

  /** Build a path. NOT IN CYPHER
    *
    * TODO: proper error handling
    *
    * @param nodeEdges alternating sequence of nodes and edges
    */
  final case class PathExpression(nodeEdges: Vector[Expr]) extends Expr {

    def isPure: Boolean = nodeEdges.forall(_.isPure)

    // Argument is not alternating node/relationship values
    def cannotFail: Boolean = false

    def substitute(parameters: ScalaMap[Parameter, Value]): PathExpression = copy(
      nodeEdges = nodeEdges.map(_.substitute(parameters)),
    )

    override def eval(
      qc: QueryContext,
    )(implicit idp: QuineIdProvider, p: Parameters, logConfig: LogConfig): Either[CypherException, Value] = for {
      evaled <- nodeEdges.traverse(_.eval(qc))
      head = evaled.head.asInstanceOf[Node]
      tail <- evaled.tail
        .grouped(2)
        .toVector
        .traverse {
          case Vector(r: Relationship, n: Node) => (r, n).asRight
          case _ => CypherException.Runtime("Path expression must alternate between relationship and node").asLeft
        }
    } yield Path(head, tail)
  }

  /** Extract the [[com.thatdot.quine.model.QuineId]] of the start of a relationship
    *
    * @param relationship the relationship whose start we are getting
    */
  final case class RelationshipStart(relationship: Expr) extends Expr {

    def isPure: Boolean = relationship.isPure

    // Argument is not a relationship
    def cannotFail: Boolean = false

    def substitute(parameters: ScalaMap[Parameter, Value]): RelationshipStart = copy(
      relationship = relationship.substitute(parameters),
    )

    override def eval(
      qc: QueryContext,
    )(implicit idp: QuineIdProvider, p: Parameters, logConfig: LogConfig): Either[CypherException, Value] =
      relationship.eval(qc) flatMap {
        case Null => Null.asRight
        case Relationship(start, _, _, _) => Bytes(start).asRight

        case other =>
          CypherException
            .TypeMismatch(
              expected = Seq(Type.Relationship),
              actualValue = other,
              context = "start of relationship",
            )
            .asLeft
      }
  }

  /** Extract the [[com.thatdot.quine.model.QuineId]] of the end of a relationship
    *
    * @param relationship the relationship whose end we are getting
    */
  final case class RelationshipEnd(relationship: Expr) extends Expr {

    def isPure: Boolean = relationship.isPure

    // Argument is not a relationship
    def cannotFail: Boolean = false

    def substitute(parameters: ScalaMap[Parameter, Value]): RelationshipEnd = copy(
      relationship = relationship.substitute(parameters),
    )

    override def eval(
      qc: QueryContext,
    )(implicit idp: QuineIdProvider, p: Parameters, logConfig: LogConfig): Either[CypherException, Value] =
      relationship.eval(qc) flatMap {
        case Null => Null.asRight
        case Relationship(_, _, _, end) => Bytes(end).asRight

        case other =>
          CypherException
            .TypeMismatch(
              expected = Seq(Type.Relationship),
              actualValue = other,
              context = "end of relationship",
            )
            .asLeft
      }
  }

  /** Expression equality
    *
    * {{{
    * RETURN 2 = 1.0 + 1.0
    * }}}
    *
    * @note cypher uses Kleene's strong three-valued logic with [[Null]]
    * @param lhs one side of the equality
    * @param rhs the other side of the equality
    */
  final case class Equal(lhs: Expr, rhs: Expr) extends Expr {

    def isPure: Boolean = lhs.isPure && rhs.isPure

    def cannotFail: Boolean = lhs.cannotFail && rhs.cannotFail

    def substitute(parameters: ScalaMap[Parameter, Value]): Equal = copy(
      lhs = lhs.substitute(parameters),
      rhs = rhs.substitute(parameters),
    )

    override def eval(
      qc: QueryContext,
    )(implicit idp: QuineIdProvider, p: Parameters, logConfig: LogConfig): Either[CypherException, Bool] =
      (lhs, rhs).eval(qc) map { case (l, r) => Value.compare(l, r) }
  }

  /** Convenience wrapper trait for all of the arithmetic expression forms */
  sealed abstract class ArithmeticExpr extends Expr {
    @inline
    def operation(n1: Number, n2: Number): Either[CypherException.Arithmetic, Number]

    @inline
    val contextName: String

    val lhs: Expr
    val rhs: Expr

    // Non-number arguments
    def cannotFail: Boolean = false

    override def eval(
      qc: QueryContext,
    )(implicit idp: QuineIdProvider, p: Parameters, logConfig: LogConfig): Either[CypherException, Number] =
      (lhs, rhs).eval(qc) flatMap {
        case (n1: Number, n2: Number) => operation(n1, n2)
        case (_: Number, other) =>
          CypherException
            .TypeMismatch(
              expected = Seq(Type.Number),
              actualValue = other,
              context = contextName,
            )
            .asLeft
        case (other, _) =>
          CypherException
            .TypeMismatch(
              expected = Seq(Type.Number),
              actualValue = other,
              context = contextName,
            )
            .asLeft
      }
  }

  /** Subtraction expression
    *
    * {{{
    * RETURN 3.0 - 2
    * }}}
    *
    * @param lhs left hand side of the subtraction
    * @param rhs right hand side of the subtraction
    */
  final case class Subtract(lhs: Expr, rhs: Expr) extends Expr {

    def isPure: Boolean = lhs.isPure && rhs.isPure

    // incompatible argument types
    def cannotFail: Boolean = false

    def substitute(parameters: ScalaMap[Parameter, Value]): Subtract = copy(
      lhs = lhs.substitute(parameters),
      rhs = rhs.substitute(parameters),
    )

    override def eval(
      qc: QueryContext,
    )(implicit idp: QuineIdProvider, p: Parameters, logConfig: LogConfig): Either[CypherException, Value] =
      (lhs, rhs).eval(qc) flatMap {
        case (Null, _) | (_, Null) => Null.asRight
        // Number return
        case (n1: Number, n2: Number) => n1 - n2
        // Subtract a duration from a date
        case (DateTime(t), Duration(d)) => DateTime(t.minus(d)).asRight
        case (LocalDateTime(t), Duration(d)) => LocalDateTime(t.minus(d)).asRight

        // Subtract a duration from a duration
        case (Duration(d1), Duration(d2)) => Duration(d1.minus(d2)).asRight

        // "Helpful" error messages trying to guess the alternative you wanted
        case (_: Number, other) =>
          CypherException
            .TypeMismatch(
              expected = Seq(Type.Number),
              actualValue = other,
              context = "subtraction",
            )
            .asLeft
        case (_: DateTime | _: LocalDateTime | _: Duration, other) =>
          CypherException
            .TypeMismatch(
              expected = Seq(Type.Duration),
              actualValue = other,
              context = "subtraction",
            )
            .asLeft
        case (other, _) =>
          CypherException
            .TypeMismatch(
              expected = Seq(Type.Number, Type.LocalDateTime, Type.DateTime, Type.Duration),
              actualValue = other,
              context = "subtraction",
            )
            .asLeft
      }
  }

  /** Multiplication expression
    *
    * {{{
    * RETURN 3.0 * 2
    * }}}
    *
    * TODO: multiply a duration
    *
    * @param lhs left hand side factor
    * @param rhs right hand side factor
    */
  final case class Multiply(lhs: Expr, rhs: Expr) extends ArithmeticExpr {

    def isPure: Boolean = lhs.isPure && rhs.isPure

    def substitute(parameters: ScalaMap[Parameter, Value]): Multiply = copy(
      lhs = lhs.substitute(parameters),
      rhs = rhs.substitute(parameters),
    )

    @inline
    def operation(n1: Number, n2: Number): Either[CypherException.Arithmetic, Number] = n1 * n2
    val contextName = "multiplication"
  }

  /** Division expression
    *
    * {{{
    * RETURN 3.0 / 2
    * }}}
    *
    * TODO: divide a duration
    *
    * @param lhs left hand side, dividend
    * @param rhs right hand side, divisor
    */
  final case class Divide(lhs: Expr, rhs: Expr) extends ArithmeticExpr {

    def isPure: Boolean = lhs.isPure && rhs.isPure

    def substitute(parameters: ScalaMap[Parameter, Value]): Divide = copy(
      lhs = lhs.substitute(parameters),
      rhs = rhs.substitute(parameters),
    )

    @inline
    def operation(n1: Number, n2: Number): Either[CypherException.Arithmetic, Number] = n1 / n2
    val contextName = "division"
  }

  /** Modulus expression
    *
    * {{{
    * RETURN 3.0 % 2
    * }}}
    *
    * @param lhs left hand side
    * @param rhs right hand side, modulo
    */
  final case class Modulo(lhs: Expr, rhs: Expr) extends ArithmeticExpr {

    def isPure: Boolean = lhs.isPure && rhs.isPure

    def substitute(parameters: ScalaMap[Parameter, Value]): Modulo = copy(
      lhs = lhs.substitute(parameters),
      rhs = rhs.substitute(parameters),
    )

    @inline
    def operation(n1: Number, n2: Number): Either[CypherException.Arithmetic, Number] = n1 % n2
    val contextName = "modulus"
  }

  /** Exponentiation expression
    *
    * {{{
    * RETURN 3.0 ^ 2
    * }}}
    *
    * @note this always returns a [[Floating]] (even when given [[Integer]]'s)
    * @param lhs left hand side, base
    * @param rhs right hand side, exponent
    */
  final case class Exponentiate(lhs: Expr, rhs: Expr) extends ArithmeticExpr {

    def isPure: Boolean = lhs.isPure && rhs.isPure

    def substitute(parameters: ScalaMap[Parameter, Value]): Exponentiate = copy(
      lhs = lhs.substitute(parameters),
      rhs = rhs.substitute(parameters),
    )

    @inline
    def operation(n1: Number, n2: Number): Either[CypherException.Arithmetic, Number] = n1 ^ n2
    val contextName = "exponentiation"
  }

  /** Addition expression
    *
    *  - can be string concatenation
    *  - number addition
    *  - list concatenation
    *  - list appending or prepending
    *
    * {{{
    * RETURN 3.0 + 2
    * }}}
    *
    * @note this is heavily overloaded!
    * @param lhs left hand side "addend"
    * @param rhs right hand side "addend"
    */
  final case class Add(lhs: Expr, rhs: Expr) extends Expr {

    def isPure: Boolean = lhs.isPure && rhs.isPure

    // Incompatible argument types
    def cannotFail: Boolean = false

    def substitute(parameters: ScalaMap[Parameter, Value]): Add = copy(
      lhs = lhs.substitute(parameters),
      rhs = rhs.substitute(parameters),
    )

    override def eval(
      qc: QueryContext,
    )(implicit idp: QuineIdProvider, p: Parameters, logConfig: LogConfig): Either[CypherException, Value] =
      (lhs, rhs).eval(qc) flatMap {
        case (Null, _) | (_, Null) => Null.asRight

        // String return
        case (Str(lhsStr), Str(rhsStr)) => Str(lhsStr + rhsStr).asRight
        case (Str(lhsStr), n: Number) => Str(lhsStr + n.string).asRight
        case (n: Number, Str(rhsStr)) => Str(n.string + rhsStr).asRight

        // Number return
        case (n1: Number, n2: Number) => n1 + n2

        // List return
        case (List(lhsList), List(rhsList)) => List(lhsList ++ rhsList).asRight
        case (nonList, List(rhsList)) => List(nonList +: rhsList).asRight
        case (List(lhsList), nonList) => List(lhsList :+ nonList).asRight

        // Adding duration to date (or vice-versa)
        case (DateTime(d), Duration(dur)) => DateTime(d.plus(dur)).asRight
        case (LocalDateTime(d), Duration(dur)) => LocalDateTime(d.plus(dur)).asRight
        case (Duration(dur), DateTime(d)) => DateTime(d.plus(dur)).asRight
        case (Duration(dur), LocalDateTime(d)) => LocalDateTime(d.plus(dur)).asRight

        // Adding duration to duration
        case (Duration(d1), Duration(d2)) => Duration(d1.plus(d2)).asRight

        // "Helpful" error messages trying to guess the alternative you wanted
        case (_: Str, other) =>
          CypherException
            .TypeMismatch(
              expected = Seq(Type.Number, Type.Str, Type.ListOfAnything),
              actualValue = other,
              context = "addition",
            )
            .asLeft
        case (_: Number, other) =>
          CypherException
            .TypeMismatch(
              expected = Seq(Type.Number, Type.Str, Type.ListOfAnything),
              actualValue = other,
              context = "addition",
            )
            .asLeft
        case (_: DateTime | _: LocalDateTime, other) =>
          CypherException
            .TypeMismatch(
              expected = Seq(Type.Duration),
              actualValue = other,
              context = "addition",
            )
            .asLeft
        case (_: Duration, other) =>
          CypherException
            .TypeMismatch(
              expected = Seq(Type.LocalDateTime, Type.DateTime, Type.Duration),
              actualValue = other,
              context = "addition",
            )
            .asLeft
        case (_, other) =>
          CypherException
            .TypeMismatch(
              expected = Seq(Type.ListOfAnything),
              actualValue = other,
              context = "addition",
            )
            .asLeft
      }
  }

  /** Unary addition expression
    *
    * {{{
    * RETURN +3.0
    * }}}
    *
    * @note this does nothing but assert its argument is numeric
    * @param argument right hand side number
    */
  final case class UnaryAdd(argument: Expr) extends Expr {

    def isPure: Boolean = argument.isPure

    // Non-number argument
    def cannotFail: Boolean = false

    def substitute(parameters: ScalaMap[Parameter, Value]): UnaryAdd = copy(
      argument = argument.substitute(parameters),
    )

    override def eval(
      qc: QueryContext,
    )(implicit idp: QuineIdProvider, p: Parameters, logConfig: LogConfig): Either[CypherException, Number] =
      argument.eval(qc) flatMap {
        case n: Number => n.asRight

        case other =>
          CypherException
            .TypeMismatch(
              expected = Seq(Type.Number),
              actualValue = other,
              context = "unary addition",
            )
            .asLeft
      }
  }

  /** Negation expression
    *
    * {{{
    * RETURN -3.0
    * }}}
    *
    * @param argument right hand side number
    */
  final case class UnarySubtract(argument: Expr) extends Expr {

    def isPure: Boolean = argument.isPure

    // Non-number argument
    def cannotFail: Boolean = false

    def substitute(parameters: ScalaMap[Parameter, Value]): UnarySubtract = copy(
      argument = argument.substitute(parameters),
    )

    override def eval(
      qc: QueryContext,
    )(implicit idp: QuineIdProvider, p: Parameters, logConfig: LogConfig): Either[CypherException, Number] =
      argument.eval(qc) flatMap {
        case n: Number => -n
        case other =>
          CypherException
            .TypeMismatch(
              expected = Seq(Type.Number),
              actualValue = other,
              context = "unary negation",
            )
            .asLeft
      }
  }

  /** Check if an expression is greater than or equal to another
    *
    * {{{
    * RETURN (1 + 2) >= 2.5
    * }}}
    *
    * @note cypher uses Kleene's strong three-valued logic with [[Null]]
    * @param lhs left-hand side of the inequality
    * @param rhs right-hand side of the inequality
    */
  final case class GreaterEqual(lhs: Expr, rhs: Expr) extends Expr {

    def isPure: Boolean = lhs.isPure && rhs.isPure

    // Incompatible types cannot be compared
    def cannotFail: Boolean = false

    def substitute(parameters: ScalaMap[Parameter, Value]): GreaterEqual = copy(
      lhs = lhs.substitute(parameters),
      rhs = rhs.substitute(parameters),
    )

    override def eval(
      qc: QueryContext,
    )(implicit idp: QuineIdProvider, p: Parameters, logConfig: LogConfig): Either[CypherException, Value] =
      (lhs, rhs).eval(qc) flatMap { case (l, r) =>
        Value.partialOrder.tryCompare(l, r) map {
          case Some(x) => if (x >= 0) True else False
          case None => Null
        }
      }
  }

  /** Check if an expression is less than or equal to another
    *
    * {{{
    * RETURN (1 + 2) <= 2.5
    * }}}
    *
    * @note cypher uses Kleene's strong three-valued logic with [[Null]]
    * @param lhs left-hand side of the inequality
    * @param rhs right-hand side of the inequality
    */
  final case class LessEqual(lhs: Expr, rhs: Expr) extends Expr {

    def isPure: Boolean = lhs.isPure && rhs.isPure

    // Incompatible types cannot be compared
    def cannotFail: Boolean = false

    def substitute(parameters: ScalaMap[Parameter, Value]): LessEqual = copy(
      lhs = lhs.substitute(parameters),
      rhs = rhs.substitute(parameters),
    )

    override def eval(
      qc: QueryContext,
    )(implicit idp: QuineIdProvider, p: Parameters, logConfig: LogConfig): Either[CypherException, Value] =
      (lhs, rhs).eval(qc) flatMap { case (l, r) =>
        Value.partialOrder.tryCompare(l, r) map {
          case Some(x) => if (x <= 0) True else False
          case None => Null
        }
      }
  }

  /** Check if an expression is strictly greate than another
    *
    * {{{
    * RETURN (1 + 2) > 2.5
    * }}}
    *
    * @note cypher uses Kleene's strong three-valued logic with [[Null]]
    * @param lhs left-hand side of the inequality
    * @param rhs right-hand side of the inequality
    */
  final case class Greater(lhs: Expr, rhs: Expr) extends Expr {

    def isPure: Boolean = lhs.isPure && rhs.isPure

    // Incompatible types cannot be compared
    def cannotFail: Boolean = false

    def substitute(parameters: ScalaMap[Parameter, Value]): Greater = copy(
      lhs = lhs.substitute(parameters),
      rhs = rhs.substitute(parameters),
    )

    override def eval(
      qc: QueryContext,
    )(implicit idp: QuineIdProvider, p: Parameters, logConfig: LogConfig): Either[CypherException, Value] =
      (lhs, rhs).eval(qc) flatMap { case (l, r) =>
        Value.partialOrder.tryCompare(l, r) map {
          case Some(x) => if (x > 0) True else False
          case None => Null
        }
      }
  }

  /** Check if an expression is strictly less than another
    *
    * {{{
    * RETURN (1 + 2) < 2.5
    * }}}
    *
    * @note cypher uses Kleene's strong three-valued logic with [[Null]]
    * @param lhs left-hand side of the inequality
    * @param rhs right-hand side of the inequality
    */
  final case class Less(lhs: Expr, rhs: Expr) extends Expr {

    def isPure: Boolean = lhs.isPure && rhs.isPure

    // Incompatible types cannot be compared
    def cannotFail: Boolean = false

    def substitute(parameters: ScalaMap[Parameter, Value]): Less = copy(
      lhs = lhs.substitute(parameters),
      rhs = rhs.substitute(parameters),
    )

    override def eval(
      qc: QueryContext,
    )(implicit idp: QuineIdProvider, p: Parameters, logConfig: LogConfig): Either[CypherException, Value] =
      (lhs, rhs).eval(qc) flatMap { case (l, r) =>
        Value.partialOrder.tryCompare(l, r) map {
          case Some(x) => if (x < 0) True else False
          case None => Null
        }
      }
  }

  /** Check if an expression is contained in a list
    *
    * {{{
    * RETURN (1 + 2) IN [1,2,3,4,5,6]
    * }}}
    *
    * @see [[https://neo4j.com/docs/cypher-manual/current/syntax/operators/#query-operators-list]]
    *
    * @note cypher uses Kleene's strong three-valued logic with [[Null]]
    * @param element expression to find in the list
    * @param list expressions to test against
    */
  final case class InList(element: Expr, list: Expr) extends Expr {

    def isPure: Boolean = element.isPure && list.isPure

    // Non-list RHS
    def cannotFail: Boolean = false

    def substitute(parameters: ScalaMap[Parameter, Value]): InList = copy(
      element = element.substitute(parameters),
      list = list.substitute(parameters),
    )

    override def eval(
      qc: QueryContext,
    )(implicit idp: QuineIdProvider, p: Parameters, logConfig: LogConfig): Either[CypherException, Value] =
      (element, list).eval(qc) flatMap {
        case (_, Null) => Null.asRight
        case (x, List(es)) =>
          es.foldLeft[Bool](False) { (acc: Bool, e: Value) =>
            acc.or(Value.compare(x, e))
          }.asRight
        case (_, other) =>
          CypherException
            .TypeMismatch(
              expected = Seq(Type.ListOfAnything),
              actualValue = other,
              context = "list containment",
            )
            .asLeft
      }
  }

  /** Check if a string starts with another string
    *
    * {{{
    * RETURN ("hell" + "o world") STARTS WITH "hello"
    * }}}
    *
    * @param scrutinee expression we are testing
    * @param startsWith prefix to look for
    */
  final case class StartsWith(scrutinee: Expr, startsWith: Expr) extends Expr {

    def isPure: Boolean = scrutinee.isPure && startsWith.isPure

    def cannotFail: Boolean = scrutinee.cannotFail && startsWith.cannotFail

    def substitute(parameters: ScalaMap[Parameter, Value]): StartsWith = copy(
      scrutinee = scrutinee.substitute(parameters),
      startsWith = startsWith.substitute(parameters),
    )

    override def eval(
      qc: QueryContext,
    )(implicit idp: QuineIdProvider, p: Parameters, logConfig: LogConfig): Either[CypherException, Bool] =
      (scrutinee, startsWith).eval(qc) map {
        case (Str(scrut), Str(start)) => Bool.apply(scrut.startsWith(start))
        case _ => Null
      }
  }

  /** Check if a string ends with another string
    *
    * {{{
    * RETURN ("hell" + "o world") ENDS WITH "world"
    * }}}
    *
    * @param scrutinee expression we are testing
    * @param endsWith suffix to look for
    */
  final case class EndsWith(scrutinee: Expr, endsWith: Expr) extends Expr {

    def isPure: Boolean = scrutinee.isPure && endsWith.isPure

    def cannotFail: Boolean = scrutinee.cannotFail && endsWith.cannotFail

    def substitute(parameters: ScalaMap[Parameter, Value]): EndsWith = copy(
      scrutinee = scrutinee.substitute(parameters),
      endsWith = endsWith.substitute(parameters),
    )

    override def eval(
      qc: QueryContext,
    )(implicit idp: QuineIdProvider, p: Parameters, logConfig: LogConfig): Either[CypherException, Bool] =
      (scrutinee, endsWith).eval(qc) map {
        case (Str(scrut), Str(end)) => Bool.apply(scrut.endsWith(end))
        case _ => Null
      }
  }

  /** Check if a string is contained in another string
    *
    * {{{
    * RETURN ("hell" + "o world") CONTAINS "lo wo"
    * }}}
    *
    * @param scrutinee expression we are testing
    * @param contained string to look for
    */
  final case class Contains(scrutinee: Expr, contained: Expr) extends Expr {

    def isPure: Boolean = scrutinee.isPure && contained.isPure

    def cannotFail: Boolean = scrutinee.cannotFail && contained.cannotFail

    def substitute(parameters: ScalaMap[Parameter, Value]): Contains = copy(
      scrutinee = scrutinee.substitute(parameters),
      contained = contained.substitute(parameters),
    )

    override def eval(
      qc: QueryContext,
    )(implicit idp: QuineIdProvider, p: Parameters, logConfig: LogConfig): Either[CypherException, Bool] =
      (scrutinee, contained).eval(qc) map {
        case (Str(scrut), Str(cont)) => Bool.apply(scrut.contains(cont))
        case _ => Null
      }
  }

  /** Check if a string matches a regex (represented as another string)
    *
    * {{{
    * RETURN ("hell" + "o world") =~ "^he[lo]{1,8} w.*"
    * }}}
    *
    * @note the regex must match the 'full' string body
    * @see [[https://neo4j.com/docs/cypher-manual/current/clauses/where/#query-where-regex]]
    *
    * @param scrutinee expression we are testing
    * @param regex pattern to check for full match
    *
    * TODO optimize by using a compiled and deduplicated Regex
    */
  final case class Regex(scrutinee: Expr, regex: Expr) extends Expr {

    def isPure: Boolean = scrutinee.isPure && regex.isPure

    // Regex pattern can be invalid
    def cannotFail: Boolean = false

    def substitute(parameters: ScalaMap[Parameter, Value]): Regex = copy(
      scrutinee = scrutinee.substitute(parameters),
      regex = regex.substitute(parameters),
    )

    override def eval(
      qc: QueryContext,
    )(implicit idp: QuineIdProvider, p: Parameters, logConfig: LogConfig): Either[CypherException, Bool] =
      (scrutinee, regex).eval(qc) map {
        case (Str(scrut), Str(reg)) => Bool.apply(scrut.matches(reg))
        case _ => Null
      }
  }

  /** Check if an expression is 'not' [[Null]]
    *
    * {{{
    * RETURN x IS NOT NULL
    * }}}
    *
    * @param notNull expression to test for existence
    */
  final case class IsNotNull(notNull: Expr) extends Expr {

    def isPure: Boolean = notNull.isPure

    def cannotFail: Boolean = notNull.cannotFail

    def substitute(parameters: ScalaMap[Parameter, Value]): IsNotNull = copy(
      notNull = notNull.substitute(parameters),
    )

    override def eval(
      qc: QueryContext,
    )(implicit idp: QuineIdProvider, p: Parameters, logConfig: LogConfig): Either[CypherException, Bool] =
      notNull.eval(qc) map {
        case Null => False
        case _ => True
      }
  }

  /** Check if an expression is [[Null]]
    *
    * {{{
    * RETURN x IS NULL
    * }}}
    *
    * @param isNull expression to test for existence
    */
  final case class IsNull(isNull: Expr) extends Expr {

    def isPure: Boolean = isNull.isPure

    def cannotFail: Boolean = isNull.cannotFail

    def substitute(parameters: ScalaMap[Parameter, Value]): IsNull = copy(
      isNull = isNull.substitute(parameters),
    )

    override def eval(
      qc: QueryContext,
    )(implicit idp: QuineIdProvider, p: Parameters, logConfig: LogConfig): Either[CypherException, Bool] =
      isNull.eval(qc) map {
        case Null => True
        case _ => False
      }
  }

  /** Logical negation of an expression
    *
    * {{{
    * RETURN NOT (person.isChild AND person.isMale)
    * }}}
    *
    * @note cypher uses Kleene's strong three-valued logic with [[Null]]
    * @param negated expression to negate
    */
  final case class Not(negated: Expr) extends Expr {

    def isPure: Boolean = negated.isPure

    // Non boolean argument
    def cannotFail: Boolean = false

    def substitute(parameters: ScalaMap[Parameter, Value]): Not = copy(
      negated = negated.substitute(parameters),
    )

    override def eval(
      qc: QueryContext,
    )(implicit idp: QuineIdProvider, p: Parameters, logConfig: LogConfig): Either[CypherException, Value] =
      negated.eval(qc) flatMap {
        case bool: Bool => bool.negate.asRight
        case other =>
          CypherException
            .TypeMismatch(
              expected = Seq(Type.Bool),
              actualValue = other,
              context = "logical NOT",
            )
            .asLeft
      }
  }

  /** Logical conjunction of expressions
    *
    * {{{
    * RETURN person.isChild AND person.isMale
    * }}}
    *
    * @note this does not short-circuit (exceptions would be unreliable)
    * @note cypher uses Kleene's strong three-valued logic with [[Null]]
    * @param conjuncts expressions AND-ed together
    */
  final case class And(conjuncts: Vector[Expr]) extends Expr {

    def isPure: Boolean = conjuncts.forall(_.isPure)

    // Non boolean arguments
    def cannotFail: Boolean = false

    def substitute(parameters: ScalaMap[Parameter, Value]): And = copy(
      conjuncts = conjuncts.map(_.substitute(parameters)),
    )

    override def eval(
      qc: QueryContext,
    )(implicit idp: QuineIdProvider, p: Parameters, logConfig: LogConfig): Either[CypherException, Bool] =
      conjuncts
        .traverse { boolExpr =>
          boolExpr.eval(qc).flatMap {
            case bool: Bool => bool.asRight
            case other =>
              CypherException
                .TypeMismatch(
                  expected = Seq(Type.Bool),
                  actualValue = other,
                  context = "operand of logical AND",
                )
                .asLeft
          }
        }
        .map(_.foldLeft[Bool](True) { case (acc: Bool, b: Bool) => acc.and(b) })
  }

  /** Logical disjunction of expressions
    *
    * {{{
    * RETURN person.isChild OR person.isMale
    * }}}
    *
    * @note this does not short-circuit (exceptions would be unreliable)
    * @note cypher uses Kleene's strong three-valued logic with [[Null]]
    * @param disjuncts expressions OR-ed together
    */
  final case class Or(disjuncts: Vector[Expr]) extends Expr {

    def isPure: Boolean = disjuncts.forall(_.isPure)

    // Non boolean arguments
    def cannotFail: Boolean = false

    def substitute(parameters: ScalaMap[Parameter, Value]): Or = copy(
      disjuncts = disjuncts.map(_.substitute(parameters)),
    )

    override def eval(
      qc: QueryContext,
    )(implicit idp: QuineIdProvider, p: Parameters, logConfig: LogConfig): Either[CypherException, Bool] =
      disjuncts
        .traverse { boolExpr =>
          boolExpr.eval(qc).flatMap {
            case bool: Bool => bool.asRight
            case other =>
              CypherException
                .TypeMismatch(
                  expected = Seq(Type.Bool),
                  actualValue = other,
                  context = "operand of logical OR",
                )
                .asLeft
          }
        }
        .map(_.foldLeft[Bool](False) { case (acc: Bool, b: Bool) => acc.or(b) })
  }

  /** Match for expressions
    *
    * {{{
    * RETURN CASE WHEN r.value = 1 THEN 1 ELSE 0 END
    * }}}
    *
    * @param scrutinee expression switch over (if none, implicitly `true`)
    * @param branches branches: conditions and right-hand-sides
    * @param default fallback expression (if none, implicitly [[Null]])
    */
  final case class Case(
    scrutinee: Option[Expr],
    branches: Vector[(Expr, Expr)],
    default: Option[Expr],
  ) extends Expr {

    def isPure: Boolean = scrutinee.forall(_.isPure) &&
      branches.forall(t => t._1.isPure && t._2.isPure) && default.forall(_.isPure)

    // If nothing matches, this return `NULL`, not an exception
    def cannotFail: Boolean = scrutinee.forall(_.cannotFail) &&
      branches.forall(t => t._1.cannotFail && t._2.cannotFail) && default.forall(_.cannotFail)

    def substitute(parameters: ScalaMap[Parameter, Value]): Case = copy(
      scrutinee = scrutinee.map(_.substitute(parameters)),
      branches = branches.map { case (l, r) => l.substitute(parameters) -> r.substitute(parameters) },
      default = default.map(_.substitute(parameters)),
    )

    override def eval(
      qc: QueryContext,
    )(implicit idp: QuineIdProvider, p: Parameters, logConfig: LogConfig): Either[CypherException, Value] = for {
      scrut <- scrutinee.getOrElse(True).eval(qc)
      found <- branches.findM { case (comp, _) => comp.eval(qc).map(e => Value.ordering.equiv(e, scrut)) }
      result <- found.map(_._2).orElse(default).fold((Null: Value).asRight[CypherException])(_.eval(qc))
    } yield result
  }

  /** Scalar function call
    *
    * {{{
    * RETURN cos(x) + sin(y)^2
    * }}}
    *
    * @note apart from `coalesce`, a [[Null]] argument means a [[Null]] return
    * @param function function to call
    * @param arguments expressions with which the function is called
    */
  final case class Function(
    function: Func,
    arguments: Vector[Expr],
  ) extends Expr {

    // TODO function purity should be determined per-signature, not per-function name
    def isPure: Boolean = function.isPure && arguments.forall(_.isPure)

    // TODO: consider tracking which _functions_ cannot fail
    def cannotFail: Boolean = false

    def substitute(parameters: ScalaMap[Parameter, Value]): Function =
      copy(arguments = arguments.map(_.substitute(parameters)))

    override def eval(
      qc: QueryContext,
    )(implicit idp: QuineIdProvider, p: Parameters, logConfig: LogConfig): Either[CypherException, Value] =
      arguments.traverse(_.eval(qc)) flatMap { argVals =>
        if (function != Func.Coalesce && argVals.contains(Expr.Null)) {
          Expr.Null.asRight
        } else {
          try Right(function.call(argVals))
          catch {
            case e: CypherException => Left(e)
          }
        }
      }
  }

  /** Filter & map a list
    *
    * {{{
    * RETURN [ x in range(0,10) WHERE x > 3 | x ^ 2 ]
    * }}}
    *
    * @note `variable` is in scope for only `filterPredicate` and `exttract`
    * @param variable the variable to bind for each element
    * @param list the list being filtered
    * @param filterPredicate the predicate which must hold to keep the element
    * @param extract the expression to calculate for each element
    */
  final case class ListComprehension(
    variable: Symbol,
    list: Expr,
    filterPredicate: Expr,
    extract: Expr,
  ) extends Expr {

    def isPure: Boolean = list.isPure && filterPredicate.isPure && extract.isPure

    def cannotFail: Boolean = list.cannotFail && filterPredicate.cannotFail && extract.cannotFail

    def substitute(parameters: ScalaMap[Parameter, Value]): ListComprehension = copy(
      list = list.substitute(parameters),
      filterPredicate = filterPredicate.substitute(parameters),
      extract = extract.substitute(parameters),
    )

    override def eval(
      qc: QueryContext,
    )(implicit idp: QuineIdProvider, p: Parameters, logConfig: LogConfig): Either[CypherException, List] =
      list
        .eval(qc)
        .flatMap(_.asList("filter comprehension"))
        .flatMap(_.traverse { (elem: Value) =>
          val newQc = qc + (variable -> elem)
          filterPredicate.eval(newQc).flatMap {
            case Expr.True => extract.eval(newQc).map(Vector(_))
            case _ => Vector.empty[Value].asRight[CypherException] // TODO: should we throw if we don't find a Bool?
          }
        })
        .map(l => Expr.List(l.flatten))
  }

  /** Check that a predicate holds for all elements in the list
    *
    * {{{
    * RETURN all(x IN [1,3,5,9] WHERE x % 2 = 1)
    * }}}
    *
    * @note `variable` is in scope for only `filterPredicate`
    * @note cypher uses Kleene's strong three-valued logic with [[Null]]
    *
    * @param variable the variable to bind for each element
    * @param list the list being examined
    * @param filterPredicate the predicate tested on every element element
    */
  final case class AllInList(
    variable: Symbol,
    list: Expr,
    filterPredicate: Expr,
  ) extends Expr {

    def isPure: Boolean = list.isPure && filterPredicate.isPure

    // Can fail when `filterPredicate` returns a non-boolean
    def cannotFail: Boolean = false

    def substitute(parameters: ScalaMap[Parameter, Value]): AllInList = copy(
      list = list.substitute(parameters),
      filterPredicate = filterPredicate.substitute(parameters),
    )

    override def eval(
      qc: QueryContext,
    )(implicit idp: QuineIdProvider, p: Parameters, logConfig: LogConfig): Either[CypherException, Bool] =
      list
        .eval(qc)
        .flatMap(_.asList("all list predicate"))
        .flatMap {
          _.foldM(True: Bool) { (acc: Bool, elem: Value) =>
            filterPredicate.eval(qc + (variable -> elem)).flatMap {
              case bool: Bool => acc.and(bool).asRight
              case other =>
                CypherException
                  .TypeMismatch(
                    expected = Seq(Type.Bool),
                    actualValue = other,
                    context = "predicate in `all`",
                  )
                  .asLeft
            }
          }
        }
  }

  /** Check that a predicate holds for at least one element in the list
    *
    * {{{
    * RETURN any(x IN [1,2,6,9] WHERE x % 2 = 0)
    * }}}
    *
    * @note `variable` is in scope for only `filterPredicate`
    * @note cypher uses Kleene's strong three-valued logic with [[Null]]
    *
    * @param variable the variable to bind for each element
    * @param list the list being examined
    * @param filterPredicate the predicate tested on every element element
    */
  final case class AnyInList(
    variable: Symbol,
    list: Expr,
    filterPredicate: Expr,
  ) extends Expr {

    def isPure: Boolean = list.isPure && filterPredicate.isPure

    // Can fail when `filterPredicate` returns a non-boolean
    def cannotFail: Boolean = false

    def substitute(parameters: ScalaMap[Parameter, Value]): AnyInList = copy(
      list = list.substitute(parameters),
      filterPredicate = filterPredicate.substitute(parameters),
    )

    override def eval(
      qc: QueryContext,
    )(implicit idp: QuineIdProvider, p: Parameters, logConfig: LogConfig): Either[CypherException, Bool] =
      list
        .eval(qc)
        .flatMap(_.asList("any list predicate"))
        .flatMap(_.foldM(False: Bool) { (acc: Bool, elem: Value) =>
          filterPredicate.eval(qc + (variable -> elem)).flatMap {
            case bool: Bool => acc.or(bool).asRight[CypherException]
            case other =>
              CypherException
                .TypeMismatch(
                  expected = Seq(Type.Bool),
                  actualValue = other,
                  context = "predicate in `any`",
                )
                .asLeft[Bool]
          }
        })
  }

  /** Check that a predicate holds for a single element in the list
    *
    * {{{
    * RETURN single(x IN [1,3,6,9] WHERE x % 2 = 0)
    * }}}
    *
    * @note `variable` is in scope for only `filterPredicate`
    * @note cypher uses Kleene's strong three-valued logic with [[Null]]
    *
    * @param variable the variable to bind for each element
    * @param list the list being examined
    * @param filterPredicate the predicate tested on every element element
    */
  final case class SingleInList(
    variable: Symbol,
    list: Expr,
    filterPredicate: Expr,
  ) extends Expr {

    def isPure: Boolean = list.isPure && filterPredicate.isPure

    // Can fail when `filterPredicate` returns a non-boolean
    def cannotFail: Boolean = false

    def substitute(parameters: ScalaMap[Parameter, Value]): SingleInList = copy(
      list = list.substitute(parameters),
      filterPredicate = filterPredicate.substitute(parameters),
    )

    override def eval(
      qc: QueryContext,
    )(implicit idp: QuineIdProvider, p: Parameters, logConfig: LogConfig): Either[CypherException, Bool] =
      list
        .eval(qc)
        .flatMap(_.asList("single list predicate"))
        .flatMap(_.foldM((0, false)) { (acc: (Int, Boolean), elem: Value) =>
          filterPredicate.eval(qc + (variable -> elem)).flatMap {
            case Null => acc.copy(_2 = true).asRight
            case True => acc.copy(_1 = acc._1 + 1).asRight
            case False => acc.asRight
            case other =>
              CypherException
                .TypeMismatch(
                  expected = Seq(Type.Bool),
                  actualValue = other,
                  context = "predicate in `single`",
                )
                .asLeft
          }
        })
        .map { case (truesCount: Int, sawNull: Boolean) =>
          //      val (truesCount: Int, sawNull: Boolean)
          if (truesCount > 1)
            False // Definitely more than one positive match
          else if (sawNull)
            Null // May have seen a [[True]], but the [[Null]]'s make it unclear
          else if (truesCount == 1)
            True // No [[Null]], one match
          else
            /* (truesCount == 0) */
            False
        }
  }

  /** Fold over a list (starting from the left), updating some accumulator
    *
    * {{{
    * RETURN reduce(acc = 1, x IN [1,3,6,9] | acc * x) AS product
    * }}}
    *
    * @note `accumulator` and `variable` are in scope for only `reducer`
    *
    * @param accumulator the variable that will hold partial results
    * @param initial the starting value of the accumulator
    * @param variable the variable to bind for each element
    * @param list the list being examined
    * @param reducer the expression re-evaluated at every list element
    */
  final case class ReduceList(
    accumulator: Symbol,
    initial: Expr,
    variable: Symbol,
    list: Expr,
    reducer: Expr,
  ) extends Expr {

    def isPure: Boolean = initial.isPure && list.isPure && reducer.isPure

    // Can fail when `list` returns a non-list
    def cannotFail: Boolean = false

    def substitute(parameters: ScalaMap[Parameter, Value]): ReduceList = copy(
      initial = initial.substitute(parameters),
      list = list.substitute(parameters),
      reducer = reducer.substitute(parameters),
    )

    override def eval(
      qc: QueryContext,
    )(implicit idp: QuineIdProvider, p: Parameters, logConfig: LogConfig): Either[CypherException, Value] =
      list
        .eval(qc)
        .flatMap(_.asList("reduce list"))
        .flatMap { l =>
          initial.eval(qc).flatMap { e =>
            l.foldM(e) { (acc: Value, elem: Value) =>
              val newQc = qc + (variable -> elem) + (accumulator -> acc)
              reducer.eval(newQc)
            }
          }
        }
  }

  /** Generates a fresh ID every time it is evaluated. This ID gets put into a
    * `Bytes` object.
    */
  case object FreshNodeId extends Expr {

    def isPure: Boolean = false

    def cannotFail: Boolean = true

    def substitute(parameters: ScalaMap[Parameter, Value]): FreshNodeId.type = this

    override def eval(
      qc: QueryContext,
    )(implicit idp: QuineIdProvider, p: Parameters, logConfig: LogConfig): Either[CypherException, Value] =
      Expr.fromQuineValue(idp.qidToValue(idp.newQid())).asRight
  }
}

/** A value in Cypher
  *
  * Values are the subset of expressions which evaluate to themselves. These
  * get classified into three categories:
  *
  *   - 'Property types': Number, String, Boolean, Point, Temporal
  *   - 'Structural types': nodes, relationships, paths
  *   - 'Composite types': lists, maps
  *
  * @see [[https://neo4j.com/docs/cypher-manual/current/syntax/values/]]
  */
sealed abstract class Value extends Expr {

  def isPure: Boolean = true

  def cannotFail: Boolean = true

  def substitute(parameters: ScalaMap[Expr.Parameter, Value]): Value = this

  def asLong(context: String): Either[CypherException.TypeMismatch, Long] = this match {
    case Expr.Integer(long) => long.asRight
    case other =>
      CypherException
        .TypeMismatch(
          expected = Seq(Type.Integer),
          actualValue = other,
          context,
        )
        .asLeft
  }

  def asNumber(context: String): Double = this match {
    case Expr.Number(num) => num
    case other =>
      throw CypherException.TypeMismatch(
        expected = Seq(Type.Number),
        actualValue = other,
        context,
      )
  }

  def asString(context: String): Either[CypherException, String] = this match {
    case Expr.Str(string) => string.asRight
    case other =>
      CypherException
        .TypeMismatch(
          expected = Seq(Type.Str),
          actualValue = other,
          context,
        )
        .asLeft
  }

  def asList(context: String): Either[CypherException.TypeMismatch, Vector[Value]] = this match {
    case Expr.List(l) => l.asRight
    case other =>
      CypherException
        .TypeMismatch(
          expected = Seq(Type.ListOfAnything),
          actualValue = other,
          context,
        )
        .asLeft
  }

  def asMap(context: String): Either[CypherException.TypeMismatch, ScalaMap[String, Value]] = this match {
    case Expr.Map(m) => m.asRight
    case other =>
      CypherException
        .TypeMismatch(
          expected = Seq(Type.Map),
          actualValue = other,
          context,
        )
        .asLeft
  }

  def asDuration(str: String): JavaDuration = this match {
    case Expr.Duration(d) => d
    case other =>
      throw CypherException.TypeMismatch(
        expected = Seq(Type.Duration),
        actualValue = other,
        str,
      )
  }

  def getField(context: String)(fieldName: String): Either[CypherException, Value] = asMap(context).flatMap { map =>
    map.get(fieldName).map(_.asRight).getOrElse {
      CypherException.NoSuchField(fieldName, map.keySet, context).asLeft
    }
  }
  override def eval(
    qc: QueryContext,
  )(implicit idp: QuineIdProvider, p: Parameters, logConfig: LogConfig): Either[CypherException, Value] = this.asRight

  /** Runtime representation of the type of the value
    *
    * This is useful for error reporting, especially type mismatch errors.
    */
  def typ: Type

  /** Hash of the value, using Guava's implementation of 128-bit murmur3 hash.
    *
    * This should be stable across product versions and JVMs, including dependency updates.
    * Where implementation uses `hashCode`, it does so on strings (which have very stable hashes)
    * or ZoneIds, which delegate to hashing their string representations
    *
    * TODO: re-consider whether this should work on [[Expr.Node]], [[Expr.Path]],
    * and [[Expr.Relationship]]
    *
    * @return 128-bit hash code
    */
  def hash: HashCode =
    addToHasher(Hashing.murmur3_128().newHasher()).hash()

  /** @see [[hash]]
    */
  def addToHasher(hasher: Hasher): Hasher

  /** Turn a value into its usual Java value.
    *
    *   - [[Expr.Node]], [[Expr.Relationship]], [[Expr.Path]] will error
    *   - [[Expr.List]] gets turned into a Scala `Vector`
    *   - [[Expr.Map]] gets turned into a Scala `Map`
    */
  def toAny: Either[CypherException.TypeMismatch, Any] = this match {
    case Expr.Str(str) => str.asRight
    case Expr.Integer(i) => i.asRight
    case Expr.Floating(f) => f.asRight
    case Expr.True => true.asRight
    case Expr.False => false.asRight
    case Expr.Null => Right(null)
    case Expr.Bytes(byteArray, _) => byteArray.asRight

    case e: Expr.Node =>
      CypherException
        .TypeMismatch(
          Seq.empty,
          e,
          "conversion to 'Any'",
        )
        .asLeft
    case e: Expr.Relationship =>
      CypherException
        .TypeMismatch(
          Seq.empty,
          e,
          "conversion to 'Any'",
        )
        .asLeft
    case e: Expr.Path =>
      CypherException
        .TypeMismatch(
          Seq.empty,
          e,
          "conversion to 'Any'",
        )
        .asLeft
    case Expr.List(cypherList) => cypherList.map(_.toAny).asRight
    case Expr.Map(cypherMap) => cypherMap.fmap(_.toAny).asRight

    case Expr.LocalDateTime(localDateTime) => localDateTime.asRight
    case Expr.DateTime(instant) => instant.asRight
    case Expr.Duration(duration) => duration.asRight
    case Expr.Date(date) => date.asRight
    case Expr.Time(time) => time.asRight
    case Expr.LocalTime(time) => time.asRight

  }

  /** Pretty print the value for consumption by the end-user. For debugging these values and presenting to an
    * operator, use [[com.thatdot.quine.util.Log.implicits.LogValue]] instead
    *
    * This should endeavour to round-trip parsing literals/expressions whenever possible
    */
  def pretty: String = this match {
    case Expr.Str(str) => "\"" + StringEscapeUtils.escapeJson(str) + "\""
    case Expr.Integer(i) => i.toString
    case Expr.Floating(f) => f.toString
    case Expr.True => "true"
    case Expr.False => "false"
    case Expr.Null => "null"
    case Expr.Bytes(b, representsId) =>
      if (representsId) {
        s"#${ByteConversions.formatHexBinary(b)}" // #-prefix matches [[QuineId.pretty]]
      } else {
        s"""bytes("${ByteConversions.formatHexBinary(b)}")"""
      }

    case Expr.Node(id, lbls, props) =>
      val propsStr = props
        .map { case (k, v) => s"${k.name}: ${v.pretty}" }
        .mkString(" { ", ", ", " }")
      val labels = lbls.map(_.name).mkString(":", ":", "")
      s"($id$labels${if (props.isEmpty) "" else propsStr})"

    case Expr.Relationship(start, id, props, end) =>
      val propsStr = props
        .map { case (k, v) => s"${k.name}: ${v.pretty}" }
        .mkString(" { ", ",", " }")
      s"($start)-[:${id.name}${if (props.isEmpty) "" else propsStr}]->($end)"

    case Expr.List(cypherList) =>
      cypherList
        .map(_.pretty)
        .mkString("[ ", ", ", " ]")

    case Expr.Map(cypherMap) =>
      cypherMap
        .map { case (k, v) => s"$k: ${v.pretty}" }
        .mkString("{ ", ", ", " }")

    case p: Expr.Path => p.toList.pretty
    case Expr.LocalDateTime(localDateTime) => s"""localdatetime("$localDateTime")"""
    case Expr.DateTime(zonedDateTime) => s"""datetime("$zonedDateTime")"""
    case Expr.Duration(duration) => s"""duration("$duration")"""
    case Expr.Date(date) => s"""date("$date")"""
    case Expr.Time(time) => s"""time("$time")"""
    case Expr.LocalTime(time) => s"""localtime("$time")"""
  }
}
object Value {
  // utility for comparing maps' (already key-sorted) entries
  private val sortedMapEntryOrdering = Ordering.Tuple2(Ordering.String, ordering)

  /** Compare two property values in a strict homogeneous fashion (ex: `x < y`)
    *
    * This order implements the conceptual model of "comparability"
    * outlined in the OpenCypher 9 spec.
    *
    * This form of comparison fails if given a non-property type (such as a
    * list or a node) or if given operands of different types.
    *
    * @see [[https://neo4j.com/docs/cypher-manual/current/syntax/operators/#cypher-comparison]]
    * @note the docs are stricter than Neo4j. I've followed in Neo4j's steps.
    *
    * @param lhs the left-hand side of the comparison
    * @param rhs the right-hand side of the comparison
    * @return a negative integer, zero, or a positive integer if the LHS is less than, equal to, or
    *         greater than the RHS (or [[scala.None]] if the comparison fails)
    */
  object partialOrder {
    @inline
    def tryCompare(lhs: Value, rhs: Value): Either[CypherException, Option[Int]] = (lhs, rhs) match {
      // `null` taints the whole comparison
      case (_, Expr.Null) | (Expr.Null, _) => None.asRight

      // Strings: lexicographic
      case (Expr.Str(s1), Expr.Str(s2)) => Some(s1.compareTo(s2)).asRight
      case (_: Expr.Str, other) =>
        CypherException
          .TypeMismatch(
            expected = Seq(Type.Str),
            actualValue = other,
            context = "right-hand side of a comparison",
          )
          .asLeft

      // Booleans: `false < true`
      case (Expr.False, Expr.False) => Some(0).asRight
      case (Expr.False, Expr.True) => Some(-1).asRight
      case (Expr.True, Expr.False) => Some(1).asRight
      case (Expr.True, Expr.True) => Some(0).asRight
      case (_: Expr.Bool, other) =>
        CypherException
          .TypeMismatch(
            expected = Seq(Type.Bool),
            actualValue = other,
            context = "right-hand side of a comparison",
          )
          .asLeft

      // Numbers: `NaN` is larger than all others
      case (Expr.Integer(i1), Expr.Integer(i2)) =>
        Some(JavaLong.compare(i1, i2)).asRight
      case (Expr.Integer(i1), Expr.Floating(f2)) =>
        Some(JavaDouble.compare(i1.toDouble, f2)).asRight
      case (Expr.Floating(f1), Expr.Integer(i2)) =>
        Some(JavaDouble.compare(f1, i2.toDouble)).asRight
      case (Expr.Floating(f1), Expr.Floating(f2)) =>
        Some(JavaDouble.compare(f1, f2)).asRight
      case (_: Expr.Number, other) =>
        CypherException
          .TypeMismatch(
            expected = Seq(Type.Number),
            actualValue = other,
            context = "right-hand side of a comparison",
          )
          .asLeft

      // Dates
      case (Expr.LocalDateTime(t1), Expr.LocalDateTime(t2)) => Some(t1.compareTo(t2)).asRight
      case (_: Expr.LocalDateTime, other) =>
        CypherException
          .TypeMismatch(
            expected = Seq(Type.LocalDateTime),
            actualValue = other,
            context = "right-hand side of a comparison",
          )
          .asLeft
      case (Expr.DateTime(i1), Expr.DateTime(i2)) => Some(i1.compareTo(i2)).asRight
      case (_: Expr.DateTime, other) =>
        CypherException
          .TypeMismatch(
            expected = Seq(Type.DateTime),
            actualValue = other,
            context = "right-hand side of a comparison",
          )
          .asLeft

      // Duration
      case (Expr.Duration(d1), Expr.Duration(d2)) => Some(d1.compareTo(d2)).asRight
      case (_: Expr.Duration, other) =>
        CypherException
          .TypeMismatch(
            expected = Seq(Type.Duration),
            actualValue = other,
            context = "right-hand side of a comparison",
          )
          .asLeft
      case (Expr.Map(m1), Expr.Map(m2)) =>
        if (m1.valuesIterator.contains(Expr.Null) || m2.valuesIterator.contains(Expr.Null)) {
          // Null makes maps incomparable
          None.asRight
        } else {
          // Otherwise match ORDER BY because the semantics are at our discretion
          Some(
            ((m1.view) zip (m2.view))
              .map { case (entry1, entry2) => sortedMapEntryOrdering.compare(entry1, entry2) }
              .dropWhile(_ == 0)
              .headOption
              .getOrElse(JavaInteger.compare(m1.size, m2.size)),
          ).asRight
        }
      case (_: Expr.Map, other) =>
        CypherException
          .TypeMismatch(
            expected = Seq(Type.Map),
            actualValue = other,
            context = "right-hand side of a comparison",
          )
          .asLeft

      // TODO: Compare lists, possibly more

      // Not comparable
      case (other, _) =>
        CypherException
          .TypeMismatch(
            expected = Seq(Type.Str, Type.Bool, Type.Number, Type.Duration, Type.LocalDateTime, Type.DateTime),
            actualValue = other,
            context = "left-hand side of a comparison",
          )
          .asLeft
    }
  }

  /** A reflexive, transitive, symmetric ordering of all values (for `ORDER BY`)
    *
    * This order implements the conceptual model of "orderability and equivalence"
    * outlined in the OpenCypher 9 spec.
    *
    * IMPORTANT: do not use this ordering in evaluating cypher expressions. In
    * expressions, you probably need [[partialOrder]]. This order explicitly
    * contradicts many expression language axioms:
    *
    *    - `null = null`  ==> true
    *    - `NaN = NaN`    ==> true
    *    - different types can always be compared (without fear of crash)
    */
  object ordering extends Ordering[Value] {
    def compare(v1: Value, v2: Value): Int = (v1, v2) match {

      // Null is the largest value
      case (Expr.Null, Expr.Null) => 0
      case (Expr.Null, _) => 1
      case (_, Expr.Null) => -1

      // Numbers come next... (note: `java.lang.Double.compare` is a total order and with NaN the biggest value)
      case (Expr.Integer(i1), Expr.Integer(i2)) =>
        JavaLong.compare(i1, i2)
      case (Expr.Integer(i1), Expr.Floating(f2)) =>
        JavaDouble.compare(i1.toDouble, f2)
      case (Expr.Floating(f1), Expr.Integer(i2)) =>
        JavaDouble.compare(f1, i2.toDouble)
      case (Expr.Floating(f1), Expr.Floating(f2)) =>
        JavaDouble.compare(f1, f2)
      case (_: Expr.Number, _) => 1
      case (_, _: Expr.Number) => -1

      // Booleans come next...
      case (Expr.False, Expr.False) => 0
      case (Expr.False, Expr.True) => -1
      case (Expr.True, Expr.False) => 1
      case (Expr.True, Expr.True) => 0
      case (_: Expr.Bool, _) => 1
      case (_, _: Expr.Bool) => -1

      // String come next...
      case (Expr.Str(s1), Expr.Str(s2)) => s1.compareTo(s2)
      case (_: Expr.Str, _) => 1
      case (_, _: Expr.Str) => -1

      // Duration comes next
      case (Expr.Duration(d1), Expr.Duration(d2)) => d1.compareTo(d2)
      case (_: Expr.Duration, _) => 1
      case (_, _: Expr.Duration) => -1

      // DateTime come next...
      case (Expr.DateTime(d1), Expr.DateTime(d2)) => d1.compareTo(d2)
      case (_: Expr.DateTime, _) => 1
      case (_, _: Expr.DateTime) => -1

      // LocalDateTime comes next...
      case (Expr.LocalDateTime(d1), Expr.LocalDateTime(d2)) => d1.compareTo(d2)
      case (_: Expr.LocalDateTime, _) => 1
      case (_, _: Expr.LocalDateTime) => -1

      // Date
      case (Expr.Date(d1), Expr.Date(d2)) => d1.compareTo(d2)
      case (_: Expr.Date, _) => 1
      case (_, _: Expr.Date) => -1

      // Time
      case (Expr.Time(t1), Expr.Time(t2)) => t1.compareTo(t2)
      case (_: Expr.Time, _) => 1
      case (_, _: Expr.Time) => -1

      // LocalTime
      case (Expr.LocalTime(t1), Expr.LocalTime(t2)) => t1.compareTo(t2)
      case (_: Expr.LocalTime, _) => 1
      case (_, _: Expr.LocalTime) => -1

      // Paths come next...
      // TODO: optimize this
      case (Expr.Path(n1, s1), Expr.Path(n2, s2)) =>
        val head = ordering.compare(n1, n2)
        val tails = s1
          .zip(s2)
          .view
          .map { case (t1, t2) =>
            Ordering.Tuple2(ordering, ordering).compare(t1, t2)
          }
        (head +: tails)
          .dropWhile(_ == 0)
          .headOption
          .getOrElse(0)
      case (_: Expr.Path, _) => 1
      case (_, _: Expr.Path) => -1

      // Lists come next...
      case (Expr.List(l1), Expr.List(l2)) =>
        l1.zip(l2)
          .view
          .map { case (v1, v2) => ordering.compare(v1, v2) }
          .dropWhile(_ == 0)
          .headOption
          .getOrElse(JavaInteger.compare(l1.size, l2.size))
      case (_: Expr.List, _) => 1
      case (_, _: Expr.List) => -1

      // Maps comes next...
      case (Expr.Map(m1), Expr.Map(m2)) =>
        // Map orderability written to be consistent with other cypher systems, though underspecified in openCypher.
        // See [[CypherEquality]] test suite for some examples
        ((m1.view) zip (m2.view))
          .map { case (entry1, entry2) => sortedMapEntryOrdering.compare(entry1, entry2) }
          .dropWhile(_ == 0)
          .headOption
          .getOrElse(JavaInteger.compare(m1.size, m2.size))
      case (_: Expr.Map, _) => 1
      case (_, _: Expr.Map) => 1

      // Next byte strings
      // TODO: where do these actually go?
      case (Expr.Bytes(b1, _), Expr.Bytes(b2, _)) =>
        TypeclassInstances.ByteArrOrdering.compare(b1, b2)
      case (_: Expr.Bytes, _) => 1
      case (_, _: Expr.Bytes) => 1

      // Next come edges...
      // TODO: calculate a proper ordering
      case (r1: Expr.Relationship, r2: Expr.Relationship) =>
        JavaInteger.compare(r1.hashCode, r2.hashCode)
      case (_: Expr.Relationship, _) => 1
      case (_, _: Expr.Relationship) => -1

      // Nodes have lowest priority...
      // TODO: calculate a proper ordering
      case (Expr.Node(id1, _, _), Expr.Node(id2, _, _)) =>
        JavaInteger.compare(id1.hashCode, id2.hashCode)
    }
  }

  /** Ternary comparison
    *
    * This comparison implements the conceptual model of "equality"  outlined in
    * the OpenCypher 9 spec. This is consistent with comparability (ie [[partialOrder]])
    * but not necessarily with orderability or equivalence (ie [[ordering]])
    *
    * [[Expr.Null]] represents some undetermined value. This leads to a handful of
    * surprising identities:
    *
    *   - `compare(null, null) = null` since the two values 'could' be equal
    *   - `compare([1,2], [null,2]) = null`
    *   - `compare([1,2], [null,3]) = false`
    *
    * Only structurally identical values should equal [[Expr.True]]. Values of
    * different types ([[Expr.Null]] aside) should always compare unequal. Another
    * exception: integers can be coerced to floating here.
    *
    * TODO: paths are treated as lists of alternating nodes and relationships
    *
    * @see [[https://neo4j.com/docs/cypher-manual/current/syntax/operators/#_equality]]
    *
    * @note not reflexive (`null != null`)
    * @note symmetric (forall `x` `y`. `x = y` -> `y = x`)
    *
    * @param value1 one value
    * @param value2 other value
    * @return a ternary boolean
    */
  def compare(value1: Value, value2: Value): Expr.Bool = (value1, value2) match {
    case (Expr.Null, _) | (_, Expr.Null) => Expr.Null

    case (Expr.Integer(i1), Expr.Integer(i2)) =>
      Expr.Bool(i1 == i2)
    case (Expr.Integer(i1), Expr.Floating(f2)) =>
      Expr.Bool.apply(i1.toDouble == f2)
    case (Expr.Floating(f1), Expr.Integer(i2)) =>
      Expr.Bool.apply(f1 == i2.toDouble)
    case (Expr.Floating(f1), Expr.Floating(f2)) =>
      Expr.Bool.apply(f1 == f2)

    case (Expr.True, Expr.True) => Expr.True
    case (Expr.False, Expr.False) => Expr.True
    case (Expr.Str(s1), Expr.Str(s2)) => Expr.Bool.apply(s1 == s2)
    case (Expr.Bytes(b1, _), Expr.Bytes(b2, _)) => Expr.Bool.apply(b1 sameElements b2)

    case (Expr.List(vs1), Expr.List(vs2)) if vs1.length == vs2.length =>
      vs1
        .zip(vs2)
        .view
        .map { case (v1, v2) => compare(v1, v2) }
        .foldLeft[Expr.Bool](Expr.True)(_ and _)
    case (Expr.Map(m1), Expr.Map(m2)) if m1.keySet == m2.keySet =>
      m1.view
        .map { case (k, v1) => compare(v1, m2(k)) } // since keysets matched, this is safe
        .foldLeft[Expr.Bool](Expr.True)(_ and _)

    // TODO: should we just look at IDs? If not, add a comment explaining why not
    case (Expr.Node(id1, l1, p1), Expr.Node(id2, l2, p2)) if (id1 == id2) && p1.keySet == p2.keySet && l1 == l2 =>
      p1.view
        .map { case (k, v1) => compare(v1, p2(k)) } // since keysets matched, this is safe
        .foldLeft[Expr.Bool](Expr.True)(_ and _)

      Expr.True
    case (Expr.Relationship(id1, s1, p1, id3), Expr.Relationship(id2, s2, p2, id4))
        if (id1 == id2) && (id3 == id4) &&
          s1 == s2 && p1.keySet == p2.keySet =>
      p1.view
        .map { case (k, v1) => compare(v1, p2(k)) } // since keysets matched, this is safe
        .foldLeft[Expr.Bool](Expr.True)(_ and _)

    case (Expr.LocalDateTime(d1), Expr.LocalDateTime(d2)) => Expr.Bool(d1 == d2)
    case (Expr.DateTime(d1), Expr.DateTime(d2)) => Expr.Bool(d1 == d2)
    case (Expr.Duration(d1), Expr.Duration(d2)) => Expr.Bool(d1 == d2)

    case _ => Expr.False
  }

  /** Extract a value into its usual Java representation
    *
    *   - [[scala.Vector]] gets turned into [[Expr.List]]
    *   - [[scala.collection.immutable.Map]] gets turned into [[Expr.Map]]
    */
  def fromAny(any: Any): Either[CypherException, Value] = any match {
    case null => Expr.Null.asRight
    case str: String => Expr.Str(str).asRight
    case long: Long => Expr.Integer(long).asRight
    case dbl: Double => Expr.Floating(dbl).asRight
    case true => Expr.True.asRight
    case false => Expr.False.asRight
    case bytes: Array[Byte] => Expr.Bytes(bytes).asRight

    case v: Vector[Any] => v.traverse(fromAny).map(Expr.List.apply)
    case l: List[Any] => l.traverse(fromAny).map(_.toVector).map(Expr.List.apply)
    case map: Map[_, _] =>
      map.toSeq
        .traverse {
          case (k: String, v) => fromAny(v).map((k, _))
          case other =>
            CypherException
              .Runtime(
                s"Value.fromAny: non-string key in map $other",
              )
              .asLeft
        }
        .map(m => Expr.Map(m.toMap))

    case localDateTime: JavaLocalDateTime => Expr.LocalDateTime(localDateTime).asRight
    case zonedDateTime: JavaZonedDateTime => Expr.DateTime(zonedDateTime).asRight
    case duration: JavaDuration => Expr.Duration(duration).asRight

    // TODO: what breaks if we remove these?
    case None => Expr.Null.asRight
    case Some(a) => fromAny(a)
    case int: Int => Expr.Integer(int.toLong).asRight

    case other =>
      CypherException
        .Runtime(
          s"Value.fromAny: unexpected Java value $other",
        )
        .asLeft
  }

  /** Attempt to decoded a Cypher value from a JSON-encoded value
    *
    * The right inverse of [[fromJson]] is [[toJson]], meaning that
    *
    * {{{
    * val roundtripped = fromJson(_).compose(toJson(_))
    * forAll { (json: Json) =>
    *   roundtripped(json) == json
    * }
    * }}}
    *
    * @see [[com.thatdot.quine.model.QuineValue.fromJson]]
    * @param jvalue json value to decode
    * @return decoded Cypher value
    */
  def fromJson(jvalue: Json): Value = jvalue.fold(
    Expr.Null,
    b => Expr.Bool(b),
    (n: JsonNumber) =>
      n.toLong match {
        case Some(l: Long) => Expr.Integer(l)
        case None => Expr.Floating(n.toDouble)
      },
    (s: String) => Expr.Str(s),
    (u: Seq[Json]) => Expr.List(u.map(fromJson): _*),
    (m: JsonObject) => Expr.Map(m.toMap.fmap(fromJson)),
  )

  /** Encode a Cypher value into JSON
    *
    * @see [[com.thatdot.quine.model.QuineValue.toJson]]
    * @param value Cypher value to encode
    * @param idProvider ID provider used to try to serialize IDs nicely
    * @return encoded JSON value
    */
  def toJson(value: Value)(implicit idProvider: QuineIdProvider, logConfig: LogConfig): Json = value match {
    case Expr.Null => Json.Null
    case Expr.Str(str) => Json.fromString(str)
    // Can't use `case Expr.Bool(b) =>` here because then scalac thinks the match isn't exhaustive
    // Can't use `case b: Expr.Bool =>` here because `Expr.Null` also extends `Bool`.
    case Expr.True => Json.fromBoolean(true)
    case Expr.False => Json.fromBoolean(false)
    case Expr.Integer(lng) => Json.fromLong(lng)
    case Expr.Floating(dbl) => Json.fromDoubleOrString(dbl)
    case Expr.List(vs) => Json.fromValues(vs.map(toJson))
    case Expr.Map(kvs) => Json.fromFields(kvs.map(kv => kv._1 -> toJson(kv._2)))
    case Expr.Bytes(byteArray, false) => Json.fromString(Base64.getEncoder.encodeToString(byteArray))
    case Expr.Bytes(byteArray, true) => Json.fromString(QuineId(byteArray).pretty)
    case Expr.LocalDateTime(localDateTime) => Json.fromString(localDateTime.toString)
    case Expr.DateTime(zonedDateTime) => Json.fromString(zonedDateTime.toString)
    case Expr.Duration(duration) => Json.fromString(duration.toString)
    case Expr.Date(date) => Json.fromString(date.toString)
    case Expr.Time(time) => Json.fromString(time.toString)
    case Expr.LocalTime(time) => Json.fromString(time.toString)
    case Expr.Node(qid, labels, props) =>
      Json.obj(
        "id" -> Json.fromString(qid.pretty),
        "labels" -> Json.fromValues(labels.map(sym => Json.fromString(sym.name))),
        "properties" -> Json.fromFields(props.map(kv => (kv._1.name, toJson(kv._2)))),
      )
    case Expr.Relationship(start, name, props, end) =>
      Json.obj(
        "start" -> Json.fromString(start.pretty),
        "end" -> Json.fromString(end.pretty),
        "name" -> Json.fromString(name.name),
        "properties" -> Json.fromFields(props.map(kv => (kv._1.name, toJson(kv._2)))),
      )
    case path: Expr.Path => toJson(path.toList)
  }
}

/** Constant parameters
  *
  *  @param params parameters that are held constant throughout the query
  */
final case class Parameters(
  params: IndexedSeq[Value],
)
object Parameters {
  val empty: Parameters = Parameters(IndexedSeq.empty)
}
