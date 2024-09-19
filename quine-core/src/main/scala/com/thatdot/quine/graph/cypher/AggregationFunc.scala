package com.thatdot.quine.graph.cypher

import java.time.Duration

import scala.collection.mutable.ArrayBuffer

import cats.implicits.catsSyntaxEitherId

import com.thatdot.quine.model.QuineIdProvider
import com.thatdot.quine.util.Log._
import com.thatdot.quine.util.MonadHelpers._

sealed abstract class Aggregator {

  /** Build an object which papers over the accumulator type by holding that
    * all internally and presenting a monomorphic interface to the external
    * world.
    */
  def aggregate()(implicit logConfig: LogConfig): AggregateState

  /** Is this a pure aggregate? A pure aggregate satisfies all of:
    *
    * - Returns a value that is fully computed from its inputs
    *   (therefore the same input always produce the same result)
    *
    * - Does not read or write any non-local state
    *
    * - Does not cause side effects
    */
  def isPure: Boolean

  /** Barring unbound variable or parameter exceptions, is it impossible for
    * the expression to throw exceptions when evaluated?
    */
  def cannotFail: Boolean

  /** substitute all parameters in this aggregator
    * @param parameters a [[Parameters]] providing parameters used by [[Expr.Parameter]]s within this aggregator.
    * @return a copy of this expression with all provided parameters substituted
    * INV: If all parameters used by [[Expr.Parameter]] instances are provided, the returned
    * aggregator will have no [[Expr.Parameter]]-typed [[Expr]]s remaining
    */
  def substitute(parameters: Map[Expr.Parameter, Value]): Aggregator
}

object Aggregator {

  /** Aggregator for [[Query.EagerAggregation]]
    *
    * @param initial initial value of the aggregator
    * @param computeOnEveryRow what to calculate for each row
    * @param combine the accumulated and newly-computed value
    * @param extractOutput for the aggregator
    */
  private def aggregateWith[Acc](
    initial: Acc,
    computeOnEveryRow: Expr,
    distinct: Boolean,
    combine: (Acc, Value) => Acc,
    extractOutput: Acc => Value,
  ): AggregateState = new AggregateState {
    private var state: Acc = initial
    private val seen = collection.mutable.HashSet.empty[Value]

    /** Aggregate results over a fresh row */
    def visitRow(qc: QueryContext)(implicit idp: QuineIdProvider, p: Parameters, logConfig: LogConfig): Unit = {
      val newValue: Value = computeOnEveryRow.eval(qc).getOrThrow
      if (!distinct || seen.add(newValue))
        state = combine(state, newValue)
    }

    /** Extract the aggregated result */
    def result(): Value =
      extractOutput(state)
  }

  /** Tally up the number of results */
  case object countStar extends Aggregator {
    def aggregate()(implicit logConfig: LogConfig): AggregateState = aggregateWith[Long](
      initial = 0L,
      computeOnEveryRow = Expr.Null,
      distinct = false,
      combine = (n: Long, _val: Value) => n + 1,
      extractOutput = Expr.Integer(_: Long),
    )
    val isPure = true
    def cannotFail = true
    def substitute(parameters: Map[Expr.Parameter, Value]): countStar.type = this
  }

  /** Tally up the number of non-null results */
  final case class count(distinct: Boolean, expr: Expr) extends Aggregator {
    def aggregate()(implicit logConfig: LogConfig): AggregateState = aggregateWith[Long](
      initial = 0L,
      computeOnEveryRow = expr,
      distinct,
      combine = (n: Long, value: Value) => if (value != Expr.Null) n + 1 else n,
      extractOutput = Expr.Integer(_: Long),
    )
    def isPure = expr.isPure
    def cannotFail = expr.cannotFail
    def substitute(parameters: Map[Expr.Parameter, Value]): count = copy(expr = expr.substitute(parameters))
  }

  /** Accumulate the results in a list value */
  final case class collect(distinct: Boolean, expr: Expr) extends Aggregator {
    def aggregate()(implicit logConfig: LogConfig): AggregateState = aggregateWith[List[Value]](
      initial = List.empty,
      computeOnEveryRow = expr,
      distinct,
      combine = (prev: List[Value], value: Value) => if (value != Expr.Null) value :: prev else prev,
      extractOutput = (l: List[Value]) => Expr.List(l.reverse.toVector),
    )
    def isPure = expr.isPure
    def cannotFail = expr.cannotFail
    def substitute(parameters: Map[Expr.Parameter, Value]): collect = copy(expr = expr.substitute(parameters))
  }

  /** Compute the average of numeric results */
  final case class avg(distinct: Boolean, expr: Expr) extends Aggregator {
    def aggregate()(implicit logConfig: LogConfig): AggregateState = aggregateWith[Option[(Long, Value)]](
      initial = None,
      computeOnEveryRow = expr,
      distinct,
      combine = (prev: Option[(Long, Value)], value: Value) =>
        value match {
          case Expr.Null => prev

          case Expr.Number(nextNumber) =>
            val (prevCount, prevTotalNumber) =
              prev
                .map { case (count, total) => count -> total.asNumber("average of values") }
                .getOrElse(0L -> 0.0)
            Some((prevCount + 1) -> Expr.Floating(prevTotalNumber + nextNumber))

          case Expr.Duration(nextDuration) =>
            val (prevCount, prevTotalDuration) = prev
              .map { case (count, total) => count -> total.asDuration("average of values") }
              .getOrElse(0L -> Duration.ZERO)
            Some(prevCount + 1 -> Expr.Duration(prevTotalDuration.plus(nextDuration)))

          case other =>
            throw CypherException.TypeMismatch(
              expected = Seq(Type.Number),
              actualValue = other,
              context = "average of values",
            )
        },
      extractOutput = (acc: Option[(Long, Value)]) =>
        acc match {
          case None => Expr.Null
          case Some((count, Expr.Number(numericTotal))) => Expr.Floating(numericTotal / count.toDouble)
          case Some((count, Expr.Duration(durationTotal))) => Expr.Duration(durationTotal.dividedBy(count))
          case Some((_, wrongTypeValue)) =>
            throw CypherException.TypeMismatch(
              expected = Seq(Type.Number, Type.Duration),
              actualValue = wrongTypeValue,
              context = "average of values",
            )
        },
    )

    def isPure: Boolean = expr.isPure

    // Non-number arguments
    def cannotFail: Boolean = false

    def substitute(parameters: Map[Expr.Parameter, Value]): avg = copy(expr = expr.substitute(parameters))
  }

  // TODO: this needs to work for duration types
  /** Compute the sum of numeric results.
    *
    * Expects inputs that are numbers (throws [[CypherException.TypeMismatch]]
    * if this is not the case).
    */
  final case class sum(distinct: Boolean, expr: Expr) extends Aggregator {
    def aggregate()(implicit logConfig: LogConfig): AggregateState = aggregateWith[Option[Value]](
      initial = None,
      computeOnEveryRow = expr,
      distinct,
      combine = (prev, value: Value) =>
        value match {
          case Expr.Null => prev
          case n: Expr.Number =>
            val p = prev match {
              case Some(i: Expr.Integer) => i
              case Some(f: Expr.Floating) => f
              case Some(other) =>
                throw CypherException.TypeMismatch(
                  expected = Seq(Type.Number),
                  actualValue = other,
                  context = "sum of values",
                )
              case None => Expr.Integer(0L)
            }
            Some((n + p).getOrThrow)
          case d: Expr.Duration =>
            val p = prev.map(_.asDuration("sum of values")).getOrElse(Duration.ZERO)
            Some(Expr.Duration(d.duration.plus(p)))
          case other =>
            throw CypherException.TypeMismatch(
              expected = Seq(Type.Number),
              actualValue = other,
              context = "sum of values",
            )
        },
      extractOutput = acc => acc.getOrElse(Expr.Integer(0L)),
    )

    def isPure: Boolean = expr.isPure

    // Non-number arguments
    def cannotFail: Boolean = false

    def substitute(parameters: Map[Expr.Parameter, Value]): sum = copy(expr = expr.substitute(parameters))
  }

  /** Compute the maximum of results.
    *
    * This follows the usual total ordering of values, with one exception:
    * [[Expr.Null]] values are ignored. Without any (non-[[Expr.Null]]) input
    * values, this returns [[Expr.Null]].
    */
  final case class max(expr: Expr) extends Aggregator {
    def aggregate()(implicit logConfig: LogConfig): AggregateState = aggregateWith[Option[Value]](
      initial = None,
      computeOnEveryRow = expr,
      distinct = false,
      combine = (prev, value: Value) =>
        value match {
          case Expr.Null => prev
          case other if prev.forall(Value.ordering.gt(other, _)) => Some(other)
          case _ => prev
        },
      extractOutput = _.getOrElse(Expr.Null),
    )

    def isPure: Boolean = expr.isPure

    def cannotFail: Boolean = expr.cannotFail

    def substitute(parameters: Map[Expr.Parameter, Value]): max = copy(expr = expr.substitute(parameters))
  }

  /** Compute the minimum of results.
    *
    * This follows the usual total ordering of values, with one exception:
    * [[Expr.Null]] values are ignored. Without any (non-[[Expr.Null]]) input
    * values, this returns [[Expr.Null]].
    */
  final case class min(expr: Expr) extends Aggregator {
    def aggregate()(implicit logConfig: LogConfig): AggregateState = aggregateWith[Option[Value]](
      initial = None,
      computeOnEveryRow = expr,
      distinct = false,
      combine = (prev: Option[Value], value: Value) =>
        value match {
          case Expr.Null => prev
          case other if prev.forall(Value.ordering.lt(other, _)) => Some(other)
          case _ => prev
        },
      extractOutput = (acc: Option[Value]) => acc.getOrElse(Expr.Null),
    )

    def isPure: Boolean = expr.isPure

    def cannotFail: Boolean = expr.cannotFail

    def substitute(parameters: Map[Expr.Parameter, Value]): min = copy(expr = expr.substitute(parameters))
  }

  /** Compute the standard deviation of results.
    *
    * This is intentionally done as the usual two-pass solution.
    *
    * @param expr expression for whose output we are calculating the standard deviation
    * @param partialSample is the sampling partial or complete (affects the denominator)
    */
  final case class StDev(expr: Expr, partialSampling: Boolean) extends Aggregator {
    def aggregate()(implicit logConfig: LogConfig): AggregateState = new AggregateState {
      var sum = 0.0d
      val original = ArrayBuffer.empty[Double]

      def visitRow(qc: QueryContext)(implicit idp: QuineIdProvider, p: Parameters, logConfig: LogConfig): Unit =
        expr.evalUnsafe(qc) match {
          // Skip null values
          case Expr.Null =>

          case Expr.Number(dbl) =>
            sum += dbl
            original += dbl

          case other =>
            throw CypherException.TypeMismatch(
              expected = Seq(Type.Number),
              actualValue = other,
              context = "standard deviation of values",
            )
        }

      def result(): Value = {
        val count = original.length
        val denominator = if (partialSampling) (count - 1) else count
        val average = sum / count.toDouble
        val numerator = original.foldLeft(0.0d) { case (sum, value) =>
          val diff = value - average
          sum + diff * diff
        }
        Expr.Floating(if (denominator <= 0) 0.0 else math.sqrt(numerator / denominator))
      }
    }

    def isPure: Boolean = expr.isPure

    // Non-number arguments
    def cannotFail: Boolean = false

    def substitute(parameters: Map[Expr.Parameter, Value]): StDev = copy(expr = expr.substitute(parameters))
  }

  /** Compute the percentile of results.
    *
    * @param expr expression for whose output we are calculating the percentile
    * @param percentileExpr expression for getting the percentile (between 0.0 and 1.0)
    * @param continuous is the sampling interpolated
    */
  final case class Percentile(expr: Expr, percentileExpr: Expr, continuous: Boolean) extends Aggregator {
    def aggregate()(implicit logConfig: LogConfig): AggregateState = new AggregateState {
      val original = ArrayBuffer.empty[Expr.Number]

      /** This is the percentile value and it gets filled in based on the firs
        * row fed into the aggregator. Yes, these semantics are a little bit
        * insane.
        */
      var percentileOpt: Option[Double] = None

      def visitRow(qc: QueryContext)(implicit idp: QuineIdProvider, p: Parameters, logConfig: LogConfig): Unit = {
        expr.evalUnsafe(qc) match {
          // Skip null values
          case Expr.Null =>

          case n: Expr.Number =>
            original += n

          case other =>
            throw CypherException.TypeMismatch(
              expected = Seq(Type.Number),
              actualValue = other,
              context = "percentile of values",
            )
        }

        // Fill in the percentile with the first row
        if (percentileOpt.isEmpty) {
          percentileExpr.evalUnsafe(qc) match {
            case Expr.Number(dbl) =>
              if (0.0d <= dbl && dbl <= 1.0d) {
                percentileOpt = Some(dbl)
              } else {
                throw CypherException.Runtime("percentile of values between 0.0 and 1.0")
              }

            case other =>
              throw CypherException.TypeMismatch(
                expected = Seq(Type.Number),
                actualValue = other,
                context = "percentile of values",
              )
          }
        }
      }

      def result(): Value = {
        val sorted = original.sorted(Value.ordering) // Switch to `sortInPlace` when 2.12 is dropped
        percentileOpt match {
          case None => Expr.Null
          case _ if sorted.length == 0 => Expr.Null
          case _ if sorted.length == 1 => sorted.head
          case Some(percentile) =>
            val indexDbl: Double = percentile * (sorted.length - 1)
            if (continuous) {
              val indexLhs = math.floor(indexDbl).toInt
              val indexRhs = math.ceil(indexDbl).toInt
              val mult: Double = indexDbl - indexLhs
              val valueLhs = sorted(indexLhs).asRight
              val valueRhs = sorted(indexRhs).asRight
              valueLhs + (Expr.Floating(mult).asRight[CypherException]) * (valueRhs - valueLhs)
            }.getOrThrow
            else {
              val index = math.round(indexDbl).toInt
              sorted(index)
            }
        }
      }
    }

    def isPure: Boolean = expr.isPure && percentileExpr.isPure

    // Non-number arguments
    def cannotFail: Boolean = false

    def substitute(parameters: Map[Expr.Parameter, Value]): Percentile = copy(
      expr = expr.substitute(parameters),
      percentileExpr = percentileExpr.substitute(parameters),
    )
  }
}

sealed abstract class AggregateState {

  /** Aggregate results over a fresh row
    *
    * @param qc     row of results
    * @param idp    ID provider
    * @param params constant parameters in the query
    */
  @throws[CypherException]
  def visitRow(qc: QueryContext)(implicit idp: QuineIdProvider, params: Parameters, logConfig: LogConfig): Unit

  /** Extract the aggregated result
    *
    * @return aggregated value result
    */
  def result(): Value
}
