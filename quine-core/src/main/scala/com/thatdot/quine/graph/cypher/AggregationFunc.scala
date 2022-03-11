package com.thatdot.quine.graph.cypher

import scala.collection.mutable.ArrayBuffer

import com.thatdot.quine.model.QuineIdProvider

sealed abstract class Aggregator {

  /** Build an object which papers over the accumulator type by holding that
    * all internally and presenting a monomorphic interface to the external
    * world.
    */
  def aggregate(): AggregateState

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
    extractOutput: Acc => Value
  ): AggregateState = new AggregateState {
    private var state: Acc = initial
    private val seen = collection.mutable.HashSet.empty[Value]

    /** Aggregate results over a fresh row */
    def visitRow(qc: QueryContext)(implicit idp: QuineIdProvider, p: Parameters): Unit = {
      val newValue: Value = computeOnEveryRow.eval(qc)
      if (!distinct || seen.add(newValue))
        state = combine(state, newValue)
    }

    /** Extract the aggregated result */
    def result(): Value =
      extractOutput(state)
  }

  /** Tally up the number of results */
  case object countStar extends Aggregator {
    def aggregate(): AggregateState = aggregateWith[Long](
      initial = 0L,
      computeOnEveryRow = Expr.Null,
      distinct = false,
      combine = (n: Long, _val: Value) => n + 1,
      extractOutput = Expr.Integer(_: Long)
    )
    val isPure = true
    def cannotFail = true
  }

  /** Tally up the number of non-null results */
  final case class count(distinct: Boolean, expr: Expr) extends Aggregator {
    def aggregate(): AggregateState = aggregateWith[Long](
      initial = 0L,
      computeOnEveryRow = expr,
      distinct,
      combine = (n: Long, value: Value) => if (value != Expr.Null) n + 1 else n,
      extractOutput = Expr.Integer(_: Long)
    )
    def isPure = expr.isPure
    def cannotFail = expr.cannotFail
  }

  /** Accumulate the results in a list value */
  final case class collect(distinct: Boolean, expr: Expr) extends Aggregator {
    def aggregate(): AggregateState = aggregateWith[List[Value]](
      initial = List.empty,
      computeOnEveryRow = expr,
      distinct,
      combine = (prev: List[Value], value: Value) => if (value != Expr.Null) value :: prev else prev,
      extractOutput = (l: List[Value]) => Expr.List(l.reverse.toVector)
    )
    def isPure = expr.isPure
    def cannotFail = expr.cannotFail
  }

  /** Compute the average of numeric results */
  final case class avg(distinct: Boolean, expr: Expr) extends Aggregator {
    def aggregate(): AggregateState = aggregateWith[Option[(Long, Double)]](
      initial = None,
      computeOnEveryRow = expr,
      distinct,
      combine = (prev: Option[(Long, Double)], value: Value) =>
        value match {
          case Expr.Null => prev

          case Expr.Integer(i) =>
            val (prevCount, prevTotal) = prev.getOrElse(0L -> 0.0)
            Some((prevCount + 1, prevTotal + i.toDouble))
          case Expr.Floating(f) =>
            val (prevCount, prevTotal) = prev.getOrElse(0L -> 0.0)
            Some((prevCount + 1, prevTotal + f))

          case other =>
            throw CypherException.TypeMismatch(
              expected = Seq(Type.Number),
              actualValue = other,
              context = "average of values"
            )
        },
      extractOutput = (acc: Option[(Long, Double)]) =>
        acc match {
          case None => Expr.Null
          case Some((count, total)) => Expr.Floating(total / count.toDouble)
        }
    )

    def isPure: Boolean = expr.isPure

    // Non-number arguments
    def cannotFail: Boolean = false
  }

  // TODO: this needs to work for duration types
  /** Compute the sum of numeric results.
    *
    * Expects inputs that are numbers (throws [[CypherException.TypeMismatch]]
    * if this is not the case).
    */
  final case class sum(distinct: Boolean, expr: Expr) extends Aggregator {
    def aggregate(): AggregateState = aggregateWith[Expr.Number](
      initial = Expr.Integer(0L),
      computeOnEveryRow = expr,
      distinct,
      combine = (prev, value: Value) =>
        value match {
          case Expr.Null => prev
          case n: Expr.Number => prev + n

          case other =>
            throw CypherException.TypeMismatch(
              expected = Seq(Type.Number),
              actualValue = other,
              context = "sum of values"
            )
        },
      extractOutput = acc => acc
    )

    def isPure: Boolean = expr.isPure

    // Non-number arguments
    def cannotFail: Boolean = false
  }

  /** Compute the maximum of results.
    *
    * This follows the usual total ordering of values, with one exception:
    * [[Expr.Null]] values are ignored. Without any (non-[[Expr.Null]]) input
    * values, this returns [[Expr.Null]].
    */
  final case class max(expr: Expr) extends Aggregator {
    def aggregate(): AggregateState = aggregateWith[Option[Value]](
      initial = None,
      computeOnEveryRow = expr,
      distinct = false,
      combine = (prev, value: Value) =>
        value match {
          case Expr.Null => prev
          case other if prev.forall(Value.ordering.gt(other, _)) => Some(other)
          case _ => prev
        },
      extractOutput = _.getOrElse(Expr.Null)
    )

    def isPure: Boolean = expr.isPure

    def cannotFail: Boolean = expr.cannotFail
  }

  /** Compute the minimum of results.
    *
    * This follows the usual total ordering of values, with one exception:
    * [[Expr.Null]] values are ignored. Without any (non-[[Expr.Null]]) input
    * values, this returns [[Expr.Null]].
    */
  final case class min(expr: Expr) extends Aggregator {
    def aggregate(): AggregateState = aggregateWith[Option[Value]](
      initial = None,
      computeOnEveryRow = expr,
      distinct = false,
      combine = (prev: Option[Value], value: Value) =>
        value match {
          case Expr.Null => prev
          case other if prev.forall(Value.ordering.lt(other, _)) => Some(other)
          case _ => prev
        },
      extractOutput = (acc: Option[Value]) => acc.getOrElse(Expr.Null)
    )

    def isPure: Boolean = expr.isPure

    def cannotFail: Boolean = expr.cannotFail
  }

  /** Compute the standard deviation of results.
    *
    * This is intentionally done as the usual two-pass solution.
    *
    * @param expr expression for whose output we are calculating the standard deviation
    * @param partialSample is the sampling partial or complete (affects the denominator)
    */
  final case class StDev(expr: Expr, partialSampling: Boolean) extends Aggregator {
    def aggregate(): AggregateState = new AggregateState {
      var sum = 0.0d
      val original = ArrayBuffer.empty[Double]

      def visitRow(qc: QueryContext)(implicit idp: QuineIdProvider, p: Parameters): Unit =
        expr.eval(qc) match {
          // Skip null values
          case Expr.Null =>

          case Expr.Number(dbl) =>
            sum += dbl
            original += dbl

          case other =>
            throw CypherException.TypeMismatch(
              expected = Seq(Type.Number),
              actualValue = other,
              context = "standard deviation of values"
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
  }

  /** Compute the percentile of results.
    *
    * @param expr expression for whose output we are calculating the percentile
    * @param percentileExpr expression for getting the percentile (between 0.0 and 1.0)
    * @param continuous is the sampling interpolated
    */
  final case class Percentile(expr: Expr, percentileExpr: Expr, continuous: Boolean) extends Aggregator {
    def aggregate(): AggregateState = new AggregateState {
      val original = ArrayBuffer.empty[Expr.Number]

      /** This is the percentile value and it gets filled in based on the firs
        * row fed into the aggregator. Yes, these semantics are a little bit
        * insane.
        */
      var percentileOpt: Option[Double] = None

      def visitRow(qc: QueryContext)(implicit idp: QuineIdProvider, p: Parameters): Unit = {
        expr.eval(qc) match {
          // Skip null values
          case Expr.Null =>

          case n: Expr.Number =>
            original += n

          case other =>
            throw CypherException.TypeMismatch(
              expected = Seq(Type.Number),
              actualValue = other,
              context = "percentile of values"
            )
        }

        // Fill in the percentile with the first row
        if (percentileOpt.isEmpty) {
          percentileExpr.eval(qc) match {
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
                context = "percentile of values"
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
              val valueLhs = sorted(indexLhs)
              val valueRhs = sorted(indexRhs)
              valueLhs + Expr.Floating(mult) * (valueRhs - valueLhs)
            } else {
              val index = math.round(indexDbl).toInt
              sorted(index)
            }
        }
      }
    }

    def isPure: Boolean = expr.isPure && percentileExpr.isPure

    // Non-number arguments
    def cannotFail: Boolean = false
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
  def visitRow(qc: QueryContext)(implicit idp: QuineIdProvider, params: Parameters): Unit

  /** Extract the aggregated result
    *
    * @return aggregated value result
    */
  def result(): Value
}
