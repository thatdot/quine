package com.thatdot.quine.graph.cypher

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
    def isPure = expr.isPure
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
    def isPure = expr.isPure
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
    def isPure = expr.isPure
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
    def isPure = expr.isPure
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
