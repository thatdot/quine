package com.thatdot.quine.compiler.cypher

import com.thatdot.quine.graph.cypher.{Columns, Expr, Location, Query, Value}

/** This provides debug information about a query in a format that can be
  * consumed by `cypher-shell`. An example of this format is listed here:
  * <https://boltprotocol.org/v1/#explaining_and_profiling_a_query>
  *
  * TODO: add in `PROFILE` information too
  */
trait Plan {
  def operatorType: String
  def args: Map[String, Value]
  def identifiers: Vector[String]
  def children: Vector[Plan]
  def toValue: Value
}

final case class PlanRoot(
  operatorType: String,
  args: Map[String, Value],
  identifiers: Vector[String],
  children: Vector[Plan],
  isReadOnly: Boolean,
  isIdempotent: Boolean,
  canContainAllNodeScan: Boolean,
) extends Plan {

  /** Convert the plan into a Bolt-compatible value */
  def toValue: Value = Expr.Map(
    Map(
      "operatorType" -> Expr.Str(operatorType),
      "args" -> Expr.Map(args),
      "identifiers" -> Expr.List(identifiers.map(Expr.Str(_))),
      "children" -> Expr.List(children.map(_.toValue)),
      "isReadOnly" -> Expr.Bool(isReadOnly),
      "canContainAllNodeScan" -> Expr.Bool(canContainAllNodeScan),
      "isIdempotent" -> Expr.Bool(isIdempotent),
    ),
  )
}

final case class PlanChild(
  operatorType: String,
  args: Map[String, Value],
  identifiers: Vector[String],
  children: Vector[Plan],
) extends Plan {

  /** Convert the plan into a Bolt-compatible value */
  def toValue: Value = Expr.Map(
    Map(
      "operatorType" -> Expr.Str(operatorType),
      "args" -> Expr.Map(args),
      "identifiers" -> Expr.List(identifiers.map(Expr.Str(_))),
      "children" -> Expr.List(children.map(_.toValue)),
    ),
  )

}

object Plan {

  /** Produce a plan from a query
    *
    * @param query compiled query
    * @param isRoot whether this is a root of a tree of a queries
    * @return plan representation of the compiled query
    */
  def fromQuery(query: Query[Location], isRoot: Boolean = true): Plan = {

    val childrenQueries = Vector.newBuilder[Plan]
    val arguments = Map.newBuilder[String, Value]

    // In 2.13, we'd use `productElementNames` and `productIterator`
    val queryCls = query.getClass
    val fields = queryCls.getDeclaredFields
      .map { field =>
        try {
          val fieldName: String = field.getName
          val fieldGetter = queryCls.getDeclaredMethod(fieldName)
          Some(fieldName -> fieldGetter.invoke(query))
        } catch {
          case _: ReflectiveOperationException | _: NoSuchMethodException => None
        }
      }
      .collect { case Some(x) => x }
      .toVector

    for ((fieldName, value) <- fields)
      (fieldName, value) match {
        case (_, subQuery: Query[Location]) => childrenQueries += Plan.fromQuery(subQuery, isRoot = false)
        case ("columns", _) => // Ignore the columns, they get pulled out at the end of `fromQuery`
        case (field, value) =>
          // TODO: pretty-print expressions in the AST
          val cypherVal = Value.fromAny(value).getOrElse(Expr.Str(value.toString))
          arguments += field -> cypherVal
      }

    if (isRoot) {
      PlanRoot(
        operatorType = query.productPrefix,
        args = arguments.result(),
        identifiers = query.columns match {
          case Columns.Omitted => Vector.empty
          case Columns.Specified(cols) => cols.map(_.name)
        },
        children = childrenQueries.result(),
        isReadOnly = query.isReadOnly,
        isIdempotent = query.isIdempotent,
        canContainAllNodeScan = query.canContainAllNodeScan,
      )
    } else {
      PlanChild(
        operatorType = query.productPrefix,
        args = arguments.result(),
        identifiers = query.columns match {
          case Columns.Omitted => Vector.empty
          case Columns.Specified(cols) => cols.map(_.name)
        },
        children = childrenQueries.result(),
      )
    }
  }
}
