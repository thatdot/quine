package com.thatdot.quine.compiler.cypher

import org.opencypher.v9_0.util.helpers.NameDeduplicator

import com.thatdot.quine.graph.cypher.{Columns, Expr, Location, Query}

/* Fill in column-schema information in queries
 *
 *  - Variables are assumed to always be fresh (`openCypher` will freshen them)
 *    There should consequently be no variable shadowing happening, ever.
 *
 *
 */
object VariableRewriter {
  import Query._

  def convertAnyQuery(
    query: Query[Location.Anywhere],
    columnsIn: Columns
  ): Query[Location.Anywhere] = {
    val converted = AnywhereConverter.convertQuery(query, columnsIn)
    converted.columns match {
      case Columns.Omitted => converted
      case Columns.Specified(mangledCols) => // if columns are specified, de-mangle them

        val demangledCols = mangledCols.map { case Symbol(mangledName) =>
          val col = NameDeduplicator.removeGeneratedNamesAndParams(mangledName)
          Symbol(col)
        }

        if (demangledCols == mangledCols) converted
        else {
          Query.AdjustContext(
            dropExisting = true,
            toAdd = demangledCols
              .zip(mangledCols)
              .map { case (demangled, mangled) => demangled -> Expr.Variable(mangled) },
            adjustThis = converted,
            columns = Columns.Specified(demangledCols)
          )
        }
    }
  }

  def convertNodeQuery(
    query: Query[Location.OnNode],
    columnsIn: Columns
  ): Query[Location.OnNode] = OnNodeConverter.convertQuery(query, columnsIn)

  private object OnNodeConverter extends VariableRewriter[Location.OnNode] {
    def convertQuery(
      query: Query[Location.OnNode],
      columnsIn: Columns
    ): Query[Location.OnNode] = query match {
      case query: Empty => convertEmpty(query, columnsIn)
      case query: Unit => convertUnit(query, columnsIn)
      case query: AnchoredEntry => convertAnchoredEntry(query, columnsIn)
      case query: ArgumentEntry => convertArgumentEntry(query, columnsIn)
      case query: Expand => convertExpand(query, columnsIn)
      case query: LocalNode => convertLocalNode(query, columnsIn)
      case query: GetDegree => convertGetDegree(query, columnsIn)
      case query: LoadCSV => convertLoadCSV(query, columnsIn)
      case query: Union[Location.OnNode @unchecked] => convertUnion(query, columnsIn)
      case query: Or[Location.OnNode @unchecked] => convertOr(query, columnsIn)
      case query: ValueHashJoin[Location.OnNode @unchecked] =>
        convertValueHashJoin(query, columnsIn)
      case query: SemiApply[Location.OnNode @unchecked] => convertSemiApply(query, columnsIn)
      case query: Apply[Location.OnNode @unchecked] => convertApply(query, columnsIn)
      case query: Optional[Location.OnNode @unchecked] => convertOptional(query, columnsIn)
      case query: Filter[Location.OnNode @unchecked] => convertFilter(query, columnsIn)
      case query: Skip[Location.OnNode @unchecked] => convertSkip(query, columnsIn)
      case query: Limit[Location.OnNode @unchecked] => convertLimit(query, columnsIn)
      case query: Sort[Location.OnNode @unchecked] => convertSort(query, columnsIn)
      case query: Return[Location.OnNode @unchecked] => convertReturn(query, columnsIn)
      case query: Distinct[Location.OnNode @unchecked] => convertDistinct(query, columnsIn)
      case query: Unwind[Location.OnNode @unchecked] => convertUnwind(query, columnsIn)
      case query: AdjustContext[Location.OnNode @unchecked] =>
        convertAdjustContext(query, columnsIn)
      case query: SetProperty => convertSetProperty(query, columnsIn)
      case query: SetProperties => convertSetProperties(query, columnsIn)
      case query: SetEdge => convertSetEdge(query, columnsIn)
      case query: SetLabels => convertSetLabels(query, columnsIn)
      case query: EagerAggregation[Location.OnNode @unchecked] =>
        convertEagerAggregation(query, columnsIn)
      case query: Delete => convertDelete(query, columnsIn)
      case query: ProcedureCall => convertProcedureCall(query, columnsIn)
      case query: SubQuery[Location.OnNode @unchecked] => convertSubQuery(query, columnsIn)
    }

    protected def convertGetDegree(
      query: GetDegree,
      columnsIn: Columns
    ): GetDegree = {
      val cols = columnsIn + query.bindName
      query.copy(columns = cols)
    }

    protected def convertExpand(
      query: Expand,
      columnsIn: Columns
    ): Expand = {
      val cols = query.bindRelation match {
        case None => columnsIn
        case Some(br) => columnsIn + br
      }
      val andThen = convertQuery(query.andThen, cols)
      query.copy(andThen = andThen, columns = andThen.columns)
    }

    protected def convertLocalNode(
      query: LocalNode,
      columnsIn: Columns
    ): LocalNode = {
      val cols = query.bindName match {
        case None => columnsIn
        case Some(bn) => columnsIn + bn
      }
      query.copy(columns = cols)
    }

    protected def convertSetProperty(
      query: SetProperty,
      columnsIn: Columns
    ): SetProperty = query.copy(columns = columnsIn)

    protected def convertSetProperties(
      query: SetProperties,
      columnsIn: Columns
    ): SetProperties = query.copy(columns = columnsIn)

    protected def convertSetEdge(
      query: SetEdge,
      columnsIn: Columns
    ): SetEdge = {
      val cols = query.bindRelation match {
        case None => columnsIn
        case Some(br) => columnsIn + br
      }
      val andThen = convertQuery(query.andThen, cols)
      query.copy(
        andThen = andThen,
        columns = andThen.columns
      )
    }

    protected def convertSetLabels(
      query: SetLabels,
      columnsIn: Columns
    ): SetLabels = query.copy(columns = columnsIn)

  }

  private object AnywhereConverter extends VariableRewriter[Location.Anywhere] {
    def convertQuery(
      query: Query[Location.Anywhere],
      columnsIn: Columns
    ): Query[Location.Anywhere] = query match {
      case query: Empty => convertEmpty(query, columnsIn)
      case query: Unit => convertUnit(query, columnsIn)
      case query: AnchoredEntry => convertAnchoredEntry(query, columnsIn)
      case query: ArgumentEntry => convertArgumentEntry(query, columnsIn)
      case query: LoadCSV => convertLoadCSV(query, columnsIn)
      case query: Union[Location.Anywhere @unchecked] => convertUnion(query, columnsIn)
      case query: Or[Location.Anywhere @unchecked] => convertOr(query, columnsIn)
      case query: ValueHashJoin[Location.Anywhere @unchecked] =>
        convertValueHashJoin(query, columnsIn)
      case query: SemiApply[Location.Anywhere @unchecked] => convertSemiApply(query, columnsIn)
      case query: Apply[Location.Anywhere @unchecked] => convertApply(query, columnsIn)
      case query: Optional[Location.Anywhere @unchecked] => convertOptional(query, columnsIn)
      case query: Filter[Location.Anywhere @unchecked] => convertFilter(query, columnsIn)
      case query: Skip[Location.Anywhere @unchecked] => convertSkip(query, columnsIn)
      case query: Limit[Location.Anywhere @unchecked] => convertLimit(query, columnsIn)
      case query: Sort[Location.Anywhere @unchecked] => convertSort(query, columnsIn)
      case query: Return[Location.Anywhere @unchecked] => convertReturn(query, columnsIn)
      case query: Distinct[Location.Anywhere @unchecked] => convertDistinct(query, columnsIn)
      case query: Unwind[Location.Anywhere @unchecked] => convertUnwind(query, columnsIn)
      case query: AdjustContext[Location.Anywhere @unchecked] =>
        convertAdjustContext(query, columnsIn)
      case query: EagerAggregation[Location.Anywhere @unchecked] =>
        convertEagerAggregation(query, columnsIn)
      case query: Delete => convertDelete(query, columnsIn)
      case query: ProcedureCall => convertProcedureCall(query, columnsIn)
      case query: SubQuery[Location.Anywhere @unchecked] => convertSubQuery(query, columnsIn)
    }
  }

  def convertExpr(
    columnsIn: Columns,
    expr: Expr
  ): Expr = expr
}

trait VariableRewriter[Start <: Location] {
  import Query._

  def convertQuery(
    query: Query[Start],
    columnsIn: Columns
  ): Query[Start]

  /* Columns unaffacted */
  protected def convertEmpty(
    query: Empty,
    columnsIn: Columns
  ): Empty = query.copy(columns = columnsIn)

  /* Columns unaffacted */
  protected def convertUnit(
    query: Unit,
    columnsIn: Columns
  ): Unit = query.copy(columns = columnsIn)

  protected def convertAnchoredEntry(
    query: AnchoredEntry,
    columnsIn: Columns
  ): AnchoredEntry = {
    val andThen = VariableRewriter.convertNodeQuery(query.andThen, columnsIn)
    query.copy(
      andThen = andThen,
      columns = andThen.columns
    )
  }

  protected def convertArgumentEntry(
    query: ArgumentEntry,
    columnsIn: Columns
  ): ArgumentEntry = {
    val andThen = VariableRewriter.convertNodeQuery(query.andThen, columnsIn)
    query.copy(
      andThen = andThen,
      columns = andThen.columns
    )
  }

  protected def convertLoadCSV(
    query: LoadCSV,
    columnsIn: Columns
  ): LoadCSV =
    query.copy(columns = columnsIn + query.variable)

  protected def convertUnion(
    query: Union[Start],
    columnsIn: Columns
  ): Union[Start] = {
    val unionLhs = convertQuery(query.unionLhs, columnsIn)
    val unionRhs = convertQuery(query.unionRhs, columnsIn)

    // TODO: should this even be possible?
    require(unionLhs.columns == unionRhs.columns, "Union branches have different columns")
    query.copy(
      unionLhs = unionLhs,
      unionRhs = unionRhs,
      columns = unionLhs.columns
    )
  }

  protected def convertOr(
    query: Or[Start],
    columnsIn: Columns
  ): Or[Start] = {
    val tryFirst = convertQuery(query.tryFirst, columnsIn)
    val trySecond = convertQuery(query.trySecond, columnsIn)

    // TODO: should this even be possible?
    require(
      tryFirst.columns == trySecond.columns,
      s"Or branches have different columns ${tryFirst.columns} ${trySecond.columns}"
    )
    query.copy(
      tryFirst = tryFirst,
      trySecond = trySecond,
      columns = tryFirst.columns
    )
  }

  protected def convertValueHashJoin(
    query: ValueHashJoin[Start],
    columnsIn: Columns
  ): ValueHashJoin[Start] = {
    val joinLhs = convertQuery(query.joinLhs, columnsIn)
    val joinRhs = convertQuery(query.joinRhs, columnsIn)

    query.copy(
      joinLhs = joinLhs,
      joinRhs = joinRhs,
      columns = joinLhs.columns ++ joinRhs.columns
    )
  }

  protected def convertSemiApply(
    query: SemiApply[Start],
    columnsIn: Columns
  ): SemiApply[Start] = {
    val testQuery = convertQuery(query.acceptIfThisSucceeds, columnsIn)
    query.copy(
      acceptIfThisSucceeds = testQuery,
      columns = columnsIn
    )
  }

  protected def convertApply(
    query: Apply[Start],
    columnsIn: Columns
  ): Apply[Start] = {
    val start = convertQuery(query.startWithThis, columnsIn)
    val thenCross = convertQuery(query.thenCrossWithThis, start.columns)
    query.copy(
      startWithThis = start,
      thenCrossWithThis = thenCross,
      columns = thenCross.columns
    )
  }

  protected def convertOptional(
    query: Optional[Start],
    columnsIn: Columns
  ): Optional[Start] = {
    val optional = convertQuery(query.query, columnsIn)
    query.copy(query = optional, columns = optional.columns)
  }

  protected def convertFilter(
    query: Filter[Start],
    columnsIn: Columns
  ): Filter[Start] = {
    val toFilter = convertQuery(query.toFilter, columnsIn)
    query.copy(toFilter = toFilter, columns = toFilter.columns)
  }

  protected def convertSkip(
    query: Skip[Start],
    columnsIn: Columns
  ): Skip[Start] = {
    val toSkip = convertQuery(query.toSkip, columnsIn)
    query.copy(toSkip = toSkip, columns = toSkip.columns)
  }

  protected def convertLimit(
    query: Limit[Start],
    columnsIn: Columns
  ): Limit[Start] = {
    val toLimit = convertQuery(query.toLimit, columnsIn)
    query.copy(toLimit = toLimit, columns = toLimit.columns)
  }

  protected def convertSort(
    query: Sort[Start],
    columnsIn: Columns
  ): Sort[Start] = {
    val toSort = convertQuery(query.toSort, columnsIn)
    query.copy(toSort = toSort, columns = toSort.columns)
  }

  protected def convertReturn(
    query: Return[Start],
    columnsIn: Columns
  ): Return[Start] = {
    val toReturn = convertQuery(query.toReturn, columnsIn)
    query.copy(toReturn = toReturn, columns = toReturn.columns)
  }

  protected def convertDistinct(
    query: Distinct[Start],
    columnsIn: Columns
  ): Distinct[Start] = {
    val toDedup = convertQuery(query.toDedup, columnsIn)
    query.copy(toDedup = toDedup, columns = toDedup.columns)
  }

  protected def convertUnwind(
    query: Unwind[Start],
    columnsIn: Columns
  ): Unwind[Start] = {
    val unwindFrom = convertQuery(query.unwindFrom, columnsIn + query.as)
    query.copy(columns = unwindFrom.columns)
  }

  protected def convertAdjustContext(
    query: AdjustContext[Start],
    columnsIn: Columns
  ): AdjustContext[Start] = {
    val adjusted = convertQuery(query.adjustThis, columnsIn)
    val oldCols = query.dropExisting match {
      case true => Columns.Specified(Vector.empty)
      case false => adjusted.columns
    }
    val newCols = Columns.Specified(query.toAdd.map(_._1).toVector)
    query.copy(
      adjustThis = adjusted,
      columns = oldCols ++ newCols
    )
  }

  protected def convertEagerAggregation(
    query: EagerAggregation[Start],
    columnsIn: Columns
  ): EagerAggregation[Start] = {
    val toAggregate = convertQuery(query.toAggregate, columnsIn)
    val oldCols = query.keepExisting match {
      case true => toAggregate.columns
      case false => Columns.Specified(Vector.empty)
    }
    val newCols = Columns.Specified(
      (query.aggregateAlong ++ query.aggregateWith).map(_._1).toVector
    )
    query.copy(
      toAggregate = toAggregate,
      columns = oldCols ++ newCols
    )
  }

  protected def convertDelete(
    query: Delete,
    columnsIn: Columns
  ): Delete = query.copy(columns = columnsIn)

  protected def convertProcedureCall(
    query: ProcedureCall,
    columnsIn: Columns
  ): ProcedureCall = {
    val newCols = query.returns match {
      case None => query.procedure.outputColumns
      case Some(remapping) => query.procedure.outputColumns.rename(remapping)
    }
    query.copy(
      columns = columnsIn ++ newCols
    )
  }

  protected def convertSubQuery(
    query: SubQuery[Start],
    columnsIn: Columns
  ): SubQuery[Start] = {
    val subQuery = convertQuery(query.subQuery, columnsIn)
    query.copy(
      subQuery = subQuery,
      columns = columnsIn ++ subQuery.columns
    )
  }
}
