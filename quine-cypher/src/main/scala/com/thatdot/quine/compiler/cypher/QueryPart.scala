package com.thatdot.quine.compiler.cypher

import cats.implicits._
import org.opencypher.v9_0.expressions.{LogicalVariable, functions}
import org.opencypher.v9_0.{ast, expressions, util}

import com.thatdot.quine.graph.cypher
import com.thatdot.quine.graph.cypher.{Location, Query}

object QueryPart {

  /** Compile a `front-end` query
    *
    * @param queryPart query to compiler
    * @param isEntireQuery this query part is the whole query
    * @param isSubQuery is this inside a `CALL { .. }`?
    * @return execution instructions for Quine
    */
  def compile(
    queryPart: ast.QueryPart,
    isEntireQuery: Boolean = true,
    isSubQuery: Boolean = false
  ): CompM[cypher.Query[cypher.Location.Anywhere]] =
    queryPart match {
      case sq: ast.SingleQuery =>
        if (!isSubQuery) {
          compileClauses(sq.clauses, isEntireQuery)
        } else {
          for {
            // Prepare for the subquery to run by setting the imported columns
            initialColumns: Vector[Symbol] <- CompM.getColumns
            importedVariables =
              if (sq.isCorrelated) {
                sq.importColumns.view.map(Symbol.apply).toVector
              } else {
                initialColumns
              }
            () <- CompM.clearColumns
            () <- importedVariables.traverse_(CompM.addColumn)

            // Compile the subquery
            subQuery <- compileClauses(sq.clausesExceptImportWith, isEntireQuery)

            // Update the columns by appending back all of the initial columns
            () <- initialColumns.traverse_(CompM.addColumn)
          } yield cypher.Query.SubQuery(subQuery, importedVariables)
        }

      case union: ast.ProjectingUnion =>
        for {
          identityMapping: Vector[(Symbol, cypher.Expr)] <- CompM.getColumns
            .flatMap(_.traverse((col: Symbol) => CompM.getVariable(col, union).map(col -> _)))
          compiledPart <- CompM.withIsolatedContext {
            for {
              p <- compile(union.part, false, isSubQuery)
              mapping <- compileUnionMapping(isPart = true, union.unionMappings, union.part)
            } yield cypher.Query.adjustContext(true, mapping ++ identityMapping, p)
          }
          compiledSingle <- CompM.withIsolatedContext {
            for {
              q <- compile(union.query, false, isSubQuery)
              mapping <- compileUnionMapping(isPart = false, union.unionMappings, union.query)
            } yield cypher.Query.adjustContext(true, mapping ++ identityMapping, q)
          }
          () <- union.unionMappings.traverse_(u => CompM.addColumn(u.unionVariable))
          unioned = cypher.Query.Union(compiledPart, compiledSingle)

          projectedUnion <-
            if (union.isInstanceOf[ast.ProjectingUnionDistinct]) {
              // "Distinct" with respect to all of the columns returned
              queryPart.returnColumns
                .traverse(CompM.getVariable(_, queryPart))
                .map(distinctBy => cypher.Query.Distinct(distinctBy, unioned))
            } else {
              CompM.pure(unioned)
            }
        } yield projectedUnion

      case u: ast.UnmappedUnion =>
        CompM.raiseCompileError("Unmapped unions should have been transformed into projecting unions", u)
    }

  /** Compile a union mapping into the new column mapping (as can be passed to `AdjustContext`)
    *
    * @param isPart do we want the mapping for the LHS part (if not, it is for the RHS query)
    * @param unionMappings mappings of variables
    * @param astNode
    * @return variable mapping
    */
  private def compileUnionMapping(
    isPart: Boolean,
    unionMappings: List[ast.Union.UnionMapping],
    astNode: util.ASTNode
  ): CompM[Vector[(Symbol, cypher.Expr)]] = {
    def getInVariable(v: ast.Union.UnionMapping): expressions.LogicalVariable =
      if (isPart) v.variableInPart else v.variableInQuery
    unionMappings.toVector
      .traverse { (mapping: ast.Union.UnionMapping) =>
        CompM
          .getVariable(getInVariable(mapping), astNode)
          .map(e => (logicalVariable2Symbol(mapping.unionVariable)) -> e)
      }
  }

  private def compileMatchClause(
    matchClause: ast.Match
  ): CompM[cypher.Query[cypher.Location.Anywhere]] = {
    // TODO: use `hints`
    val ast.Match(isOptional, pattern, hints @ _, whereOpt) = matchClause

    val matchCompiled = whereOpt match {
      case None =>
        for {
          graph <- Graph.fromPattern(pattern)
          query <- graph.synthesizeFetch(WithFreeVariables.empty)
        } yield query
      case Some(ast.Where(expr)) =>
        for {
          // Seperate `WHERE` into ID constraints and everything else
          (anchoredIds, other) <- CompM.getContextParametersAndSource.map { ctx =>
            partitionWhereConstraints(expr)(ctx._1, ctx._2, ctx._3)
          }
          _ <- CompM.addNewAnchors(anchoredIds)
          cols <- CompM.getColumns.map(_.toSet)
          (filters, constraints) = WithFreeVariables[
            expressions.LogicalVariable,
            expressions.Expression
          ](
            other.toList,
            (lv: expressions.LogicalVariable) => cols.contains(logicalVariable2Symbol(lv)),
            (exp: expressions.Expression) => exp.dependencies
          )

          // Filter expressions that can be applied before the match even runs :O
          beforeFilter: WithQuery[cypher.Expr] <- filters
            .traverse[WithQueryT[CompM, *], cypher.Expr](Expression.compile(_))
            .map(constraints => cypher.Expr.And(constraints.toVector))
            .runWithQuery

          graph <- Graph.fromPattern(pattern)
          fetchPattern <- graph.synthesizeFetch(constraints)
          _ <- CompM.clearAnchors
        } yield beforeFilter.toQuery(cypher.Query.filter(_, fetchPattern))
    }

    if (isOptional) {
      matchCompiled.map(cypher.Query.Optional(_))
    } else {
      matchCompiled
    }
  }

  private def compileLoadCSV(l: ast.LoadCSV): CompM[cypher.Query[cypher.Location.Anywhere]] = {
    val ast.LoadCSV(withHeaders, urlString, variable, fieldTerm) = l
    val fieldTermChar: Char = fieldTerm match {
      case None => ','
      case Some(charLit) => charLit.value.head // string literal is always one character long here
    }

    for {
      urlWc <- Expression.compileM(urlString)
      varExpr <- CompM.addColumn(variable)
    } yield urlWc.toQuery { (url: cypher.Expr) =>
      cypher.Query.LoadCSV(withHeaders, url, varExpr.id, fieldTermChar)
    }
  }

  private def compileSetClause(
    setClause: ast.SetClause
  ): CompM[cypher.Query[cypher.Location.Anywhere]] =
    setClause.items.toVector
      .traverse[CompM, cypher.Query[cypher.Location.Anywhere]] {
        case ast.SetPropertyItem(prop, expression) =>
          for {
            nodeWC <- Expression.compileM(prop.map)
            valueWC <- Expression.compileM(expression)
          } yield nodeWC.toQuery { (nodeExpr: cypher.Expr) =>
            cypher.Query.ArgumentEntry(
              node = nodeExpr,
              andThen = valueWC.toNodeQuery { (value: cypher.Expr) =>
                cypher.Query.SetProperty(
                  key = Symbol(prop.propertyKey.name),
                  newValue = Some(value)
                )
              }
            )
          }

        case ast.SetExactPropertiesFromMapItem(variable, expression) =>
          for {
            nodeWC <- Expression.compileM(variable)
            propsWC <- Expression.compileM(expression)
          } yield nodeWC.toQuery { (nodeExpr: cypher.Expr) =>
            cypher.Query.ArgumentEntry(
              node = nodeExpr,
              andThen = propsWC.toNodeQuery { (props: cypher.Expr) =>
                cypher.Query.SetProperties(
                  properties = props,
                  includeExisting = false
                )
              }
            )
          }

        case ast.SetIncludingPropertiesFromMapItem(variable, expression) =>
          for {
            nodeWC <- Expression.compileM(variable)
            propsWC <- Expression.compileM(expression)
          } yield nodeWC.toQuery { (nodeExpr: cypher.Expr) =>
            cypher.Query.ArgumentEntry(
              node = nodeExpr,
              andThen = propsWC.toNodeQuery { (props: cypher.Expr) =>
                cypher.Query.SetProperties(
                  properties = props,
                  includeExisting = true
                )
              }
            )
          }

        case ast.SetLabelItem(variable, labels) =>
          for {
            nodeWC <- Expression.compileM(variable)
          } yield nodeWC.toQuery { (nodeExpr: cypher.Expr) =>
            cypher.Query.ArgumentEntry(
              node = nodeExpr,
              andThen = cypher.Query.SetLabels(
                labels.map(lbl => Symbol(lbl.name)),
                add = true
              )
            )
          }
      }
      .map(
        _.foldLeft[cypher.Query[cypher.Location.Anywhere]](cypher.Query.Unit()) { (queryAcc, setQuery) =>
          cypher.Query.apply(queryAcc, cypher.Query.Optional(setQuery))
        }
      )

  private def compileRemoveClause(
    removeClause: ast.Remove
  ): CompM[cypher.Query[cypher.Location.Anywhere]] =
    removeClause.items.toVector
      .traverse[CompM, cypher.Query[cypher.Location.Anywhere]] {
        case ast.RemovePropertyItem(prop) =>
          Expression
            .compileM(prop.map)
            .map(_.toQuery { (nodeExpr: cypher.Expr) =>
              cypher.Query.ArgumentEntry(
                node = nodeExpr,
                andThen = cypher.Query.SetProperty(
                  key = Symbol(prop.propertyKey.name),
                  newValue = None
                )
              )
            })

        case ast.RemoveLabelItem(variable, labels) =>
          Expression
            .compileM(variable)
            .map(_.toQuery { (nodeExpr: cypher.Expr) =>
              cypher.Query.ArgumentEntry(
                node = nodeExpr,
                andThen = cypher.Query.SetLabels(
                  labels.map(lbl => Symbol(lbl.name)),
                  add = false
                )
              )
            })
      }
      .map(
        _.foldLeft[cypher.Query[cypher.Location.Anywhere]](cypher.Query.Unit()) { (queryAcc, remQuery) =>
          cypher.Query.apply(queryAcc, cypher.Query.Optional(remQuery))
        }
      )

  // TODO: this won't delete paths (and it should)
  private def compileDeleteClause(
    deleteClause: ast.Delete
  ): CompM[cypher.Query[cypher.Location.Anywhere]] = {
    val ast.Delete(exprs, forced) = deleteClause
    exprs.toVector
      .traverse[CompM, cypher.Query[cypher.Location.Anywhere]] { expr =>
        Expression
          .compileM(expr)
          .map(_.toQuery { (targetExpr: cypher.Expr) =>
            cypher.Query.Delete(targetExpr, detach = forced)
          })
      }
      .map(
        _.foldLeft[cypher.Query[cypher.Location.Anywhere]](cypher.Query.Unit()) { (queryAcc, delQuery) =>
          cypher.Query.apply(queryAcc, cypher.Query.Optional(delQuery))
        }
      )
  }

  private def compileCreateClause(
    createClause: ast.Create
  ): CompM[cypher.Query[cypher.Location.Anywhere]] =
    Graph.fromPattern(createClause.pattern).flatMap(_.synthesizeCreate)

  private def compileMergeClause(
    mergeClause: ast.Merge
  ): CompM[cypher.Query[cypher.Location.Anywhere]] = {
    // TODO: is a non-empty where clause here ever possible?
    val ast.Merge(pattern, mergeAction, whereCls @ _) = mergeClause

    // Match and then run all the "on match" clauses
    def tryFirst(graph: Graph) = for {
      findQuery <- graph.synthesizeFetch(WithFreeVariables.empty)
      matchActionsQuery <- mergeAction.view
        .collect { case ast.OnMatch(c) => c }
        .toVector
        .traverse[CompM, cypher.Query[cypher.Location.Anywhere]](compileSetClause(_))
        .map {
          _.foldRight[cypher.Query[cypher.Location.Anywhere]](cypher.Query.Unit())(
            cypher.Query.apply(_, _)
          )
        }
    } yield cypher.Query.apply(findQuery, matchActionsQuery)

    // Create and then fun all the "on create" clauses
    def trySecond(graph: Graph) = for {
      createQuery <- graph.synthesizeCreate
      createActionsQuery <- mergeAction.view
        .collect { case ast.OnCreate(c) => c }
        .toVector
        .traverse[CompM, cypher.Query[cypher.Location.Anywhere]](compileSetClause(_))
        .map {
          _.foldRight[cypher.Query[cypher.Location.Anywhere]](cypher.Query.Unit())(
            cypher.Query.apply(_, _)
          )
        }
    } yield cypher.Query.apply(createQuery, createActionsQuery)

    /* The way `Or` works, the `trySecond` argument only ever gets run if the
     * first returned nothing. This is exactly the behaviour we need for `MATCH`
     * or else `CREATE` if `MATCH` found nothing
     */
    for {
      graph <- Graph.fromPattern(pattern)
      tryFirstQuery <- CompM.withIsolatedContext(tryFirst(graph))
      trySecondQuery <- trySecond(graph)
    } yield cypher.Query.Or(tryFirstQuery, trySecondQuery)
  }

  private def compileUnwind(
    unwindClause: ast.Unwind
  ): CompM[cypher.Query[cypher.Location.Anywhere]] = {
    val ast.Unwind(expr, asVar) = unwindClause
    for {
      listWc <- Expression.compileM(expr)
      asVarExpr <- CompM.addColumn(asVar)
    } yield listWc.toQuery { (list: cypher.Expr) =>
      cypher.Query.Unwind(list, asVarExpr.id, cypher.Query.Unit())
    }
  }

  private def compileForeach(
    foreachClause: ast.Foreach
  ): CompM[cypher.Query[cypher.Location.Anywhere]] = {
    // TODO: can we get away with this?
    val ast.Foreach(asVar, expr, updates) = foreachClause
    for {
      listExpr <- Expression.compileM(expr)
      (asVarExpr, foreachBody) <- CompM.withIsolatedContext {
        for {
          asVarExpr <- CompM.addColumn(asVar)
          foreachBody <- compileClauses(updates, false)
        } yield (asVarExpr, foreachBody)
      }
    } yield listExpr.toQuery { (list: cypher.Expr) =>
      cypher.Query.EagerAggregation(
        aggregateAlong = Vector.empty,
        aggregateWith = Vector.empty,
        toAggregate = cypher.Query.Unwind(list, asVarExpr.id, foreachBody),
        keepExisting = true
      )
    }
  }

  /** Compile a potentially-aggregating projection.
    *
    * NB because WHERE and ORDER BY can use both agregated and non-aggregated values, if aggregation is present,
    * the ORDER BY clause must be compiled alongside the aggregation.
    *
    * @param projection
    */
  private def compileSortFilterAndAggregate(
    querySoFar: Query[cypher.Location.Anywhere],
    returnItems: ast.ReturnItems,
    orderByOpt: Option[ast.OrderBy],
    whereOpt: Option[ast.Where]
  ): CompM[Query[Location.Anywhere]] = {
    for {
      compiledReturnItems <- compileReturnItems(returnItems)
      WithQuery((groupers, aggregators), setupQuery) = compiledReturnItems
      grouped <-
        if (aggregators.isEmpty) {
          for {
            // RETURN
            _ <- groupers.traverse_ { (col: (Symbol, cypher.Expr)) =>
              CompM.hasColumn(col._1).flatMap {
                case true => CompM.pure(())
                case false => CompM.addColumn(col._1).map(_ => ())
              }
            }
            adjusted = cypher.Query.adjustContext(
              dropExisting = false,
              groupers,
              cypher.Query.apply(querySoFar, setupQuery)
            )

            // WHERE
            filtered: cypher.Query[cypher.Location.Anywhere] <- whereOpt match {
              case None => CompM.pure(adjusted)
              case Some(ast.Where(expr)) =>
                Expression.compileM(expr).map(_.toQuery(cypher.Query.filter(_, adjusted)))
            }

            // ORDER BY
            ordered: cypher.Query[cypher.Location.Anywhere] <- orderByOpt match {
              case None => CompM.pure(filtered)
              case Some(ast.OrderBy(sortItems)) =>
                sortItems.toVector
                  .traverse[CompM, WithQuery[(cypher.Expr, Boolean)]] {
                    case ast.AscSortItem(e) => Expression.compileM(e).map(_.map(_ -> true))
                    case ast.DescSortItem(e) => Expression.compileM(e).map(_.map(_ -> false))
                  }
                  .map(_.sequence)
                  .map { case WithQuery(sortBy, setupSort) =>
                    cypher.Query.Sort(sortBy, cypher.Query.apply(filtered, setupSort))
                  }
            }

            // We need to adjust the context both before
            // and after the context because ORDER BY might be using one of the
            // newly created variables, or it might be using one of the newly
            // deleted variables.
            toReturn: cypher.Query[cypher.Location.Anywhere] <-
              if (!returnItems.includeExisting) {
                for {
                  // values for `groupers` in original context
                  boundGroupers <- groupers.traverse { case (colName, _) =>
                    CompM.getVariable(colName, returnItems).map(colName -> _)
                  }
                  // allocate a new set of columns to hold the `groupers` values
                  () <- CompM.clearColumns
                  () <- groupers.traverse_ { case (colName, _) => CompM.addColumn(colName) }
                } yield cypher.Query.adjustContext(
                  dropExisting = true,
                  toAdd = boundGroupers,
                  adjustThis = ordered
                )
              } else {
                CompM.pure(ordered)
              }
          } yield toReturn
        } else {

          for {
            // Aggregate columns
            () <- CompM.clearColumns
            totalCols: Vector[(Symbol, cypher.Expr.Variable)] <- returnItems.items.toVector
              .traverse { (retItem: ast.ReturnItem) =>
                val colName = Symbol(retItem.name)
                CompM.addColumn(colName).map(colName -> _)
              }

            aggregated = cypher.Query.adjustContext(
              dropExisting = true,
              toAdd = totalCols,
              adjustThis = cypher.Query.EagerAggregation(
                groupers,
                aggregators,
                cypher.Query.apply(querySoFar, setupQuery),
                keepExisting = false
              )
            )

            // Where
            filtered: cypher.Query[cypher.Location.Anywhere] <- whereOpt match {
              case None => CompM.pure(aggregated)
              case Some(ast.Where(expr)) =>
                Expression.compileM(expr).map(_.toQuery(cypher.Query.filter(_, aggregated)))
            }

            // ORDER BY
            ordered: cypher.Query[cypher.Location.Anywhere] <- orderByOpt match {
              case None => CompM.pure(filtered)
              case Some(ast.OrderBy(sortItems)) =>
                sortItems.toVector
                  .traverse[CompM, WithQuery[(cypher.Expr, Boolean)]] {
                    case ast.AscSortItem(e) => Expression.compileM(e).map(_.map(_ -> true))
                    case ast.DescSortItem(e) => Expression.compileM(e).map(_.map(_ -> false))
                  }
                  .map(_.sequence.toQuery(cypher.Query.Sort(_, filtered)))
            }
          } yield ordered
        }
    } yield grouped
  }

  /** Compile return items into non-aggregates and aggregates
    *
    * @param items return items
    * @return items by which to group and aggregations for these groups
    */
  private def compileReturnItems(
    items: ast.ReturnItems
  ): CompM[WithQuery[(Vector[(Symbol, cypher.Expr)], Vector[(Symbol, cypher.Aggregator)])]] =
    items.items.toVector
      .traverse[WithQueryT[CompM, *], Either[(Symbol, cypher.Expr), (Symbol, cypher.Aggregator)]] {
        (ret: ast.ReturnItem) =>
          val retSym = Symbol(ret.name)

          /* Because of the `isolateAggregation` phase, we can rely on aggregate
           * operators being all top-level.
           *
           * TODO: generalize properly instead of hardcoding a handful of constructs
           */
          ret.expression match {
            case expressions.CountStar() =>
              WithQueryT.pure[CompM, Either[(Symbol, cypher.Expr), (Symbol, cypher.Aggregator)]](
                Right(retSym -> cypher.Aggregator.countStar)
              )

            case expressions.IsAggregate(fi: expressions.FunctionInvocation) =>
              fi.function match {
                case expressions.functions.Count =>
                  Expression.compile(fi.args(0)).map { arg =>
                    Right(retSym -> cypher.Aggregator.count(fi.distinct, arg))
                  }
                case expressions.functions.Collect =>
                  Expression.compile(fi.args(0)).map { arg =>
                    Right(retSym -> cypher.Aggregator.collect(fi.distinct, arg))
                  }
                case expressions.functions.Sum =>
                  Expression.compile(fi.args(0)).map { arg =>
                    Right(retSym -> cypher.Aggregator.sum(fi.distinct, arg))
                  }
                case expressions.functions.Avg =>
                  Expression.compile(fi.args(0)).map { arg =>
                    Right(retSym -> cypher.Aggregator.avg(fi.distinct, arg))
                  }
                case expressions.functions.Min =>
                  Expression.compile(fi.args(0)).map { arg =>
                    Right(retSym -> cypher.Aggregator.min(arg))
                  }
                case expressions.functions.Max =>
                  Expression.compile(fi.args(0)).map { arg =>
                    Right(retSym -> cypher.Aggregator.max(arg))
                  }

                case expressions.functions.StdDev =>
                  Expression.compile(fi.args(0)).map { arg =>
                    Right(retSym -> cypher.Aggregator.StDev(arg, partialSampling = true))
                  }

                case expressions.functions.StdDevP =>
                  Expression.compile(fi.args(0)).map { arg =>
                    Right(retSym -> cypher.Aggregator.StDev(arg, partialSampling = false))
                  }

                case expressions.functions.PercentileCont =>
                  for {
                    expr <- Expression.compile(fi.args(0))
                    perc <- Expression.compile(fi.args(1))
                  } yield Right(retSym -> cypher.Aggregator.Percentile(expr, perc, continuous = true))

                case expressions.functions.PercentileDisc =>
                  for {
                    expr <- Expression.compile(fi.args(0))
                    perc <- Expression.compile(fi.args(1))
                  } yield Right(retSym -> cypher.Aggregator.Percentile(expr, perc, continuous = false))

                case func =>
                  WithQueryT.lift(
                    CompM.raiseCompileError(
                      s"Compiler internal error: unknown aggregating function `${func.name}`",
                      fi
                    )
                  )
              }

            case _ =>
              Expression.compile(ret.expression).map { arg =>
                Left(retSym -> arg)
              }
          }
      }
      .map(_.separate)
      .runWithQuery

  /** Compile a series of clauses that occur one after another
    *
    * @param clauses what to compile
    * @param isEntireQuery this query part is the whole query
    * @return a query
    */
  private def compileClauses(
    clauses: Seq[ast.Clause],
    isEntireQuery: Boolean
  ): CompM[cypher.Query[cypher.Location.Anywhere]] = clauses.toVector
    .foldLeftM[CompM, cypher.Query[cypher.Location.Anywhere]](cypher.Query.unit) {
      case (accQuery, m: ast.Match) =>
        compileMatchClause(m).map(cypher.Query.apply(accQuery, _))

      case (accQuery, c: ast.Create) =>
        compileCreateClause(c).map(cypher.Query.apply(accQuery, _))

      case (accQuery, m: ast.Merge) =>
        compileMergeClause(m).map(cypher.Query.apply(accQuery, _))

      case (accQuery, s: ast.SetClause) =>
        compileSetClause(s).map(cypher.Query.apply(accQuery, _))

      case (accQuery, r: ast.Remove) =>
        compileRemoveClause(r).map(cypher.Query.apply(accQuery, _))

      case (accQuery, d: ast.Delete) =>
        compileDeleteClause(d).map(cypher.Query.apply(accQuery, _))

      case (accQuery, l: ast.LoadCSV) =>
        compileLoadCSV(l).map(cypher.Query.apply(accQuery, _))

      case (accQuery, u: ast.Unwind) =>
        compileUnwind(u).map(cypher.Query.apply(accQuery, _))

      case (accQuery, f: ast.Foreach) =>
        compileForeach(f).map(cypher.Query.apply(accQuery, _))

      case (accQuery, ast.SubQuery(part)) =>
        for {
          subQuery <- compile(part, isEntireQuery = false, isSubQuery = true)
        } yield cypher.Query.apply(accQuery, subQuery)

      case (accQuery, QuineProcedureCall(proc, unresolvedCall)) =>
        val callIsWholeQuery = clauses.length == 1 && isEntireQuery
        val (whereOpt, returnsOpt) = unresolvedCall.declaredResult match {
          case Some(ast.ProcedureResult(items, where)) =>
            val returns = items
              .map(p => Symbol(p.outputName) -> Symbol(p.variable.name))
              .toMap

            (where, Some(returns))
          case None => (None, None)
        }

        for {
          /* Figure out what the new columns are (so we can bring them into scope)
           * This part has weird logic about what Cypher allows:
           *
           *   - procedures that return no columns (aka, `VOID` procedures)
           *     always omit the yield
           *   - if the procedure call is the entire query, omitting `YIELD`
           *     implicitly returns all of the procedures output columns
           *   - otherwise, the `YIELD` is mandatory (even if you don't care
           *     about the output)
           */
          outputColumns: Vector[Symbol] <- returnsOpt match {
            case Some(cols) =>
              val outputs = proc.signature.outputs.view.map(_._1).toSet
              val invalidYields = cols.keys.view.map(_.name).filter(!outputs.contains(_)).toVector
              if (invalidYields.isEmpty) {
                CompM.pure(cols.values.toVector)
              } else {
                CompM.raiseCompileError(
                  s"Procedure does not have output(s): ${invalidYields.mkString(",")}",
                  unresolvedCall
                )
              }
            case None if callIsWholeQuery || proc.outputColumns.variables.isEmpty =>
              CompM.pure(proc.outputColumns.variables)
            case None =>
              CompM.raiseCompileError(
                "Procedure call inside a query does not support naming results implicitly (name explicitly using `YIELD` instead)",
                unresolvedCall
              )
          }

          /* Compute the arguments. Cypher supports specifying no argument list
           * at all if the call is the entire query, in which case it goes to
           * the parameters to find the arguments
           */
          args: WithQuery[Vector[cypher.Expr]] <- unresolvedCall.declaredArguments match {
            case Some(args) =>
              args.toVector
                .traverse(Expression.compileM(_))
                .map(_.sequence)

            case None if callIsWholeQuery =>
              proc.signature.arguments.view
                .map(_._1)
                .toVector
                .traverse(CompM.getParameter(_, unresolvedCall))
                .map(WithQuery(_))

            case None =>
              CompM.raiseCompileError(
                "Procedure call inside a query does not support passing arguments implicitly (pass explicitly after procedure name instead)",
                unresolvedCall
              )
          }

          _ <- outputColumns.traverse_(CompM.addColumn)
          callQuery = cypher.Query.apply(
            accQuery,
            args.toQuery { (args: Seq[cypher.Expr]) =>
              val udp = cypher.Proc.UserDefined(proc.name)
              cypher.Query.ProcedureCall(udp, args, returnsOpt)
            }
          )

          // WHERE
          filteredCall: cypher.Query[cypher.Location.Anywhere] <- whereOpt match {
            case None => CompM.pure(callQuery)
            case Some(ast.Where(expr)) =>
              // TODO: SemiApply for path predicates in top-level conjunction
              Expression.compileM(expr).map(_.toQuery(cypher.Query.filter(_, callQuery)))
          }
        } yield filteredCall

      // These are now errors
      case (_, uc: ast.UnresolvedCall) =>
        val ucName = (uc.procedureNamespace.parts :+ uc.procedureName.name).mkString(".")
        CompM.raiseCompileError(
          message = s"Failed to resolve procedure `$ucName`",
          astNode = uc.procedureName
        )

      // TODO: Return items have `excludedNames: Set[String]` and I'm not sure what that is
      case (
            accQuery,
            clause @ ast.ProjectionClause(
              isDistinct,
              items,
              orderByOpt,
              skipOpt,
              limitOpt,
              whereOpt
            )
          ) if !clause.isReturn =>
        for {
          // Handle aggregations, ORDER BY, and grouping, if any
          grouped: Query[Location.Anywhere] <- compileSortFilterAndAggregate(accQuery, items, orderByOpt, whereOpt)

          // DISTINCT
          deduped <- isDistinct match {
            case false => CompM.pure[cypher.Query[cypher.Location.Anywhere]](grouped)
            case true =>
              clause.returnColumns
                .traverse(CompM.getVariable(_, clause))
                .map(distinctBy => cypher.Query.Distinct(distinctBy, grouped))
          }

          // SKIP
          skipped <- skipOpt match {
            case None => CompM.pure[cypher.Query[cypher.Location.Anywhere]](deduped)
            case Some(ast.Skip(expr)) =>
              Expression.compileM(expr).map(_.toQuery(cypher.Query.Skip(_, deduped)))
          }

          // LIMIT
          limited <- limitOpt match {
            case None => CompM.pure[cypher.Query[cypher.Location.Anywhere]](skipped)
            case Some(ast.Limit(expr)) =>
              Expression.compileM(expr).map(_.toQuery(cypher.Query.Limit(_, skipped)))
          }
        } yield limited
      case (
            accQuery,
            clause @ ast.Return(
              isDistinct,
              items,
              orderByOpt,
              skipOpt,
              limitOpt,
              excludedNames @ _
            )
          ) =>
        compileReturnItems(items).flatMap {
          case WithQuery((groupers, aggregators), setupQuery) if aggregators.isEmpty =>
            /** non-aggregating RETURN: We can compile directly to a single fused [[cypher.Query.Return]]
              */
            for {
              // add directly-returned columns (ie `groupers`) to context
              _ <- groupers.traverse_ { (col: (Symbol, cypher.Expr)) =>
                CompM.hasColumn(col._1).flatMap {
                  case true => CompM.pure(())
                  case false => CompM.addColumn(col._1).map(_ => ())
                }
              }
              adjusted = cypher.Query.adjustContext(
                dropExisting = false,
                groupers,
                cypher.Query.apply(accQuery, setupQuery)
              )
              // compile the ORDER BY rule and any query necessary to set up an environment to run the sorting
              orderedWQ: WithQuery[Option[cypher.Query.Sort.SortBy]] <- orderByOpt match {
                case None => CompM.pure(WithQuery(None))
                case Some(ast.OrderBy(sortItems)) =>
                  sortItems.toVector
                    .traverse[WithQueryT[CompM, *], (cypher.Expr, Boolean)] {
                      case ast.AscSortItem(e) => Expression.compile(e).map(_ -> true)
                      case ast.DescSortItem(e) => Expression.compile(e).map(_ -> false)
                    }
                    .map(Some(_))
                    .runWithQuery
              }
              WithQuery(orderingRule, orderingQueryPart) = orderedWQ
              // compile the DISTINCT rule
              dedupeRule: Option[cypher.Query.Distinct.DistinctBy] <- isDistinct match {
                case false => CompM.pure(None)
                case true =>
                  // NB because interpreting variables is independent of graph state, this doesn't need a WithQuery closure
                  clause.returnColumns
                    .traverse(CompM.getVariable(_, clause))
                    .map(Some(_))
              }
              // compile the SKIP rule and any query necessary to set up an environment to run the rule
              dropWQ: WithQuery[Option[cypher.Query.Skip.Drop]] <- skipOpt match {
                case None => CompM.pure(WithQuery(None))
                case Some(ast.Skip(expr)) =>
                  Expression.compile(expr).map(Some(_)).runWithQuery
              }
              WithQuery(dropRule, dropQueryPart) = dropWQ
              // compile the LIMIT rule and any query necessary to set up an environment to run the rule
              limitWQ: WithQuery[Option[cypher.Query.Limit.Take]] <- limitOpt match {
                case None => CompM.pure(WithQuery(None))
                case Some(ast.Limit(expr)) =>
                  Expression.compile(expr).map(Some(_)).runWithQuery
              }
              WithQuery(takeRule, takeQueryPart) = limitWQ
              // unprojected query (plus setup for ordering and (implicitly) deduplication)
              unprojectedQuery = Query.apply(adjusted, orderingQueryPart)
              // ORDER BY can use values from the main query, so we need to ensure that clause's related query is
              // fully interpreted before the `RETURN` evaluates the ORDER BY clause
              returnQueryWithDedupe = Query.Return(
                toReturn = unprojectedQuery,
                orderBy = orderingRule,
                distinctBy = dedupeRule,
                drop = dropRule,
                take = takeRule
              )
              // We need to adjust the context both before
              // and after the context because ORDER BY might be using one of the
              // newly created variables, or it might be using one of the newly
              // deleted variables.
              returnQueryWithDedupeAndOrdering: cypher.Query[cypher.Location.Anywhere] <-
                if (!items.includeExisting) {
                  for {
                    // values for `groupers` in original context
                    boundGroupers <- groupers.traverse { case (colName, _) =>
                      CompM.getVariable(colName, items).map(colName -> _)
                    }
                    // allocate a new set of columns to hold the `groupers` values
                    () <- CompM.clearColumns
                    () <- groupers.traverse_ { case (colName, _) => CompM.addColumn(colName) }
                  } yield cypher.Query.adjustContext(
                    dropExisting = true,
                    toAdd = boundGroupers,
                    adjustThis = returnQueryWithDedupe
                  )
                } else {
                  CompM.pure(returnQueryWithDedupe)
                }
              // DROP/SKIP Exprs need to be evaluated before the query they are windowing, so the related queries for
              // those clauses need to be fully interpreted before the RETURN evaluates its main query
              returnQueryWithDrop = Query.apply(returnQueryWithDedupeAndOrdering, dropQueryPart)
              returnQueryWithTake = Query.apply(returnQueryWithDrop, takeQueryPart)
            } yield returnQueryWithTake
          case _ =>
            /** aggregating RETURN: We need to compile the aggregation (and therefore the [[orderByOpt]]) separately,
              * but we can still fuse the LIMIT/SKIP/DISTINCT to leverage some optimizations
              */
            for {
              // Handle aggregations, ORDER BY, and grouping, if any
              grouped: Query[Location.Anywhere] <- compileSortFilterAndAggregate(
                accQuery,
                items,
                orderByOpt,
                whereOpt = None
              )
              dedupeRule: Option[cypher.Query.Distinct.DistinctBy] <- isDistinct match {
                case false => CompM.pure(None)
                case true =>
                  // NB because interpreting variables is independent of graph state, this doesn't need a WithQuery closure
                  clause.returnColumns
                    .traverse(CompM.getVariable(_, clause))
                    .map(Some(_))
              }
              // compile the SKIP rule and any query necessary to set up an environment to run the rule
              dropWQ: WithQuery[Option[cypher.Query.Skip.Drop]] <- skipOpt match {
                case None => CompM.pure(WithQuery(None))
                case Some(ast.Skip(expr)) =>
                  Expression.compile(expr).map(Some(_)).runWithQuery
              }
              WithQuery(dropRule, dropQueryPart) = dropWQ
              // compile the LIMIT rule and any query necessary to set up an environment to run the rule
              limitWQ: WithQuery[Option[cypher.Query.Limit.Take]] <- limitOpt match {
                case None => CompM.pure(WithQuery(None))
                case Some(ast.Limit(expr)) =>
                  Expression.compile(expr).map(Some(_)).runWithQuery
              }
              WithQuery(takeRule, takeQueryPart) = limitWQ
              returnQueryWithDedupe = Query.Return(
                toReturn = grouped,
                orderBy = None, // `grouped` is already ordered
                distinctBy = dedupeRule,
                drop = dropRule,
                take = takeRule
              )
              returnQueryWithDrop = Query.apply(returnQueryWithDedupe, dropQueryPart)
              returnQueryWithTake = Query.apply(returnQueryWithDrop, takeQueryPart)
            } yield returnQueryWithTake
        }

      // TODO: what can go here?
      case (_, other) =>
        CompM.raiseCompileError(s"Compiler internal error: unknown clause type", other)
    }
    .map { (query: cypher.Query[cypher.Location.Anywhere]) =>
      /** Determine which output context to use. Usually, this will just be the output columns of the query, but
        * some queries return no rows when used as a top-level query, yet return something when used as part of
        * another query. For example:
        *
        * The `SET` subqueries of `MATCH (n) SET n:Node SET n.kind = 'node' RETURN n`.
        *   - The first SET clause should run once for each `n` -- so the MATCH should return one row per valid `n`
        *   - The second SET clause should run once for each `n` -- so the first SET should return one row per valid `n`
        * This establishes that a SET clause should return one row per invocation. However, a query like the following
        * should return no rows: `MATCH (n) SET n:Node SET n.kind = 'node'`
        * To account for the different behavior of SET when used "inside" a query versus SET when used "at the end of"
        * a query, we wrap any final-clause-SET with a `cypher.Query.Empty()`, so the overall query returns no rows,
        * as expected.
        *
        * The same trick applies to other clauses, such as VOID procedures.
        *
        * TODO: handle unions and subqueries of these special cases
        * NB: Neo4j Console throws a NPE on a union of VOID procedures, so it's unlikely users will try such a thing
        */
      clauses.lastOption match {
        // When the final clause is a CREATE/SET/DELETE/etc, return no rows
        case Some(_: ast.UpdateClause) =>
          cypher.Query.adjustContext(
            dropExisting = true,
            Vector.empty,
            cypher.Query.apply(query, cypher.Query.Empty())
          )
        // When the final clause is a CALL clause on a VOID procedure, return no rows
        case Some(cc: QuineProcedureCall) if cc.resolvedProcedure.signature.outputs.isEmpty =>
          cypher.Query.adjustContext(
            dropExisting = true,
            Vector.empty,
            cypher.Query.apply(query, cypher.Query.Empty())
          )
        case _ => query
      }
    }

  /** Split a predicate expression into node ID constraints (ie. `id(n) = 1`)
    * and other filter constraints (ie. `n.name = "Bob"`).
    *
    * TODO: this should return a `Map[expressions.LogicalVariable, List[cypher.Expr]]`
    * TODO: this should also lift out constraints in the top-level conjunct (so that we can integrate them in the `Graph`)
    * TODO: track which side of the equation still has free-variables
    *
    * @param whereExpr predicate expression (from a `WHERE` clause)
    * @return node ID constraints, other filters
    */
  private def partitionWhereConstraints(
    whereExpr: expressions.Expression
  )(implicit
    scopeInfo: QueryScopeInfo,
    paramIdx: ParametersIndex,
    source: cypher.SourceText
  ): (Map[Symbol, cypher.Expr], Vector[expressions.Expression]) = {

    val constraints = Map.newBuilder[Symbol, cypher.Expr]
    val conjuncts = Vector.newBuilder[expressions.Expression]

    /* Add to constraints only if `expr` compiles side-effect free
     *
     * @param v name of the variable for which we may have a constraint
     * @param arg possible constraint expression
     * @param fullExpr the whole predicate
     */
    def visitPossibleConstraint(
      v: LogicalVariable,
      arg: expressions.Expression,
      fullExpr: expressions.Expression
    ): Unit =
      Expression.compileM(arg).run(paramIdx, source, scopeInfo) match {
        case Right(WithQuery(expr, cypher.Query.Unit(_))) => constraints += (logicalVariable2Symbol(v) -> expr)
        case _ => conjuncts += fullExpr
      }

    // `IN` variants matter because openCypher sometimes rewrites `=` to these
    object EqualLike {
      def unapply(e: expressions.Expression) =
        e match {
          case expressions.Equals(lhs, rhs) => Some((lhs, rhs))
          case expressions.In(lhs, expressions.ListLiteral(List(rhs))) => Some((lhs, rhs))
          case expressions.In(expressions.ListLiteral(List(lhs)), rhs) => Some((lhs, rhs))
          case _ => None
        }
    }

    // Collect all constraints and other filters
    def visit(e: expressions.Expression): Unit = e match {
      case expressions.And(lhs, rhs) =>
        visit(lhs)
        visit(rhs)
      case expressions.Ands(conjs) =>
        conjs.foreach(visit)

      case EqualLike(IdFunc(variable), arg) =>
        visitPossibleConstraint(variable, arg, e)
      case EqualLike(arg, IdFunc(variable)) =>
        visitPossibleConstraint(variable, arg, e)

      case other => conjuncts += other
    }
    visit(whereExpr)

    (constraints.result(), conjuncts.result())
  }

  // Match expressions that look like `id(n)` or `strId(n)`
  object IdFunc {
    def unapply(expr: expressions.Expression): Option[expressions.LogicalVariable] = expr match {
      case fi @ expressions.FunctionInvocation(
            _,
            _,
            _,
            Vector(variable: expressions.LogicalVariable)
          ) if fi.function == functions.Id =>
        Some(variable)

      // TODO: decide on a principled approach to this
      case expressions.FunctionInvocation(
            _,
            expressions.FunctionName("strId"),
            false,
            Vector(variable: expressions.LogicalVariable)
          ) =>
        Some(variable)

      case _ => None

    }
  }
}
