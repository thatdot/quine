package com.thatdot.quine.compiler.cypher

import cats.implicits._
import org.opencypher.v9_0.expressions.functions
import org.opencypher.v9_0.{ast, expressions, util}

import com.thatdot.quine.graph.cypher

object QueryPart {

  /** Compile a `front-end` query
    *
    * @param queryPart query to compiler
    * @param isEntireQuery this query part is the whole query
    * @return execution instructions for Quine
    */
  def compile(
    queryPart: ast.QueryPart,
    isEntireQuery: Boolean = true
  ): CompM[cypher.Query[cypher.Location.Anywhere]] =
    queryPart match {
      case ast.SingleQuery(clauses) =>
        compileClauses(clauses, isEntireQuery)

      case union: ast.ProjectingUnion =>
        for {
          compiledPart <- CompM.withIsolatedContext {
            for {
              p <- compile(union.part, false)
              mapping <- compileUnionMapping(isPart = true, union.unionMappings, union.part)
            } yield cypher.Query.adjustContext(true, mapping, p)
          }
          compiledSingle <- CompM.withIsolatedContext {
            for {
              q <- compileClauses(union.query.clauses, false)
              mapping <- compileUnionMapping(isPart = false, union.unionMappings, union.query)
            } yield cypher.Query.adjustContext(true, mapping, q)
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
          .map(e => (mapping.unionVariable: Symbol) -> e)
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
            (lv: expressions.LogicalVariable) => cols.contains(lv),
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
          (subQuery, subOutput) <- CompM.withIsolatedContext(
            for {
              subQuery <- compile(part, isEntireQuery = false)
              subOutput <- CompM.getColumns
            } yield (subQuery, subOutput)
          )
          () <- subOutput.traverse_(CompM.addColumn)
        } yield cypher.Query.apply(accQuery, cypher.Query.SubQuery(subQuery))

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
          ) =>
        for {
          // Detect aggregation if there is any
          returnItems <- compileReturnItems(items)
          WithQuery((groupers, aggregators), setupQuery) = returnItems
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
                  cypher.Query.apply(accQuery, setupQuery)
                )

                // WHERE
                filtered: cypher.Query[cypher.Location.Anywhere] <- whereOpt match {
                  case None => CompM.pure(adjusted)
                  case Some(ast.Where(expr)) =>
                    Expression.compileM(expr).map(_.toQuery(cypher.Query.filter(_, adjusted)))
                }

                // ORDER BY
                //
                // TODO: combine ORDER BY and LIMIT into TOP
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

                // TODO: make this nice.
                //
                // We need to adjust the context both before
                // and after the context because ORDER BY might be using one of the
                // newly created variables, or it might be using one of the newly
                // deleted variables.
                toReturn: cypher.Query[cypher.Location.Anywhere] <-
                  if (!items.includeExisting) {
                    for {
                      () <- CompM.clearColumns
                      () <- groupers.traverse_ { case (colName, _) => CompM.addColumn(colName) }
                    } yield cypher.Query.adjustContext(
                      dropExisting = true,
                      toAdd = groupers,
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
                totalCols: Vector[(Symbol, cypher.Expr.Variable)] <- items.items.toVector
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
                    cypher.Query.apply(accQuery, setupQuery),
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
                //
                // TODO: combine ORDER BY and LIMIT into TOP
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

          // SKIP
          skipped <- skipOpt match {
            case None => CompM.pure[cypher.Query[cypher.Location.Anywhere]](grouped)
            case Some(ast.Skip(expr)) =>
              Expression.compileM(expr).map(_.toQuery(cypher.Query.Skip(_, grouped)))
          }

          // LIMIT
          limited <- limitOpt match {
            case None => CompM.pure[cypher.Query[cypher.Location.Anywhere]](skipped)
            case Some(ast.Limit(expr)) =>
              Expression.compileM(expr).map(_.toQuery(cypher.Query.Limit(_, skipped)))
          }

          // DISTINCT
          deduped <- isDistinct match {
            case false => CompM.pure[cypher.Query[cypher.Location.Anywhere]](limited)
            case true =>
              clause.returnColumns
                .traverse(CompM.getVariable(_, clause))
                .map(distinctBy => cypher.Query.Distinct(distinctBy, limited))
          }
        } yield deduped

      // TODO: what can go here?
      case (_, other) =>
        CompM.raiseCompileError(s"Compiler internal error: unknown clause type", other)
    }
    .map { (query: cypher.Query[cypher.Location.Anywhere]) =>
      // TODO: this is a hack to make sure a `CREATE`-only query returns no rows
      clauses.lastOption match {
        case Some(_: ast.UpdateClause) =>
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
      v: Symbol,
      arg: expressions.Expression,
      fullExpr: expressions.Expression
    ): Unit =
      Expression.compileM(arg).run(paramIdx, source, scopeInfo) match {
        case Right(WithQuery(expr, cypher.Query.Unit(_))) => constraints += (v -> expr)
        case _ => conjuncts += fullExpr
      }

    // Collect all constraints and other filters
    def visit(e: expressions.Expression): Unit = e match {
      case expressions.And(lhs, rhs) =>
        visit(lhs)
        visit(rhs)
      case expressions.Ands(conjs) =>
        conjs.foreach(visit)

      case expressions.Equals(IdFunc(variable), arg) =>
        visitPossibleConstraint(variable, arg, e)
      case expressions.Equals(arg, IdFunc(variable)) =>
        visitPossibleConstraint(variable, arg, e)
      case expressions.In(IdFunc(variable), expressions.ListLiteral(List(arg))) =>
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
