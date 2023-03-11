package com.thatdot.quine.compiler.cypher

import cats.implicits._
import org.opencypher.v9_0.expressions
import org.opencypher.v9_0.expressions.functions
import org.opencypher.v9_0.frontend.phases.StatementRewriter
import org.opencypher.v9_0.util.StepSequencer.Condition
import org.opencypher.v9_0.util.{NodeNameGenerator, UnNamedNameGenerator}

import com.thatdot.quine.graph.cypher
import com.thatdot.quine.model.EdgeDirection

object Expression {

  /** Monadic version of [[compile]] */
  def compileM(e: expressions.Expression): CompM[WithQuery[cypher.Expr]] =
    compile(e).runWithQuery

  /** Run an action in a new scope.
    *
    * This means that columns/variables defined in the argument are visible for
    * the rest of its execution, but not once `scoped` has finished running.
    *
    * @param wq action to run in a new scope
    */
  private def scoped[A](wq: WithQueryT[CompM, A]): WithQueryT[CompM, A] =
    WithQueryT(CompM.withIsolatedContext(wq.runWithQuery))

  /** Compile an expression into a pure Quine expression
    *
    * The key difference between the input and output here (besides the location
    * in which the types are defined) is that input expressions can still
    * interact with the graph, which output expressions can be evaluated with
    * nothing more than a context of variables.
    *
    *   - [[expressions.GetDegree]] fetches a count of edges for a node
    *   - path patterns allow arbitrary queries in expressions
    *   - `startNode` fetches the entire node on one end of a relationship
    *   - `endNode` fetches the entire node on the other end of a relationship
    *
    * In order bridge this gap, we return both a pure expression and an
    * effectful query that must be run 'before' evaluating the expression.
    *
    * @return the compiled expression and some side-effecting query
    */
  def compile(e: expressions.Expression): WithQueryT[CompM, cypher.Expr] = e match {

    case i: expressions.IntegerLiteral => WithQueryT.pure(cypher.Expr.Integer(i.value))
    case d: expressions.DoubleLiteral => WithQueryT.pure(cypher.Expr.Floating(d.value))
    case s: expressions.StringLiteral => WithQueryT.pure(cypher.Expr.Str(s.value))
    case _: expressions.Null => WithQueryT.pure(cypher.Expr.Null)
    case _: expressions.True => WithQueryT.pure(cypher.Expr.True)
    case _: expressions.False => WithQueryT.pure(cypher.Expr.False)

    case expressions.ListLiteral(exprs) =>
      exprs.toVector
        .traverse[WithQueryT[CompM, *], cypher.Expr](compile)
        .map(xs => cypher.Expr.ListLiteral(xs.toVector))

    case expressions.MapExpression(items) =>
      items.toList
        .traverse[WithQueryT[CompM, *], (String, cypher.Expr)] { case (k, v) =>
          compile(v).map(k.name -> _)
        }
        .map(xs => cypher.Expr.MapLiteral(xs.toMap))

    case expressions.DesugaredMapProjection(variable, items, includeAll) =>
      for {
        keyValues <- items.toList.traverse[WithQueryT[CompM, *], (String, cypher.Expr)] {
          case expressions.LiteralEntry(k, v) => compile(v).map(k.name -> _)
        }
        theMap <- WithQueryT.lift(CompM.getVariable(variable, e))
      } yield cypher.Expr.MapProjection(theMap, keyValues, includeAll)

    case lv: expressions.Variable =>
      WithQueryT(CompM.getVariable(lv, e).map(WithQuery[cypher.Expr](_)))

    case expressions.Parameter(name, _) =>
      WithQueryT(CompM.getParameter(name, e).map(WithQuery[cypher.Expr](_)))

    case expressions.Property(expr, expressions.PropertyKeyName(keyStr)) =>
      for { expr1 <- compile(expr) } yield cypher.Expr.Property(expr1, Symbol(keyStr))

    case expressions.ContainerIndex(expr, idx) =>
      for { expr1 <- compile(expr); idx1 <- compile(idx) } yield cypher.Expr
        .DynamicProperty(expr1, idx1)

    case expressions.ListSlice(expr, start, end) =>
      for {
        expr1 <- compile(expr)
        start1 <- start.traverse[WithQueryT[CompM, *], cypher.Expr](compile)
        end1 <- end.traverse[WithQueryT[CompM, *], cypher.Expr](compile)
      } yield cypher.Expr.ListSlice(expr1, start1, end1)

    /* All of these turn into list comprehensions
     *
     * Also, for reasons that aren't clear to me, this is one place in openCyphe
     * where the `variable` being bound may not have been freshened (it may shadow
     * another variable of the same name). Since the rest of our compilation
     * scoping assumes fresh names, we defensively manually replace the bound
     * variable with a fresh one.
     */
    case expressions.FilterExpression(expressions.FilterScope(variable, predOpt), list) =>
      val freshVar = variable.renameId(UnNamedNameGenerator.name(variable.position.newUniquePos()))
      val freshPredOpt = predOpt.map(_.copyAndReplace(variable).by(freshVar))
      for {
        (varExpr, predicate) <- scoped {
          for {
            varExpr <- WithQueryT.lift(CompM.addColumn(freshVar))
            predicateOpt <- freshPredOpt.traverse[WithQueryT[CompM, *], cypher.Expr](compile)
            predicate = predicateOpt.getOrElse(cypher.Expr.True)
          } yield (varExpr, predicate)
        }
        list1 <- compile(list)
      } yield cypher.Expr.ListComprehension(varExpr.id, list1, predicate, varExpr)
    case expressions.ExtractExpression(expressions.ExtractScope(variable, predOpt, extOpt), list) =>
      val freshVar = variable.renameId(UnNamedNameGenerator.name(variable.position.newUniquePos()))
      val freshPredOpt = predOpt.map(_.copyAndReplace(variable).by(freshVar))
      val freshExtOpt = extOpt.map(_.copyAndReplace(variable).by(freshVar))
      for {
        (varExpr, predicate, extract) <- scoped {
          for {
            varExpr <- WithQueryT.lift(CompM.addColumn(freshVar))
            predicateOpt <- freshPredOpt.traverse[WithQueryT[CompM, *], cypher.Expr](compile)
            predicate = predicateOpt.getOrElse(cypher.Expr.True)
            extractOpt <- freshExtOpt.traverse[WithQueryT[CompM, *], cypher.Expr](compile)
            extract = extractOpt.getOrElse(varExpr)
          } yield (varExpr, predicate, extract)
        }
        list1 <- compile(list)
      } yield cypher.Expr.ListComprehension(varExpr.id, list1, predicate, extract)
    case expressions.ListComprehension(expressions.ExtractScope(variable, predOpt, extOpt), list) =>
      val freshVar = variable.renameId(UnNamedNameGenerator.name(variable.position.newUniquePos()))
      val freshPredOpt = predOpt.map(_.copyAndReplace(variable).by(freshVar))
      val freshExtOpt = extOpt.map(_.copyAndReplace(variable).by(freshVar))
      for {
        (varExpr, predicate, extract) <- scoped {
          for {
            varExpr <- WithQueryT.lift(CompM.addColumn(freshVar))
            predicateOpt <- freshPredOpt.traverse[WithQueryT[CompM, *], cypher.Expr](compile)
            predicate = predicateOpt.getOrElse(cypher.Expr.True)
            extractOpt <- freshExtOpt.traverse[WithQueryT[CompM, *], cypher.Expr](compile)
            extract = extractOpt.getOrElse(varExpr)
          } yield (varExpr, predicate, extract)
        }
        list1 <- compile(list)
      } yield cypher.Expr.ListComprehension(varExpr.id, list1, predicate, extract)

    case expressions.ReduceExpression(expressions.ReduceScope(acc, variable, expr), init, list) =>
      val freshVar = variable.renameId(UnNamedNameGenerator.name(variable.position.newUniquePos()))
      val freshAcc = acc.renameId(UnNamedNameGenerator.name(acc.position.newUniquePos()))
      val freshExpr = expr
        .copyAndReplace(variable)
        .by(freshVar)
        .copyAndReplace(acc)
        .by(freshAcc)
      for {
        init1 <- compile(init)
        list1 <- compile(list)
        (varExpr, accExpr, expr1) <- scoped {
          for {
            varExpr <- WithQueryT.lift(CompM.addColumn(freshVar))
            accExpr <- WithQueryT.lift(CompM.addColumn(freshAcc))
            expr1 <- compile(freshExpr)
          } yield (varExpr, accExpr, expr1)
        }
      } yield cypher.Expr.ReduceList(accExpr.id, init1, varExpr.id, list1, expr1)

    case expressions.AllIterablePredicate(expressions.FilterScope(variable, predOpt), list) =>
      require(predOpt.nonEmpty)
      val freshVar = variable.renameId(UnNamedNameGenerator.name(variable.position.newUniquePos()))
      val freshPredOpt = predOpt.map(_.copyAndReplace(variable).by(freshVar))
      for {
        list1 <- compile(list)
        (varExpr, pred1) <- scoped {
          for {
            varExpr <- WithQueryT.lift(CompM.addColumn(freshVar))
            pred1 <- compile(freshPredOpt.get)
          } yield (varExpr, pred1)
        }
      } yield cypher.Expr.AllInList(varExpr.id, list1, pred1)
    case expressions.AnyIterablePredicate(expressions.FilterScope(variable, predOpt), list) =>
      require(predOpt.nonEmpty)
      val freshVar = variable.renameId(UnNamedNameGenerator.name(variable.position.newUniquePos()))
      val freshPredOpt = predOpt.map(_.copyAndReplace(variable).by(freshVar))
      for {
        list1 <- compile(list)
        (varExpr, pred1) <- scoped {
          for {
            varExpr <- WithQueryT.lift(CompM.addColumn(freshVar))
            pred1 <- compile(freshPredOpt.get)
          } yield (varExpr, pred1)
        }
      } yield cypher.Expr.AnyInList(varExpr.id, list1, pred1)
    case expressions.NoneIterablePredicate(expressions.FilterScope(variable, predOpt), list) =>
      require(predOpt.nonEmpty)
      val freshVar = variable.renameId(UnNamedNameGenerator.name(variable.position.newUniquePos()))
      val freshPredOpt = predOpt.map(_.copyAndReplace(variable).by(freshVar))
      for {
        list1 <- compile(list)
        (varExpr, pred1) <- scoped {
          for {
            varExpr <- WithQueryT.lift(CompM.addColumn(freshVar))
            pred1 <- compile(freshPredOpt.get)
          } yield (varExpr, pred1)
        }
      } yield cypher.Expr.Not(cypher.Expr.AnyInList(varExpr.id, list1, pred1))
    case expressions.SingleIterablePredicate(expressions.FilterScope(variable, predOpt), list) =>
      require(predOpt.nonEmpty)
      val freshVar = variable.renameId(UnNamedNameGenerator.name(variable.position.newUniquePos()))
      val freshPredOpt = predOpt.map(_.copyAndReplace(variable).by(freshVar))
      for {
        list1 <- compile(list)
        (varExpr, pred1) <- scoped {
          for {
            varExpr <- WithQueryT.lift(CompM.addColumn(freshVar))
            pred1 <- compile(freshPredOpt.get)
          } yield (varExpr, pred1)
        }
      } yield cypher.Expr.SingleInList(varExpr.id, list1, pred1)

    case expressions.Add(lhs, rhs) =>
      for { lhs1 <- compile(lhs); rhs1 <- compile(rhs) } yield cypher.Expr.Add(lhs1, rhs1)
    case expressions.Subtract(lhs, rhs) =>
      for { lhs1 <- compile(lhs); rhs1 <- compile(rhs) } yield cypher.Expr.Subtract(lhs1, rhs1)
    case expressions.Multiply(lhs, rhs) =>
      for { lhs1 <- compile(lhs); rhs1 <- compile(rhs) } yield cypher.Expr.Multiply(lhs1, rhs1)
    case expressions.Divide(lhs, rhs) =>
      for { lhs1 <- compile(lhs); rhs1 <- compile(rhs) } yield cypher.Expr.Divide(lhs1, rhs1)
    case expressions.Modulo(lhs, rhs) =>
      for { lhs1 <- compile(lhs); rhs1 <- compile(rhs) } yield cypher.Expr.Modulo(lhs1, rhs1)
    case expressions.Pow(lhs, rhs) =>
      for { lhs1 <- compile(lhs); rhs1 <- compile(rhs) } yield cypher.Expr.Exponentiate(lhs1, rhs1)
    case expressions.UnaryAdd(rhs) =>
      for { rhs1 <- compile(rhs) } yield cypher.Expr.UnaryAdd(rhs1)
    case expressions.UnarySubtract(rhs) =>
      for { rhs1 <- compile(rhs) } yield cypher.Expr.UnarySubtract(rhs1)

    case expressions.Not(arg) =>
      for { arg1 <- compile(arg) } yield cypher.Expr.Not(arg1)
    case expressions.And(lhs, rhs) =>
      for { lhs1 <- compile(lhs); rhs1 <- compile(rhs) } yield cypher.Expr.And(Vector(lhs1, rhs1))
    case expressions.Ands(conjuncts) =>
      for {
        conjs1 <- conjuncts.toVector.traverse[WithQueryT[CompM, *], cypher.Expr](compile)
      } yield cypher.Expr.And(conjs1)
    case expressions.Or(lhs, rhs) =>
      for { lhs1 <- compile(lhs); rhs1 <- compile(rhs) } yield cypher.Expr.Or(Vector(lhs1, rhs1))
    case expressions.Ors(disjuncts) =>
      for {
        disjs1 <- disjuncts.toVector.traverse[WithQueryT[CompM, *], cypher.Expr](compile)
      } yield cypher.Expr.Or(disjs1)

    case expressions.Equals(lhs, rhs) =>
      for { lhs1 <- compile(lhs); rhs1 <- compile(rhs) } yield cypher.Expr.Equal(lhs1, rhs1)
    case expressions.In(lhs, expressions.ListLiteral(Seq(rhs))) =>
      for { lhs1 <- compile(lhs); rhs1 <- compile(rhs) } yield cypher.Expr.Equal(lhs1, rhs1)
    case expressions.NotEquals(lhs, rhs) =>
      for { lhs1 <- compile(lhs); rhs1 <- compile(rhs) } yield cypher.Expr.Not(
        cypher.Expr.Equal(lhs1, rhs1)
      )
    case expressions.LessThan(lhs, rhs) =>
      for { lhs1 <- compile(lhs); rhs1 <- compile(rhs) } yield cypher.Expr.Less(lhs1, rhs1)
    case expressions.GreaterThan(lhs, rhs) =>
      for { lhs1 <- compile(lhs); rhs1 <- compile(rhs) } yield cypher.Expr.Greater(lhs1, rhs1)
    case expressions.LessThanOrEqual(lhs, rhs) =>
      for { lhs1 <- compile(lhs); rhs1 <- compile(rhs) } yield cypher.Expr.LessEqual(lhs1, rhs1)
    case expressions.GreaterThanOrEqual(lhs, rhs) =>
      for { lhs1 <- compile(lhs); rhs1 <- compile(rhs) } yield cypher.Expr.GreaterEqual(lhs1, rhs1)
    case expressions.In(lhs, rhs) =>
      for { lhs1 <- compile(lhs); rhs1 <- compile(rhs) } yield cypher.Expr.InList(lhs1, rhs1)

    case expressions.StartsWith(lhs, rhs) =>
      for { lhs1 <- compile(lhs); rhs1 <- compile(rhs) } yield cypher.Expr.StartsWith(lhs1, rhs1)
    case expressions.EndsWith(lhs, rhs) =>
      for { lhs1 <- compile(lhs); rhs1 <- compile(rhs) } yield cypher.Expr.EndsWith(lhs1, rhs1)
    case expressions.Contains(lhs, rhs) =>
      for { lhs1 <- compile(lhs); rhs1 <- compile(rhs) } yield cypher.Expr.Contains(lhs1, rhs1)
    case expressions.RegexMatch(lhs, rhs) =>
      for { lhs1 <- compile(lhs); rhs1 <- compile(rhs) } yield cypher.Expr.Regex(lhs1, rhs1)

    case expressions.IsNull(arg) =>
      for { arg1 <- compile(arg) } yield cypher.Expr.IsNull(arg1)
    case expressions.IsNotNull(arg) =>
      for { arg1 <- compile(arg) } yield cypher.Expr.IsNotNull(arg1)

    case expressions.CaseExpression(expOpt, alts, default) =>
      for {
        expOpt1 <- expOpt.traverse[WithQueryT[CompM, *], cypher.Expr](compile)
        alts1 <- alts.toVector.traverse[WithQueryT[CompM, *], (cypher.Expr, cypher.Expr)] { case (k, v) =>
          compile(k).flatMap(k1 => compile(v).map(k1 -> _))
        }
        default1 <- default.traverse[WithQueryT[CompM, *], cypher.Expr](compile)
      } yield cypher.Expr.Case(expOpt1, alts1, default1)

    // This is for functions we manually resolved
    case qf: QuineFunctionInvocation =>
      for {
        args1 <- qf.args.toVector.traverse[WithQueryT[CompM, *], cypher.Expr](compile)
      } yield cypher.Expr.Function(cypher.Func.UserDefined(qf.function.name), args1)

    case f: expressions.FunctionInvocation =>
      f.args.toVector.traverse[WithQueryT[CompM, *], cypher.Expr](compile).flatMap { args =>

        if (f.function == functions.StartNode) {
          require(args.length == 1, "`startNode` has one argument")
          val nodeName = NodeNameGenerator.name(f.position.newUniquePos())
          val nodeVariable = expressions.Variable(nodeName)(f.position)

          WithQueryT[CompM, cypher.Expr] {
            CompM.addColumn(nodeVariable).map { nodeVarExpr =>
              WithQuery[cypher.Expr](
                result = nodeVarExpr,
                query = cypher.Query.ArgumentEntry(
                  cypher.Expr.RelationshipStart(args.head),
                  cypher.Query.LocalNode(
                    labelsOpt = None,
                    propertiesOpt = None,
                    bindName = Some(nodeVarExpr.id)
                  )
                )
              )
            }
          }
        } else if (f.function == functions.Exists) {
          require(args.length == 1, "`exists` has one argument")

          /* The case where the argument is a pattern expression has already
           * been rewritten in `patternExpressionAsComprehension`, so the only
           * remaining variant of `exists` is the one that checks whether
           * accessing a property or field produces `null`
           */
          WithQueryT.pure[CompM, cypher.Expr](cypher.Expr.IsNotNull(args.head))
        } else if (f.function == functions.EndNode) {
          require(args.length == 1, "`endNode` has one argument")
          val nodeName = NodeNameGenerator.name(f.position.newUniquePos())
          val nodeVariable = expressions.Variable(nodeName)(f.position)

          WithQueryT[CompM, cypher.Expr] {
            CompM.addColumn(nodeVariable).map { nodeVarExpr =>
              WithQuery[cypher.Expr](
                result = nodeVarExpr,
                query = cypher.Query.ArgumentEntry(
                  cypher.Expr.RelationshipEnd(args.head),
                  cypher.Query.LocalNode(
                    labelsOpt = None,
                    propertiesOpt = None,
                    bindName = Some(nodeVarExpr.id)
                  )
                )
              )
            }
          }
        } else {
          WithQueryT[CompM, cypher.Expr](
            compileBuiltinScalarFunction(f.function, f).map { func =>
              WithQuery[cypher.Expr](cypher.Expr.Function(func, args.toVector))
            }
          )
        }
      }

    case e @ expressions.GetDegree(node, relType, dir) =>
      val bindName = UnNamedNameGenerator.name(e.position.newUniquePos())
      val bindVariable = expressions.Variable(bindName)(e.position)

      WithQueryT[CompM, cypher.Expr] {
        for {
          nodeExprWc: WithQuery[cypher.Expr] <- compileM(node)
          bindVarExpr <- CompM.addColumn(bindVariable)
          direction = dir match {
            case expressions.SemanticDirection.OUTGOING => EdgeDirection.Outgoing
            case expressions.SemanticDirection.INCOMING => EdgeDirection.Incoming
            case expressions.SemanticDirection.BOTH => EdgeDirection.Undirected
          }
        } yield nodeExprWc.flatMap[cypher.Expr] { nodeExpr =>
          WithQuery(
            result = bindVarExpr,
            query = cypher.Query.ArgumentEntry(
              nodeExpr,
              cypher.Query.GetDegree(
                edgeName = relType.map(r => Symbol(r.name)),
                direction,
                bindName = bindVarExpr.id
              )
            )
          )
        }
      }

    case expressions.HasLabels(expr, labels) =>
      compile(expr).flatMap { (nodeExpr: cypher.Expr) =>
        WithQueryT[CompM, cypher.Expr](
          result = cypher.Expr.True,
          query = cypher.Query.ArgumentEntry(
            nodeExpr,
            cypher.Query.LocalNode(
              labelsOpt = Some(labels.map(lbl => Symbol(lbl.name))),
              propertiesOpt = None,
              bindName = None
            )
          )
        )
      }

    case e @ expressions.PatternComprehension(namedPath, rel, pred, project) =>
      require(namedPath.isEmpty) // TODO: is this an issue?

      // Put the pattern into a form
      val pat = expressions.Pattern(Seq(expressions.EveryPath(rel.element)))(e.position)
      val pathName = UnNamedNameGenerator.name(e.position.newUniquePos())
      val pathVariable = expressions.Variable(pathName)(e.position)

      WithQueryT {
        CompM.withIsolatedContext {
          for {
            graph <- Graph.fromPattern(pat)
            patQuery <- graph.synthesizeFetch(WithFreeVariables.empty)
            predWqOpt <- pred.traverse(compileM)
            projectWq <- compileM(project)
            pathVarExpr <- CompM.addColumn(pathVariable)
          } yield projectWq.flatMap { (returnExpr: cypher.Expr) =>
            val queryPart = cypher.Query.EagerAggregation(
              aggregateAlong = Vector.empty,
              aggregateWith = Vector(
                pathVarExpr.id -> cypher.Aggregator.collect(
                  distinct = false,
                  returnExpr
                )
              ),
              toAggregate = predWqOpt.sequence.toQuery {
                case None => patQuery
                case Some(filterCond) => cypher.Query.filter(filterCond, patQuery)
              },
              keepExisting = true
            )

            WithQuery(pathVarExpr, queryPart)
          }
        }
      }

    case _: expressions.PatternExpression =>
      // Should be impossible, thanks to [[patternExpressionAsComprehension]]
      WithQueryT {
        CompM.raiseCompileError("Unexpected pattern expression", e)
      }

    case expressions.PathExpression(steps) =>
      // TODO: gain some confidence in the exhaustiveness/correctness of this
      def visitPath(path: expressions.PathStep): WithQueryT[CompM, List[cypher.Expr]] = path match {
        case expressions.NilPathStep => WithQueryT.pure(Nil)
        case expressions.NodePathStep(node, restPath) =>
          for {
            head <- compile(node)
            tail <- visitPath(restPath)
          } yield (head :: tail)
        case expressions.SingleRelationshipPathStep(
              rel,
              _,
              _,
              restPath: expressions.NodePathStep
            ) =>
          for {
            head <- compile(rel)
            tail <- visitPath(restPath)
          } yield (head :: tail)
        case expressions.SingleRelationshipPathStep(rel, _, toNode, restPath) =>
          require(toNode.nonEmpty)
          for {
            head1 <- compile(rel)
            head2 <- compile(toNode.get)
            tail <- visitPath(restPath)
          } yield (head1 :: head2 :: tail)
        case _ =>
          WithQueryT {
            CompM.raiseCompileError("Unsupported path expression", e)
          }
      }

      visitPath(steps).map(l => cypher.Expr.PathExpression(l.toVector))

    case e @ expressions.ShortestPathExpression(expressions.ShortestPaths(elem, true)) =>
      // shortest path, implemented as syntactic sugar over a procedure call to [[ShortestPath]]
      elem match {
        case expressions.RelationshipChain(
              expressions.NodePattern(Some(startNodeLv), Seq(), None, None),
              expressions.RelationshipPattern(None, edgeTypes, length, None, direction, _, None),
              expressions.NodePattern(Some(endNodeLv), Seq(), None, None)
            ) =>
          // An APOC-style map of optional arguments passed to the algorithms.shortestPath procedure

          // length options
          val lengthOptions = length match {
            // eg (n)-[:has_father]->(m)
            case None =>
              Map("maxLength" -> cypher.Expr.Integer(1L))
            // eg (n)-[:has_father*]->(m)
            case Some(None) =>
              Map.empty
            // eg (n)-[:has_father*2..6]->(m) or (n)-[:has_father*..6]->(m) or (n)-[:has_father2*..]->(m)
            case Some(Some(expressions.Range(loOpt, hiOpt))) =>
              loOpt.map { lo =>
                "minLength" -> cypher.Expr.Integer(lo.value.toLong)
              }.toMap ++
                hiOpt.map { hi =>
                  "maxLength" -> cypher.Expr.Integer(hi.value.toLong)
                }
          }

          // direction (initially w.r.t. startNodeLv)
          val directionOption = direction match {
            case expressions.SemanticDirection.OUTGOING =>
              Map("direction" -> cypher.Expr.Str("outgoing"))
            case expressions.SemanticDirection.INCOMING =>
              Map("direction" -> cypher.Expr.Str("incoming"))
            case expressions.SemanticDirection.BOTH =>
              Map.empty
          }

          // edge types
          val edgeTypeOption = edgeTypes match {
            case Seq() =>
              Map.empty
            case edges =>
              val edgesStrVect = edges.map(rel => cypher.Expr.Str(rel.name)).toVector
              Map("types" -> cypher.Expr.List(edgesStrVect))
          }

          val shortestPathName = UnNamedNameGenerator.name(e.position.newUniquePos())
          val shortestPathVariable = expressions.Variable(shortestPathName)(e.position)

          WithQueryT {
            for {
              startNode <- CompM.getVariable(startNodeLv, e)
              endNode <- CompM.getVariable(endNodeLv, e)
              shortestPathVarExpr <- CompM.addColumn(shortestPathVariable)
            } yield WithQuery[cypher.Expr](
              shortestPathVarExpr,
              cypher.Query.ProcedureCall(
                cypher.Proc.ShortestPath,
                Vector(
                  startNode,
                  endNode,
                  cypher.Expr.Map(lengthOptions ++ directionOption ++ edgeTypeOption)
                ),
                Some(Map(cypher.Proc.ShortestPath.retColumnPathName -> shortestPathVarExpr.id))
              )
            )
          }

        // TODO: make this a little more informative...
        case p =>
          WithQueryT {
            CompM.raiseCompileError("Unsupported shortest path expression", p)
          }
      }

    case e =>
      WithQueryT {
        CompM.raiseCompileError("Unsupported expression", e)
      }
  }

  /** Map a simple scalar function into its IR equivalent.
    *
    * The monadic context is just to facilitate error messages for non-simple or
    * non-scalar functions.
    *
    * @param func function to translate
    * @param callExpr expression from which the function call came (used for errors)
    * @return IR function
    */
  private def compileBuiltinScalarFunction(
    func: functions.Function,
    callExpr: expressions.FunctionInvocation
  ): CompM[cypher.Func] =
    func match {
      case functions.Abs => CompM.pure(cypher.Func.Abs)
      case functions.Acos => CompM.pure(cypher.Func.Acos)
      case functions.Asin => CompM.pure(cypher.Func.Asin)
      case functions.Atan => CompM.pure(cypher.Func.Atan)
      case functions.Atan2 => CompM.pure(cypher.Func.Atan2)
      case functions.Ceil => CompM.pure(cypher.Func.Ceil)
      case functions.Coalesce => CompM.pure(cypher.Func.Coalesce)
      case functions.Cos => CompM.pure(cypher.Func.Cos)
      case functions.Cot => CompM.pure(cypher.Func.Cot)
      case functions.Degrees => CompM.pure(cypher.Func.Degrees)
      case functions.E => CompM.pure(cypher.Func.E)
      case functions.Exp => CompM.pure(cypher.Func.Exp)
      case functions.Floor => CompM.pure(cypher.Func.Floor)
      case functions.Haversin => CompM.pure(cypher.Func.Haversin)
      case functions.Head => CompM.pure(cypher.Func.Head)
      case functions.Id => CompM.pure(cypher.Func.Id)
      case functions.Keys => CompM.pure(cypher.Func.Keys)
      case functions.Labels => CompM.pure(cypher.Func.Labels)
      case functions.Last => CompM.pure(cypher.Func.Last)
      case functions.Left => CompM.pure(cypher.Func.Left)
      case functions.Length => CompM.pure(cypher.Func.Length)
      case functions.Log => CompM.pure(cypher.Func.Log)
      case functions.Log10 => CompM.pure(cypher.Func.Log10)
      case functions.LTrim => CompM.pure(cypher.Func.LTrim)
      case functions.Nodes => CompM.pure(cypher.Func.Nodes)
      case functions.Pi => CompM.pure(cypher.Func.Pi)
      case functions.Properties => CompM.pure(cypher.Func.Properties)
      case functions.Rand => CompM.pure(cypher.Func.Rand)
      case functions.Range => CompM.pure(cypher.Func.Range)
      case functions.Relationships => CompM.pure(cypher.Func.Relationships)
      case functions.Replace => CompM.pure(cypher.Func.Replace)
      case functions.Reverse => CompM.pure(cypher.Func.Reverse)
      case functions.Right => CompM.pure(cypher.Func.Right)
      case functions.RTrim => CompM.pure(cypher.Func.RTrim)
      case functions.Sign => CompM.pure(cypher.Func.Sign)
      case functions.Sin => CompM.pure(cypher.Func.Sin)
      case functions.Size => CompM.pure(cypher.Func.Size)
      case functions.Split => CompM.pure(cypher.Func.Split)
      case functions.Sqrt => CompM.pure(cypher.Func.Sqrt)
      case functions.Substring => CompM.pure(cypher.Func.Substring)
      case functions.Tail => CompM.pure(cypher.Func.Tail)
      case functions.Tan => CompM.pure(cypher.Func.Tan)
      case functions.ToBoolean => CompM.pure(cypher.Func.ToBoolean)
      case functions.ToFloat => CompM.pure(cypher.Func.ToFloat)
      case functions.ToInteger => CompM.pure(cypher.Func.ToInteger)
      case functions.ToLower => CompM.pure(cypher.Func.ToLower)
      case functions.ToString => CompM.pure(cypher.Func.ToString)
      case functions.ToUpper => CompM.pure(cypher.Func.ToUpper)
      case functions.Trim => CompM.pure(cypher.Func.Trim)
      case functions.Type => CompM.pure(cypher.Func.Type)

      case functions.Avg | functions.Collect | functions.Min | functions.Max | functions.PercentileCont |
          functions.PercentileDisc | functions.StdDev | functions.StdDevP | functions.Sum =>
        CompM.raiseCompileError(
          message = s"Invalid position for aggregating function `${func.name}`",
          astNode = callExpr
        )

      case functions.StartNode | functions.EndNode =>
        CompM.raiseCompileError(
          message = s"Compiler error: `${func.name}` should already have been handled",
          astNode = callExpr
        )

      case functions.File | functions.Linenumber | functions.Point | functions.Distance | functions.Reduce | _ =>
        CompM.raiseCompileError(
          message = s"Failed to resolve function `${callExpr.name}`",
          astNode = callExpr
        )
    }
}

/** Turns all pattern expressions into pattern comprehensions
  *
  * This happens for two reasons:
  *   - to take advantage of `namePatternComprehensionPatternElements`
  *   - to take advantage of rewrites that synthesize `PathExpression`'s for us
  *   - to leave the avenue open for additional rewrites adding predicates
  *
  * TODO: add a phase that adds uniqueness constraints to the predicate of the
  *       pattern comprehensions, so `(a)<--(b)` won't produce a result where
  *       `a = b`. Inspiration: `AddUniquenessPredicates`
  */
case object patternExpressionAsComprehension extends StatementRewriter {

  import org.opencypher.v9_0.frontend.phases._
  import org.opencypher.v9_0.util.{bottomUp, Rewriter}

  override def instance(ctx: BaseContext): Rewriter = bottomUp(Rewriter.lift {
    case e: expressions.PatternExpression =>
      patternExpr2Comp(e)

    /* Rewrite `exists(patternComprehension)` into `patternComprehension <> []`.
     *
     * Putting this rewrite here is actually necessary: our re-write for general
     * pattern expressions will ensure that the argument to `exists` is no
     * longer a pattern expression. This means that if we _don't_ rewrite the
     * call to `exists` here, we'll fail openCypher's `SemanticAnalysis` phase
     */
    case fi @ expressions.FunctionInvocation(
          _,
          _,
          false,
          IndexedSeq(patternComp: expressions.PatternComprehension)
        ) if fi.function == functions.Exists =>
      val emptyList = expressions.ListLiteral(Seq())(fi.position)
      expressions.NotEquals(patternComp, emptyList)(fi.position)
  })

  def patternExpr2Comp(e: expressions.PatternExpression): expressions.PatternComprehension = {
    val expressions.PatternExpression(relsPat) = e

    val freshName = UnNamedNameGenerator.name(e.position.newUniquePos())
    val freshVariable = expressions.Variable(freshName)(e.position)

    expressions.PatternComprehension(
      namedPath = Some(freshVariable),
      pattern = relsPat,
      predicate = None,
      projection = freshVariable
    )(e.position, Set.empty)
  }

  // TODO: add to this
  override def postConditions: Set[Condition] = Set.empty
}
