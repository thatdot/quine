package com.thatdot.quine.graph.cypher.quinepattern

import com.thatdot.cypher.ast.{QueryPart, ReadingClause}
import com.thatdot.cypher.phases.SymbolAnalysisModule
import com.thatdot.cypher.{ast => Cypher}
import com.thatdot.language.{ast => Pattern}

object LazyQuinePatternQueryPlanner {

  /** Represents the structure and behavior of a lazily evaluated query execution plan.
    *
    * This sealed trait acts as the core abstraction for constructing, transforming,
    * and executing query plans in a deferred manner. It serves as a foundation for
    * various specific types of query planning components and operations, allowing for
    * modular and composable representations of query logic.
    *
    * The `LazyQueryPlan` is primarily used within the context of the `LazyQuinePatternQueryPlanner`
    * for translating graph query specifications into executable plans. Its design supports
    * incremental evaluation, conditional processing, refinement, and optimizations.
    *
    * Subtypes of `LazyQueryPlan` include:
    * - `Product`: Represents a combination of multiple query subplans.
    * - `ReAnchor`: Reinterprets a query plan based on a specified identifier expression.
    * - `Unwind`: Iteratively processes results based on a list expression, binding variables and executing a subquery.
    * - `FilterMap`: Applies a filtering predicate, mapping logic, and optional distinctness to a query plan.
    * - `Watch`: Constructs monitoring operations for observing dynamic query predicates.
    * - `WatchEdge`: Monitors relationships with a specific direction and label within plans.
    * - `DoEffect`: Executes side-effect-producing graph operations as part of the execution flow.
    */
  sealed trait LazyQueryPlan

  object LazyQueryPlan {
    case class Product(of: List[LazyQueryPlan]) extends LazyQueryPlan
    case class FilterMap(
      filter: Pattern.Expression,
      map: List[Cypher.Projection],
      isDistinct: Boolean,
      queryPlan: LazyQueryPlan,
    ) extends LazyQueryPlan
    case class Watch(watch: Pattern.Expression) extends LazyQueryPlan
    case class WatchEdge(label: Symbol, direction: Pattern.Direction, plan: LazyQueryPlan) extends LazyQueryPlan

    case class ReAnchor(idExp: Pattern.Expression) extends LazyQueryPlan
    case class Unwind(listExp: Pattern.Expression, binding: Pattern.Identifier, subquery: LazyQueryPlan)
        extends LazyQueryPlan
    case class DoEffect(effect: Cypher.Effect) extends LazyQueryPlan
  }

  def findFiltersFor(
    identifier: Pattern.Identifier,
    maybeExpression: Option[Pattern.Expression],
  ): List[Pattern.Expression] =
    maybeExpression match {
      case Some(value) =>
        value match {
          case idl: Pattern.Expression.IdLookup =>
            if (idl.nodeIdentifier == identifier) List(value) else Nil
          case sid: Pattern.Expression.SynthesizeId =>
            sid.from.flatMap(exp => findFiltersFor(identifier, Some(exp)))
          case _: Pattern.Expression.AtomicLiteral => Nil
          case _: Pattern.Expression.ListLiteral =>
            throw new QuinePatternUnimplementedException(s"Unsupported exp: $value")
          case _: Pattern.Expression.MapLiteral =>
            throw new QuinePatternUnimplementedException(s"Unsupported exp: $value")
          case idExp: Pattern.Expression.Ident =>
            if (idExp.identifier == identifier) List(value) else Nil
          case _: Pattern.Expression.Parameter => Nil
          case _: Pattern.Expression.Apply => throw new QuinePatternUnimplementedException(s"Unsupported exp: $value")
          case unary: Pattern.Expression.UnaryOp =>
            findFiltersFor(identifier, Some(unary.exp))
          case Pattern.Expression.BinOp(_, _, lhs, rhs, _) =>
            findFiltersFor(identifier, Some(lhs)) ++ findFiltersFor(identifier, Some(rhs))
          case fa: Pattern.Expression.FieldAccess =>
            if (QuinePatternHelpers.getRootId(fa.of) == identifier) List(fa) else Nil
          case _: Pattern.Expression.IndexIntoArray =>
            throw new QuinePatternUnimplementedException(s"Unsupported exp: $value")
          case isNull: Pattern.Expression.IsNull => findFiltersFor(identifier, Some(isNull.of))
          case _: Pattern.Expression.CaseBlock =>
            throw new QuinePatternUnimplementedException(s"Unsupported exp: $value")
        }
      case None => Nil
    }

  def findProjectionsFor(identifier: Pattern.Identifier, projection: Cypher.Projection): List[Cypher.Projection] =
    projection.expression match {
      case idLookup: Pattern.Expression.IdLookup => if (idLookup.nodeIdentifier == identifier) List(projection) else Nil
      case _: Pattern.Expression.SynthesizeId =>
        throw new QuinePatternUnimplementedException(s"Unsupported exp: ${projection.expression}")
      case _: Pattern.Expression.AtomicLiteral =>
        throw new QuinePatternUnimplementedException(s"Unsupported exp: ${projection.expression}")
      case _: Pattern.Expression.ListLiteral =>
        throw new QuinePatternUnimplementedException(s"Unsupported exp: ${projection.expression}")
      case _: Pattern.Expression.MapLiteral =>
        throw new QuinePatternUnimplementedException(s"Unsupported exp: ${projection.expression}")
      case _: Pattern.Expression.Ident =>
        throw new QuinePatternUnimplementedException(s"Unsupported exp: ${projection.expression}")
      case _: Pattern.Expression.Parameter =>
        throw new QuinePatternUnimplementedException(s"Unsupported exp: ${projection.expression}")
      case _: Pattern.Expression.Apply =>
        throw new QuinePatternUnimplementedException(s"Unsupported exp: ${projection.expression}")
      case _: Pattern.Expression.UnaryOp =>
        throw new QuinePatternUnimplementedException(s"Unsupported exp: ${projection.expression}")
      case _: Pattern.Expression.BinOp =>
        throw new QuinePatternUnimplementedException(s"Unsupported exp: ${projection.expression}")
      case fa: Pattern.Expression.FieldAccess =>
        val rootId = QuinePatternHelpers.getRootId(fa.of)
        if (rootId == identifier) List(projection) else Nil
      case _: Pattern.Expression.IndexIntoArray =>
        throw new QuinePatternUnimplementedException(s"Unsupported exp: ${projection.expression}")
      case _: Pattern.Expression.IsNull =>
        throw new QuinePatternUnimplementedException(s"Unsupported exp: ${projection.expression}")
      case _: Pattern.Expression.CaseBlock =>
        throw new QuinePatternUnimplementedException(s"Unsupported exp: ${projection.expression}")
    }

  def createWatches(
    nodeVariable: Pattern.Identifier,
    maybeExpression: Option[Pattern.Expression],
    projections: List[Cypher.Projection],
    parts: List[Cypher.QueryPart],
  ): List[LazyQueryPlan.Watch] = {
    val filterParts = parts.map {
      case QueryPart.ReadingClausePart(readingClause) =>
        readingClause match {
          case ReadingClause.FromPatterns(_, _, maybePredicate) => maybePredicate
          case _ => None
        }
      case QueryPart.WithClausePart(withClause) => withClause.maybePredicate
      case _: QueryPart.EffectPart => None
    }
    val filtersFor = (maybeExpression :: filterParts).flatMap(findFiltersFor(nodeVariable, _)).distinct
    val projectionParts = parts.flatMap {
      case _: QueryPart.ReadingClausePart => Nil
      case QueryPart.WithClausePart(withClause) => withClause.bindings
      case _: QueryPart.EffectPart => Nil
    }
    val projectionsFor = (projections ::: projectionParts).flatMap(findProjectionsFor(nodeVariable, _)).distinct
    val filterWatches = filtersFor.map(exp => LazyQueryPlan.Watch(exp))
    val projectionWatches = projectionsFor.map(p => LazyQueryPlan.Watch(p.expression))
    filterWatches ::: projectionWatches
  }

  //This is a placeholder for future work
  def getPatternFilters(pattern: Cypher.NodePattern): Option[Pattern.Expression] =
    pattern.maybeProperties match {
      case Some(propMap) =>
        pattern.maybeBinding match {
          case Some(binding) =>
            propMap.value.foldLeft(Option.empty[Pattern.Expression]) { (maybeExp, prop) =>
              val testExp = Pattern.Expression.BinOp(
                source = Pattern.Source.NoSource,
                op = Pattern.Operator.Equals,
                lhs = Pattern.Expression.FieldAccess(
                  source = Pattern.Source.NoSource,
                  of = Pattern.Expression.Ident(
                    source = Pattern.Source.NoSource,
                    identifier = binding,
                    ty = None,
                  ),
                  fieldName = prop._1,
                  ty = None,
                ),
                rhs = prop._2,
                ty = None,
              )
              maybeExp match {
                case None => Some(testExp)
                case Some(otherExp) =>
                  Some(
                    Pattern.Expression.BinOp(
                      source = Pattern.Source.NoSource,
                      op = Pattern.Operator.And,
                      lhs = testExp,
                      rhs = otherExp,
                      ty = None,
                    ),
                  )
              }
            }
          case None => None
        }
      case None => None
    }

  def planConnections(
    pattern: Cypher.NodePattern,
    connections: List[Cypher.Connection],
    maybeExpression: Option[Pattern.Expression],
    isDistinct: Boolean,
    projections: List[Cypher.Projection],
    parts: List[Cypher.QueryPart],
    initialPlan: Boolean,
  ): LazyQueryPlan = connections match {
    case Nil =>
      val watches = createWatches(pattern.maybeBinding.get, maybeExpression, projections, parts)
      val patternPlan = LazyQueryPlan.Product(watches)
      if (initialPlan)
        if (maybeExpression.isEmpty && projections.isEmpty) {
          patternPlan
        } else {
          LazyQueryPlan.FilterMap(
            maybeExpression.getOrElse(Pattern.Expression.mkAtomicLiteral(Pattern.Source.NoSource, Pattern.Value.True)),
            projections,
            isDistinct,
            patternPlan,
          )
        }
      else
        patternPlan
    case h :: Nil =>
      val watches = createWatches(pattern.maybeBinding.get, maybeExpression, projections, parts)
      val destWatches = createWatches(h.dest.maybeBinding.get, maybeExpression, projections, parts)
      val patternPlan = LazyQueryPlan.Product(
        watches :+ LazyQueryPlan.WatchEdge(h.edge.labels.head, h.edge.direction, LazyQueryPlan.Product(destWatches)),
      )
      if (initialPlan)
        if (maybeExpression.isEmpty && projections.isEmpty) {
          patternPlan
        } else {
          LazyQueryPlan.FilterMap(
            maybeExpression.getOrElse(Pattern.Expression.mkAtomicLiteral(Pattern.Source.NoSource, Pattern.Value.True)),
            projections,
            isDistinct,
            patternPlan,
          )
        }
      else
        patternPlan
    case h :: t =>
      val watches = createWatches(pattern.maybeBinding.get, maybeExpression, projections, parts)
      val restOfPlan = planConnections(h.dest, t, maybeExpression, isDistinct, projections, parts, false)
      val patternPlan =
        LazyQueryPlan.Product(watches :+ LazyQueryPlan.WatchEdge(h.edge.labels.head, h.edge.direction, restOfPlan))
      if (initialPlan)
        if (maybeExpression.isEmpty && projections.isEmpty) {
          patternPlan
        } else {
          LazyQueryPlan.FilterMap(
            maybeExpression.getOrElse(Pattern.Expression.mkAtomicLiteral(Pattern.Source.NoSource, Pattern.Value.True)),
            projections,
            isDistinct,
            patternPlan,
          )
        }
      else
        patternPlan
  }

  def planPatterns(
    patterns: List[Cypher.GraphPattern],
    maybePredicate: Option[Pattern.Expression],
    isDistinct: Boolean,
    projections: List[Cypher.Projection],
    remainingParts: List[Cypher.QueryPart],
  ): LazyQueryPlan = patterns match {
    case Nil => throw new QuinePatternUnimplementedException("Empty patterns")
    case h :: Nil => planConnections(h.initial, h.path, maybePredicate, isDistinct, projections, remainingParts, true)
    case h :: t =>
      LazyQueryPlan.Product(
        List(
          planConnections(h.initial, h.path, maybePredicate, isDistinct, projections, remainingParts, true),
          planPatterns(t, maybePredicate, isDistinct, projections, remainingParts),
        ),
      )
  }

  def planSinglePartQuery(
    queryParts: List[Cypher.QueryPart],
    isDistinct: Boolean,
    projections: List[Cypher.Projection],
    table: SymbolAnalysisModule.SymbolTable,
  ): LazyQueryPlan = queryParts match {
    case Nil => throw new QuinePatternUnimplementedException("Empty query parts")
    case h :: Nil =>
      h match {
        case Cypher.QueryPart.ReadingClausePart(readingClause) =>
          readingClause match {
            case Cypher.ReadingClause.FromPatterns(_, patterns, maybePredicate) =>
              planPatterns(patterns, maybePredicate, isDistinct, projections, Nil)
            case _: Cypher.ReadingClause.FromUnwind =>
              throw new QuinePatternUnimplementedException(s"Unsupported clause: $h")
            case _: Cypher.ReadingClause.FromProcedure =>
              throw new QuinePatternUnimplementedException(s"Unsupported clause: $h")
            case _: Cypher.ReadingClause.FromSubquery =>
              throw new QuinePatternUnimplementedException(s"Unsupported clause: $h")
          }
        case _: Cypher.QueryPart.WithClausePart =>
          throw new QuinePatternUnimplementedException(s"Unsupported clause: $h")
        case Cypher.QueryPart.EffectPart(effect) => LazyQueryPlan.DoEffect(effect)
      }
    case h :: t =>
      h match {
        case Cypher.QueryPart.ReadingClausePart(readingClause) =>
          readingClause match {
            case Cypher.ReadingClause.FromPatterns(_, patterns, maybePredicate) =>
              planPatterns(patterns, maybePredicate, isDistinct, projections, Nil)
            case Cypher.ReadingClause.FromUnwind(_, list, as) =>
              LazyQueryPlan.Unwind(list, as, planSinglePartQuery(t, isDistinct, projections, table))
            case _: Cypher.ReadingClause.FromProcedure =>
              throw new QuinePatternUnimplementedException(s"Unsupported clause: $h")
            case _: Cypher.ReadingClause.FromSubquery =>
              throw new QuinePatternUnimplementedException(s"Unsupported clause: $h")
          }
        case _: Cypher.QueryPart.WithClausePart =>
          throw new QuinePatternUnimplementedException(s"Unsupported clause: $h")
        case Cypher.QueryPart.EffectPart(effect) => LazyQueryPlan.DoEffect(effect)
      }
  }

  /** Plans a Cypher query and constructs a lazy query execution plan.
    *
    * This method processes a `Cypher.Query` and generates a corresponding `LazyQueryPlan`
    * based on the underlying query structure. It supports `SingleQuery` types and throws
    * an exception for unsupported query types, such as unions.
    *
    * @param cypherAst   the abstract syntax tree representation of the Cypher query to be planned
    * @param symbolTable the symbol table containing contextual information about symbols used in the query
    * @return a lazily evaluated query plan that represents the execution logic for the given query
    */
  def planQuery(cypherAst: Cypher.Query, symbolTable: SymbolAnalysisModule.SymbolTable): LazyQueryPlan =
    cypherAst match {
      case _: Cypher.Query.Union => throw new QuinePatternUnimplementedException("Union queries are not supported")
      case sq: Cypher.Query.SingleQuery =>
        sq match {
          case mpq: Cypher.Query.SingleQuery.MultipartQuery =>
            LazyQueryPlan.Product(
              planSinglePartQuery(mpq.queryParts, false, Nil, symbolTable) :: mpq.into
                .map(spq => List(planSinglePartQuery(spq.queryParts, spq.isDistinct, spq.bindings, symbolTable)))
                .getOrElse(Nil),
            )
          case spq: Cypher.Query.SingleQuery.SinglepartQuery =>
            planSinglePartQuery(spq.queryParts, spq.isDistinct, spq.bindings, symbolTable)
        }
    }
}

/** Object responsible for planning and generating QueryPlan representations of Cypher queries.
  * Utilizes eager evaluation to convert patterns, filters, and clauses into executable plans within Quine.
  */
object EagerQuinePatternQueryPlanner {

  /** Extracts an ID predicate condition from a given node pattern and combines it with the remaining filter conditions.
    *
    * @param pattern The node pattern being analyzed, which may contain a binding to a specific node variable.
    * @param filter  The filter expression associated with the node pattern, which may contain logical or ID-specific conditions.
    * @return A tuple where the first element is an optional extracted ID predicate and the second element is the modified filter expression with any extracted ID predicate removed
    *         .
    */
  def getIdPredicateForNodePattern(
    pattern: Cypher.NodePattern,
    filter: Pattern.Expression,
  ): (Option[Pattern.Expression], Pattern.Expression) =
    pattern.maybeBinding match {
      case Some(binding) =>
        filter match {
          case binop @ Pattern.Expression.BinOp(_, op, lhs, rhs, _) =>
            op match {
              case Pattern.Operator.And =>
                val (idMatch, restLeft) = getIdPredicateForNodePattern(pattern, lhs)
                val (idMatch2, restRight) = getIdPredicateForNodePattern(pattern, rhs)
                idMatch.orElse(idMatch2) -> binop.copy(lhs = restLeft, rhs = restRight)
              case Pattern.Operator.Equals =>
                (lhs, rhs) match {
                  // This looks like it should be an anchor, but it isn't. :(
                  case (_: Pattern.Expression.IdLookup, _: Pattern.Expression.IdLookup) =>
                    None -> binop
                  case (idl: Pattern.Expression.IdLookup, idExp) =>
                    if (idl.nodeIdentifier == binding)
                      Some(idExp) -> Pattern.Expression.mkAtomicLiteral(Pattern.Source.NoSource, Pattern.Value.True)
                    else
                      None -> binop
                  case (idExp, idl: Pattern.Expression.IdLookup) =>
                    if (idl.nodeIdentifier == binding)
                      Some(idExp) -> Pattern.Expression.mkAtomicLiteral(Pattern.Source.NoSource, Pattern.Value.True)
                    else
                      None -> binop
                  case (_, _) => (None, binop)
                }
              case _ => (None, binop)
            }
          case _ => (None, filter)
        }
      case None => None -> filter
    }

  /** Plans a query subgraph traversal or matching operation based on the provided node pattern, list of connections,
    * and remaining filter conditions.
    *
    * @param pattern         A node pattern to be matched, potentially including bindings to specific variables.
    * @param connections     A list of connections representing relationships or edges to traverse from the given node.
    * @param remainingFilter An expression representing additional constraints or filters to apply during planning.
    * @return A pair where the first element is a `QueryPlan` describing the execution steps necessary for traversal
    *         or matching, and the second element is the remaining filter expression after planning.
    */
  def planConnection(
    pattern: Cypher.NodePattern,
    connections: List[Cypher.Connection],
    remainingFilter: Pattern.Expression,
  ): (QueryPlan, Pattern.Expression) = {
    val (maybeIdPredicate, exp) = getIdPredicateForNodePattern(pattern, remainingFilter)
    maybeIdPredicate match {
      case None =>
        pattern.maybeBinding match {
          case Some(value) =>
            connections match {
              case Nil => QueryPlan.AllNodeScan(QueryPlan.LoadNode(value)) -> exp
              case h :: t =>
                val stepResult = planConnection(h.dest, t, exp)
                QueryPlan.AllNodeScan(
                  QueryPlan.Product(
                    QueryPlan.LoadNode(value),
                    QueryPlan.TraverseEdge(
                      h.edge.labels.head,
                      QuinePatternHelpers.directionToEdgeDirection(h.edge.direction),
                      stepResult._1,
                    ),
                  ),
                ) -> stepResult._2
            }
          case None =>
            connections match {
              case Nil => throw new QuinePatternUnimplementedException("???") //QueryPlan.AllNodeScan(Nil) -> exp
              case h :: t =>
                val stepResult = planConnection(h.dest, t, exp)
                QueryPlan.AllNodeScan(
                  QueryPlan.TraverseEdge(
                    h.edge.labels.head,
                    QuinePatternHelpers.directionToEdgeDirection(h.edge.direction),
                    stepResult._1,
                  ),
                ) -> stepResult._2
            }
        }
      case Some(idPredicate) =>
        connections match {
          case h :: t =>
            val stepResult = planConnection(h.dest, t, exp)
            QueryPlan.SpecificId(
              idPredicate,
              QueryPlan.TraverseEdge(
                h.edge.labels.head,
                QuinePatternHelpers.directionToEdgeDirection(h.edge.direction),
                stepResult._1,
              ),
            ) -> stepResult._2
          case Nil => QueryPlan.SpecificId(idPredicate, QueryPlan.LoadNode(pattern.maybeBinding.get)) -> exp
        }
    }
  }

  def planFilteredPattern(
    pattern: PatternChain,
    filter: Pattern.Expression,
    initial: Boolean,
  ): (QueryPlan, Pattern.Expression) =
    pattern match {
      case PatternChain.ChainCont(nodePattern, edge, chain) =>
        getIdPredicateForNodePattern(nodePattern, filter) match {
          case (None, exp) =>
            val (remainingPlan, remainingFilter) = planFilteredPattern(chain, exp, false)
            val traverseEdge = QueryPlan.TraverseEdge(
              edge.labels.head,
              QuinePatternHelpers.directionToEdgeDirection(edge.direction),
              remainingPlan,
            )
            if (initial)
              QueryPlan.AllNodeScan(
                QueryPlan.Product(QueryPlan.LoadNode(nodePattern.maybeBinding.get), traverseEdge),
              ) -> remainingFilter
            else
              QueryPlan.Product(QueryPlan.LoadNode(nodePattern.maybeBinding.get), traverseEdge) -> remainingFilter
          case (Some(idExp), exp) =>
            val (remainingPlan, remainingFilter) = planFilteredPattern(chain, exp, false)
            val traverseEdge = QueryPlan.TraverseEdge(
              edge.labels.head,
              QuinePatternHelpers.directionToEdgeDirection(edge.direction),
              remainingPlan,
            )
            if (initial)
              QueryPlan.SpecificId(
                idExp,
                QueryPlan.Product(QueryPlan.LoadNode(nodePattern.maybeBinding.get), traverseEdge),
              ) -> remainingFilter
            else
              QueryPlan.Product(
                QueryPlan.LoadNode(nodePattern.maybeBinding.get),
                QueryPlan.Product(
                  QueryPlan.Filter(
                    Pattern.Expression.BinOp(
                      Pattern.Source.NoSource,
                      Pattern.Operator.Equals,
                      Pattern.Expression.Apply(
                        Pattern.Source.NoSource,
                        Symbol("id"),
                        List(
                          Pattern.Expression.Ident(Pattern.Source.NoSource, nodePattern.maybeBinding.get, None),
                        ),
                        None,
                      ),
                      idExp,
                      None,
                    ),
                  ),
                  traverseEdge,
                ),
              ) -> exp
        }
      case PatternChain.ChainEnd(nodePattern) =>
        getIdPredicateForNodePattern(nodePattern, filter) match {
          case (None, exp) =>
            if (initial)
              QueryPlan.AllNodeScan(QueryPlan.LoadNode(nodePattern.maybeBinding.get)) -> exp
            else
              QueryPlan.LoadNode(nodePattern.maybeBinding.get) -> exp
          case (Some(idExp), exp) =>
            if (initial)
              QueryPlan.SpecificId(idExp, QueryPlan.LoadNode(nodePattern.maybeBinding.get)) -> exp
            else
              QueryPlan.Product(
                QueryPlan.LoadNode(nodePattern.maybeBinding.get),
                QueryPlan.Filter(
                  Pattern.Expression.BinOp(
                    Pattern.Source.NoSource,
                    Pattern.Operator.Equals,
                    Pattern.Expression.Apply(
                      Pattern.Source.NoSource,
                      Symbol("id"),
                      List(
                        Pattern.Expression.Ident(Pattern.Source.NoSource, nodePattern.maybeBinding.get, None),
                      ),
                      None,
                    ),
                    idExp,
                    None,
                  ),
                ),
              ) -> exp
        }
    }

  def planFilters(expression: Pattern.Expression): QueryPlan =
    QueryPlan.Filter(expression)

  sealed trait PatternChain

  object PatternChain {
    case class ChainCont(nodePattern: Cypher.NodePattern, edge: Cypher.EdgePattern, chain: PatternChain)
        extends PatternChain
    case class ChainEnd(nodePattern: Cypher.NodePattern) extends PatternChain
  }

  def rewriteGraphPattern(pattern: Cypher.GraphPattern): PatternChain = pattern.path match {
    case h :: t =>
      PatternChain.ChainCont(
        pattern.initial,
        h.edge,
        rewriteGraphPattern(Cypher.GraphPattern(Pattern.Source.NoSource, h.dest, t)),
      )
    case Nil => PatternChain.ChainEnd(pattern.initial)
  }

  def planPatterns(patterns: List[Cypher.GraphPattern], maybePredicate: Option[Pattern.Expression]): QueryPlan =
    maybePredicate match {
      case Some(exp) =>
        val (planForPatterns, remainingFilters) = patterns.foldLeft(QueryPlan.unit -> exp) {
          case ((plan, filter), pattern) =>
            val (newPlan, newFilter) = planFilteredPattern(rewriteGraphPattern(pattern), filter, true)
            QueryPlan.Product(plan, newPlan) -> newFilter
        }
        QueryPlan.Product(planForPatterns, planFilters(remainingFilters))
      case None =>
        patterns
          .map(p =>
            planFilteredPattern(
              rewriteGraphPattern(p),
              Pattern.Expression.mkAtomicLiteral(Pattern.Source.NoSource, Pattern.Value.True),
              true,
            )._1,
          )
          .reduce(QueryPlan.Product(_, _))
    }

  def planProjection(projection: Cypher.Projection): QueryPlan =
    QueryPlan.Project(projection.expression, projection.as.name)

  def planWithClause(clause: Cypher.WithClause): QueryPlan = {
    val projectionPlan = clause.bindings.foldLeft(QueryPlan.unit) { (plan, projection) =>
      QueryPlan.Product(plan, planProjection(projection))
    }
    clause.maybePredicate match {
      case Some(exp) => QueryPlan.Product(projectionPlan, planFilters(exp))
      case None => projectionPlan
    }
  }

  def planQueryPartInContext(nextPart: QueryPart, rest: List[QueryPart]): QueryPlan = nextPart match {
    case QueryPart.ReadingClausePart(readingClause) =>
      readingClause match {
        case ReadingClause.FromPatterns(_, patterns, maybePredicate) =>
          val readingPlan = planPatterns(patterns, maybePredicate)
          rest match {
            case h :: t => QueryPlan.Product(readingPlan, planQueryPartInContext(h, t))
            case Nil => readingPlan
          }
        case ReadingClause.FromUnwind(_, list, as) =>
          QueryPlan.Unwind(list, as, planQueryPartInContext(rest.head, rest.tail))
        case ReadingClause.FromProcedure(_, name, args, yields) =>
          QueryPlan.Product(QueryPlan.ProcedureCall(name, args, yields), planQueryPartInContext(rest.head, rest.tail))
        case _: ReadingClause.FromSubquery =>
          throw new QuinePatternUnimplementedException("Subqueries are not supported")
      }
    case QueryPart.WithClausePart(withClause) =>
      val withPlan = planWithClause(withClause)
      rest match {
        case h :: t => QueryPlan.Product(withPlan, planQueryPartInContext(h, t))
        case Nil => withPlan
      }
    case QueryPart.EffectPart(effect) =>
      rest match {
        case h :: t => QueryPlan.Product(QueryPlan.Effect(effect), planQueryPartInContext(h, t))
        case Nil => QueryPlan.Effect(effect)
      }
  }

  /** Generates a query plan based on the provided Cypher AST representation.
    *
    * @param cypherAst The abstract syntax tree (AST) representing the Cypher query.
    *                  It can be a union of queries or a single query, comprising either
    *                  multiple parts or a single sequence of parts.
    * @return A `QueryPlan` object that represents the computational steps necessary
    *         to execute the given Cypher query.
    */
  def generatePlan(cypherAst: Cypher.Query): QueryPlan = cypherAst match {
    case _: Cypher.Query.Union => throw new QuinePatternUnimplementedException("Union queries are not supported")
    case query: Cypher.Query.SingleQuery =>
      query match {
        case Cypher.Query.SingleQuery.MultipartQuery(_, queryParts, into) =>
          (queryParts, into) match {
            case (Nil, None) => QueryPlan.UnitPlan
            case (Nil, Some(spq)) => generatePlan(spq)
            case (h :: t, None) => planQueryPartInContext(h, t)
            case (h :: t, Some(spq)) => QueryPlan.Product(planQueryPartInContext(h, t), generatePlan(spq))
          }
        case Cypher.Query.SingleQuery.SinglepartQuery(_, queryParts, hasWildcard, isDistinct, bindings) =>
          queryParts match {
            case Nil => QueryPlan.UnitPlan
            case h :: t => planQueryPartInContext(h, t)
          }
      }
  }
}
