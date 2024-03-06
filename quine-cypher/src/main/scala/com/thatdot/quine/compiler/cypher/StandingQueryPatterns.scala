package com.thatdot.quine.compiler.cypher

import java.util.regex.Pattern

import scala.collection.mutable

import cats.data.NonEmptyList
import cats.implicits.toTraverseOps
import com.typesafe.scalalogging.LazyLogging
import org.opencypher.v9_0.ast.ReturnItem
import org.opencypher.v9_0.expressions.{LabelExpression, PropertyKeyName, functions}
import org.opencypher.v9_0.util.AnonymousVariableNameGenerator
import org.opencypher.v9_0.util.helpers.NameDeduplicator
import org.opencypher.v9_0.{ast, expressions}

import com.thatdot.quine.compiler.cypher.QueryPart.IdFunc
import com.thatdot.quine.graph.GraphQueryPattern
import com.thatdot.quine.graph.cypher.Expr.toQuineValue
import com.thatdot.quine.graph.cypher.{CypherException, Expr, Query, SourceText, UserDefinedFunction}
import com.thatdot.quine.model.{QuineId, QuineIdProvider, QuineValue}

object StandingQueryPatterns extends LazyLogging {

  import GraphQueryPattern._

  /** Compile a small subset of Cypher statements into standing query graph
    * patterns
    *
    * @param statement the Cypher statements
    * @param paramsIdx query parameters in scope
    * @return the equivalent pattern
    */
  def compile(
    statement: ast.Statement,
    avng: AnonymousVariableNameGenerator,
    paramsIdx: ParametersIndex
  )(implicit
    source: SourceText,
    idProvider: QuineIdProvider
  ): GraphQueryPattern = {
    val (parts, whereOpt, hints, returnItems, distinct) = statement match {
      case ast.Query(
            ast.SingleQuery(
              Seq(
                ast.Match(false, expressions.Pattern(parts), hints, whereOpt),
                ast.Return(
                  distinct,
                  ast.ReturnItems(false, returnItems, _),
                  None,
                  None,
                  None,
                  emptySet,
                  _
                )
              )
            )
          ) if emptySet.isEmpty =>
        (parts, whereOpt, hints, returnItems, distinct)

      case e =>
        throw new CypherException.Compile(
          wrapping = "Wrong format for a standing query (expected `MATCH ... WHERE ... RETURN ...`)",
          position = Some(position(e.position))
        )
    }

    // Divide up the `WHERE` constraints into things that can be checked on the node vs. filters
    // over the result events
    val (propertyConstraints, idConstraints, otherConstraints) = partitionConstraints(whereOpt)

    // Accumulate up a set of node and edge patterns
    var nextId = 0
    val nodeIds = mutable.Map.empty[expressions.LogicalVariable, NodePatternId]
    val nodePatterns = List.newBuilder[NodePattern]
    val edgePatterns = Seq.newBuilder[EdgePattern]

    def addNodePattern(nodePattern: expressions.NodePattern): NodePatternId = {

      val nodePatternId = nodePattern.variable match {
        case None =>
          val id = NodePatternId(nextId)
          nextId += 1
          id

        case Some(v: expressions.LogicalVariable) if nodeIds.contains(v) => return nodeIds(v)

        case Some(v: expressions.LogicalVariable) =>
          val id = NodePatternId(nextId)
          nextId += 1
          nodeIds(v) = id
          id
      }

      val constraintProps: Map[Symbol, PropertyValuePattern] = nodePattern.properties match {
        case None =>
          Map.empty
        case Some(QuineValueLiteral(QuineValue.Map(props))) =>
          props.map { case (k, v) => Symbol(k) -> PropertyValuePattern.Value(v) }
        case _ =>
          throw CypherException.Compile(
            wrapping = "Invalid node constraint (expected a map literal)",
            position = Some(position(nodePattern.position))
          )
      }
      val whereProps = nodePattern.variable
        .flatMap(propertyConstraints.remove)
        .getOrElse(Map.empty)

      val idConstraint = nodePattern.variable.flatMap(idConstraints.remove)

      nodePatterns += NodePattern(
        nodePatternId,
        nodePattern.labelExpression.fold(Set.empty[Symbol])(le =>
          handleLabelExpression(le, Some(position(nodePattern.position)))
        ),
        idConstraint,
        whereProps ++ constraintProps
      )
      nodePatternId
    }

    /* Add to `nodePatterns` and `edgePatterns` builders.
     *
     * @return the rightmost node variable
     */
    def visitPatternElement(part: expressions.PatternElement): NodePatternId = part match {
      case expressions.RelationshipChain(elem, rel, rightNode) =>
        def relPos = Some(position(rel.position))

        // Raise informative errors on various unsupported Cypher features
        if (rel.variable.nonEmpty) {
          val msg = "Assigning edges to variables is not yet supported in standing query patterns"
          throw CypherException.Compile(msg, relPos)
        } else if (rel.length.nonEmpty) {
          throw CypherException.Compile("Variable length relationships are not yet supported", relPos)
        } else if (rel.properties.nonEmpty) {
          throw CypherException.Compile("Properties on edges are not yet supported", relPos)
        }

        val edgeLabel = rel.labelExpression match {
          case Some(LabelExpression.Leaf(name)) => Symbol(name.name)
          case Some(badLabels) =>
            //val badLabels = labels.map(x => ":" + x.name).mkString(", ")
            throw CypherException.Compile(
              s"Edges in standing query patterns must have exactly one label (got ${badLabels.asCanonicalStringVal})",
              relPos
            )
          case None =>
            throw CypherException.Compile(
              s"Must include exactly one label in a standing query pattern (got none)",
              relPos
            )
        }
        val leftNodeId = visitPatternElement(elem)
        val rightNodeId = addNodePattern(rightNode)

        rel.direction match {
          case expressions.SemanticDirection.OUTGOING =>
            edgePatterns += EdgePattern(leftNodeId, rightNodeId, isDirected = true, edgeLabel)
          case expressions.SemanticDirection.INCOMING =>
            edgePatterns += EdgePattern(rightNodeId, leftNodeId, isDirected = true, edgeLabel)
          case _ =>
            throw CypherException.Compile(
              wrapping = "Edge in standing queries must specify a direction",
              position = relPos
            )
        }

        rightNodeId

      case n: expressions.NodePattern => addNodePattern(n)

      case other =>
        throw CypherException.Compile(
          wrapping = s"Unexpected pattern: $other",
          position = Some(position(other.position))
        )
    }

    parts.foreach {
      case expressions.EveryPath(pe) =>
        visitPatternElement(pe)

      case pat: expressions.NamedPatternPart =>
        throw CypherException.Compile(
          wrapping = "Named patterns are not supported in standing queries",
          position = Some(position(pat.position))
        )

      case pat: expressions.ShortestPaths =>
        throw CypherException.Compile(
          wrapping = "`shortestPath` planning in graph patterns is not supported",
          position = Some(position(pat.position))
        )
    }

    // These are the properties and IDs the standing query will need to be tracking
    val propertiesWatched =
      mutable.Map.empty[(expressions.LogicalVariable, Option[expressions.PropertyKeyName]), expressions.LogicalVariable]
    val idsWatched = mutable.Map.empty[(Boolean, expressions.LogicalVariable), expressions.LogicalVariable]

    // These are all of the columns we will be returning
    val toReturn: Seq[(Symbol, Expr)] = Seq.concat[ReturnItem](returnItems).map { (item: ast.ReturnItem) =>
      val colName = Symbol(item.name)
      val expr = compileStandingExpression(
        item.expression,
        paramsIdx,
        variableNamer = (subExpr: expressions.Expression) => {
          if (subExpr == item.expression) colName.name
          else
            NameDeduplicator.removeGeneratedNamesAndParams(avng.nextName)
        },
        nodeIds.keySet,
        propertiesWatched,
        idsWatched,
        "Returned column",
        avng
      )
      colName -> expr
    }

    // Construct the filter query (if there is one)
    val filterCond: Option[Expr] = if (otherConstraints.nonEmpty) {
      val conjuncts = otherConstraints.map { (otherConstraint: expressions.Expression) =>
        compileStandingExpression(
          otherConstraint,
          paramsIdx,
          variableNamer = (subExpr: expressions.Expression) => {
            NameDeduplicator.removeGeneratedNamesAndParams(avng.nextName)
          },
          nodeIds.keySet,
          propertiesWatched,
          idsWatched,
          "Filter condition",
          avng
        )
      }
      Some(if (conjuncts.length == 1) conjuncts.head else Expr.And(conjuncts.toVector))
    } else {
      None
    }

    // Build up the list of all columns to watch
    val toExtract: List[ReturnColumn] = {
      val builder = List.newBuilder[ReturnColumn]
      for (((node, whichProp), aliasedAs) <- propertiesWatched) // NB this may enumerate keys in an arbitrary order
        whichProp match {
          case Some(key) => builder += ReturnColumn.Property(nodeIds(node), Symbol(key.name), Symbol(aliasedAs.name))
          case None => builder += ReturnColumn.AllProperties(nodeIds(node), Symbol(aliasedAs.name))
        }
      for (((formatAsString, node), aliasedAs) <- idsWatched)
        builder += ReturnColumn.Id(nodeIds(node), formatAsString, Symbol(aliasedAs.name))
      builder.result()
    }

    // Decide what node in the query should be the starting point
    val rootId: NodePatternId = (hints, toExtract) match {
      // TODO: use the label
      // explicitly specify the starting point with a scan hint
      case (Seq(ast.UsingScanHint(nodeVar, label @ _)), _) =>
        nodeIds.getOrElse(
          nodeVar,
          throw CypherException.Compile(
            wrapping = s"Using hint refers to undefined variable `${nodeVar.name}`",
            position = Some(position(nodeVar.position))
          )
        )

      // legacy style `match ... return n`
      case (Seq(), Seq(ReturnColumn.Id(node, _, _))) => node

      // as a default fallback: use the first node in the pattern as the starting point
      case _ => NodePatternId(0)
    }

    /* Optimization: if the columns being watched are exactly the ones being returned, we don't
     * need to populate `toReturn` - we just need to re-order the columns being extracted
     */
    if (toExtract.length == toReturn.length && toReturn.forall(ve => Expr.Variable(ve._1) == ve._2)) {
      GraphQueryPattern(
        NonEmptyList.fromListUnsafe(nodePatterns.result()),
        edgePatterns.result(),
        rootId,
        toExtract.sortBy(col => toReturn.indexWhere(_._1 == col.aliasedAs)),
        filterCond,
        Nil,
        distinct
      )
    } else {
      GraphQueryPattern(
        // We know this will always be non-empty at this point?
        NonEmptyList.fromListUnsafe(nodePatterns.result()),
        edgePatterns.result(),
        rootId,
        toExtract,
        filterCond,
        toReturn,
        distinct
      )
    }
  }

  /** Compile and rewrite a Cypher expression AST to capture all property access and ID queries on
    * variables and add them to the map of tracked variables.
    *
    * @param expr input AST to compile and rewrite
    * @param paramsIdx query parameters in scope
    * @param variableNamer how to come up with names for variables
    * @param propertiesWatched what properties are already being tracked?
    * @param idsWatched what IDs are already being tracked?
    * @param contextName human-readable description of wwhat this expression represents in the query
    * @return re-written AST
    */
  @throws[CypherException.Compile]
  def compileStandingExpression(
    expr: expressions.Expression,
    paramsIdx: ParametersIndex,
    variableNamer: expressions.Expression => String,
    nodesInScope: collection.Set[expressions.LogicalVariable],
    propertiesWatched: mutable.Map[
      (expressions.LogicalVariable, Option[expressions.PropertyKeyName]),
      expressions.LogicalVariable
    ],
    idsWatched: mutable.Map[(Boolean, expressions.LogicalVariable), expressions.LogicalVariable],
    contextName: String,
    avng: AnonymousVariableNameGenerator
  )(implicit
    source: SourceText
  ): Expr = {

    /* We actually compile the expression _twice_. This is done strictly for the sake of good error
     * messages:
     *
     *   - we want the re-writing step to include position information when a variable doesn't
     *     occur under an `id(..)` or property access (so we have to use the openCypher AST)
     *
     *   - we also want other expression compilation errors to use the initial user-written AST
     *     so that errors don't refere to variables that the user never manually wrot
     *
     * Our solution is to do one extra compilation pass on the initial AST, just to try to catch
     * all the errors and report them with good messages. Then, we rewrite the initial AST and
     * compile the output of rewriting too. There is still a risk that the second compilation phase
     * will fail to where the first succeeded. If that happens, the user may see an error that is
     * more confusing because it mentions re-written variables. Although I can't come up with an
     * example of how this might happen, it isn't inconceivable.
     */

    // First compilation pass
    val initialScope =
      nodesInScope.foldLeft(QueryScopeInfo.empty)((scope, colLv) => scope.addColumn(logicalVariable2Symbol(colLv))._1)
    Expression.compileM(expr, avng).run(paramsIdx, source, initialScope) match {
      case Left(err) => throw err
      case Right(_) => // do nothing - the compilation output we use is from the second pass
    }

    import org.opencypher.v9_0.util.Rewritable._
    import org.opencypher.v9_0.util.Rewriter
    import org.opencypher.v9_0.util.{bottomUp, topDown}

    // Rewrite the AST and validate uses of node variables
    val rewritten = expr
      .endoRewrite(topDown(Rewriter.lift {

        // Rewrite `nodeVariable.someProperty` to a fresh variable
        case propAccess @ expressions.Property(nodeVariable: expressions.LogicalVariable, propKeyName) =>
          propertiesWatched.getOrElseUpdate(
            nodeVariable -> Some(propKeyName),
            expressions.Variable(variableNamer(propAccess))(propAccess.position)
          )

        case propertiesFunc @ expressions.FunctionInvocation(
              _,
              _,
              _,
              Vector(nodeVariable: expressions.LogicalVariable)
            ) if propertiesFunc.function == functions.Properties =>
          propertiesWatched.getOrElseUpdate(
            nodeVariable -> None,
            expressions.Variable(variableNamer(propertiesFunc))(propertiesFunc.position)
          )

        // Rewrite `id(nodeVariable)` to a fresh variable
        case idFunc @ expressions.FunctionInvocation(
              _,
              _,
              _,
              Vector(nodeVariable: expressions.LogicalVariable)
            ) if idFunc.function == functions.Id =>
          idsWatched.getOrElseUpdate(
            false -> nodeVariable,
            expressions.Variable(variableNamer(idFunc))(idFunc.position)
          )

        // Rewrite `strId(nodeVariable)` to a fresh variable
        case idFunc @ expressions.FunctionInvocation(
              _,
              expressions.FunctionName(functionName),
              false,
              Vector(nodeVariable: expressions.LogicalVariable)
            ) if functionName.toLowerCase == CypherStrId.name.toLowerCase =>
          idsWatched.getOrElseUpdate(
            true -> nodeVariable,
            expressions.Variable(variableNamer(idFunc))(idFunc.position)
          )

        // Raise an error for any other variables (which must not have matched the preceding cases)
        case variable: expressions.LogicalVariable =>
          throw new CypherException.Compile(
            s"Invalid use of node variable `${variable.name}` (in standing queries, node variables can only reference constant properties or IDs)",
            Some(position(variable.position))
          )
      }))
      .endoRewrite(bottomUp(Rewriter.lift(resolveFunctions.rewriteFunc)))

    // Second compilation pass
    val rewrittenScope = (propertiesWatched.values.toSet | idsWatched.values.toSet)
      .foldLeft(QueryScopeInfo.empty)((scope, col) => scope.addColumn(logicalVariable2Symbol(col))._1)
    Expression.compileM(rewritten, avng).run(paramsIdx, source, rewrittenScope) match {
      case Left(err) =>
        throw err
      case Right(WithQuery(pureExpr, Query.Unit(_))) =>
        pureExpr
      case Right(_) =>
        throw new CypherException.Compile(
          wrapping = s"$contextName is not a pure expression - it requires querying the graph",
          position = Some(position(expr.position))
        )
    }
  }

  /** Extractor to get a literal from an expression
    *
    * @note we can't give this type `expression.Literal => QuineValue` because
    * some list and map literals are encoded deeper in the AST
    *
    * @param literal expression that may be just a literal
    * @return a literal value or [[None]] if it isn't one
    */
  object QuineValueLiteral {
    def unapply(literal: expressions.Expression): Option[QuineValue] =
      literal match {
        case i: expressions.IntegerLiteral => Some(QuineValue.Integer(i.value))
        case d: expressions.DoubleLiteral => Some(QuineValue.Floating(d.value))
        case expressions.StringLiteral(str) => Some(QuineValue.Str(str))
        case expressions.Null() => Some(QuineValue.Null)
        case expressions.True() => Some(QuineValue.True)
        case expressions.False() => Some(QuineValue.False)
        case expressions.ListLiteral(exps) =>
          exps.toVector.traverse(unapply).map(QuineValue.List)
        case expressions.MapExpression(expItems) =>
          expItems.toList.traverse(p => unapply(p._2).map(v => p._1.name -> v)).map(QuineValue.Map(_))
        case _ => None
      }
  }

  /** Decompose the where constraint into property and ID constraints (and throw an
    * exception on anything else)
    *
    * @param whereOpt where expression to decompose
    * @return (property constraints, id constraints, remaining constraints)
    */
  @throws[CypherException]
  def partitionConstraints(
    whereOpt: Option[ast.Where]
  )(implicit
    source: SourceText,
    idProvider: QuineIdProvider
  ): (
    mutable.Map[expressions.LogicalVariable, Map[Symbol, PropertyValuePattern]],
    mutable.Map[expressions.LogicalVariable, QuineId],
    mutable.ListBuffer[expressions.Expression]
  ) = {

    /* Constraints of the form
     *
     *   - `nodeVariable.someProperty = <someLiteral>`
     *   - `nodeVariable.someProperty <> <someLiteral>`
     *   - `EXISTS(nodeVariable.someProperty)` or `nodeVariable.someProperty IS NOT NULL`
     *   - `NOT EXISTS(nodeVariable.someProperty)` or `nodeVariable.someProperty IS NULL`
     *   - `nodeVariable.someProperty =~ "stringPattern`
     *   - TODO QU-1453 will add `nodeVariable.someProperty IN [...]`
     */
    val propertyConstraints =
      mutable.Map.empty[expressions.LogicalVariable, Map[Symbol, PropertyValuePattern]]

    /* Constraints of the form
     *
     *   - `id(nodeVariable) = <someLiteral>`
     *   - `strId(nodeVariable) = <someLiteral>`
     *
     * TODO: add support for `strId(nodeVariable)` elsewhere in standing queries!
     * NB this may match more than the actual return values of id/strId -- for example
     * if `id(n) = 100` then a constraint WHERE `id(n) = "100"` will probably match,
     * as will `WHERE strId(n) = 100`
     */
    val idConstraints =
      mutable.Map.empty[expressions.LogicalVariable, QuineId]

    // Constraints which didn't fit any of the preceding categories
    val other = mutable.ListBuffer.empty[expressions.Expression]

    object PropertyConstraint {
      def unapply(expr: expressions.Expression): Option[(expressions.LogicalVariable, String, PropertyValuePattern)] =
        Some(expr match {
          // Constraints of the form `nodeVariable.someProperty = <someLiteral>`
          case expressions.Equals(
                expressions.Property(v: expressions.LogicalVariable, expressions.PropertyKeyName(keyName)),
                QuineValueLiteral(literalArg)
              ) =>
            (v, keyName, PropertyValuePattern.Value(literalArg))

          // Constraints of the form `nodeVariable.someProperty <> <someLiteral>`
          case expressions.NotEquals(
                expressions
                  .Property(v: expressions.LogicalVariable, expressions.PropertyKeyName(keyName)),
                QuineValueLiteral(literalArg)
              ) =>
            (v, keyName, PropertyValuePattern.AnyValueExcept(literalArg))

          // Constraints of the form `nodeVariable.someProperty =~ "stringPattern"`
          case expressions.RegexMatch(
                expressions
                  .Property(v: expressions.LogicalVariable, expressions.PropertyKeyName(keyName)),
                expressions.StringLiteral(rePattern)
              ) =>
            (v, keyName, PropertyValuePattern.RegexMatch(Pattern.compile(rePattern)))

          // Constraints of the form `EXISTS(nodeVariable.someProperty)`
          case f @ expressions.FunctionInvocation(
                _,
                _,
                false,
                Vector(
                  expressions
                    .Property(v: expressions.LogicalVariable, expressions.PropertyKeyName(keyName))
                )
              ) if f.function == functions.Exists =>
            (v, keyName, PropertyValuePattern.AnyValue)

          // Constraints of the form `nodeVariable.someProperty IS NOT NULL`
          case expressions.IsNotNull(
                expressions.Property(
                  v: expressions.LogicalVariable,
                  expressions.PropertyKeyName(keyName)
                )
              ) =>
            (v, keyName, PropertyValuePattern.AnyValue)

          // Constraints of the form `NOT EXISTS(nodeVariable.someProperty)`
          case expressions.Not(
                f @ expressions.FunctionInvocation(
                  _,
                  _,
                  false,
                  Vector(
                    expressions
                      .Property(v: expressions.LogicalVariable, expressions.PropertyKeyName(keyName))
                  )
                )
              ) if f.function == functions.Exists =>
            (v, keyName, PropertyValuePattern.NoValue)

          // Constraints of the form `nodeVariable.someProperty IS NULL`
          case expressions.IsNull(
                expressions.Property(
                  v: expressions.LogicalVariable,
                  expressions.PropertyKeyName(keyName)
                )
              ) =>
            (v, keyName, PropertyValuePattern.NoValue)

          case _ => return None
        })
    }

    object QuineIdConstant {
      def unapply(value: QuineValue): Option[QuineId] =
        idProvider.valueToQid(value).orElse {
          value match {
            case QuineValue.Str(strId) => idProvider.qidFromPrettyString(strId).toOption
            case _ => None
          }
        }
    }

    /** Extractor for deterministic invocations of id predicate functions (ie, idFrom and locIdFrom)
      *
      * Current implementation is limited to invocations with only literal arguments
      *
      * @example idFrom(100, 200, 201) should match and return the equivalent QuineId
      * @example idFrom(rand()) should not match (nondeterministic)
      * @example locIdFrom("part1") should not match (nondeterministic)
      * @example locIdFrom("part1", 100) should match
      * @example idFrom(1+2) should not match (non-literal argument) TODO support pure but non-literal expressions like this
      */
    object FunctionBasedIdPredicate {
      def unapply(expr: expressions.Expression): Option[QuineId] = {
        // Ensure there is the correct number of arguments to make the function deterministic
        val funcAndArgs: Option[(UserDefinedFunction, Seq[expressions.Expression])] = expr match {
          // IdFrom
          case expressions.FunctionInvocation(
                _,
                expressions.FunctionName(functionName),
                false,
                args
              ) if functionName.toLowerCase == CypherIdFrom.name.toLowerCase =>
            Some(CypherIdFrom -> args.toVector)
          // locIdFrom with at least 2 args
          case expressions.FunctionInvocation(
                _,
                expressions.FunctionName(functionName),
                false,
                args
              ) if functionName.toLowerCase == CypherLocIdFrom.name.toLowerCase && args.length >= 2 =>
            Some(CypherLocIdFrom -> args.toVector)
          case _ => None
        }
        // ensure all arguments to the function are QuineValue-compatible literals
        val funcWithLiteralArgs = funcAndArgs.flatMap { case (udf, args) =>
          val quineValueArgs = args.collect { case QuineValueLiteral(qv) => qv }
          if (quineValueArgs.length == args.length) Some(udf -> quineValueArgs)
          else None
        }
        // [statically] compute the actual id represented by invoking the relevant UDF
        val returnValue = funcWithLiteralArgs.map { case (udf, qvArgs) =>
          val cypherValueArgs = qvArgs.map(Expr.fromQuineValue).toVector
          udf.call(cypherValueArgs)
        }
        // Convert the computed cypher ID value to a QuineId (must be kept in sync with [[Expr.toQuineValue]])
        returnValue.flatMap { result =>
          val parsedViaIdProvider = idProvider.valueToQid(toQuineValue(result))

          // NB the below cases indicate a bad return value from (ie, a bug in) [[CypherIdFrom]] or [[CypherLocIdFrom]]
          // or somewhere the QuineValue<->cypher.Value<->QuineId<->customIdType conversions are losing information
          parsedViaIdProvider.orElse {
            result match {
              case Expr.Bytes(qidBytes, representsId @ false) =>
                logger.info(
                  "Precomputing ID predicate in Standing Query returned bytes not tagged as an ID. Using as an ID anyways"
                )
                Some(QuineId(qidBytes))
              case Expr.Bytes(qidBytes, representsId @ true) =>
                logger.debug(
                  """Precomputing ID predicate in Standing Query returned ID-tagged bytes, but idProvider didn't
                    |recognize the value as a QuineId. This is most likely a bug in toQuineValue or
                    |idProvider.valueToQid""".stripMargin.replace('\n', ' ')
                )
                Some(QuineId(qidBytes))
              case cantBeUsedAsId =>
                logger.warn(
                  s"""ID predicates in Standing Queries must use functions returning IDs (eg idFrom, locIdFrom). Precomputing
                     |the ID predicate produced a constraint with type: ${cantBeUsedAsId.typ}""".stripMargin
                    .replace('\n', ' ')
                )
                None
            }
          }
        }
      }
    }

    object IdConstraint {
      def unapply(expr: expressions.Expression): Option[(expressions.LogicalVariable, QuineId)] =
        Some(expr match {
          case expressions.Equals(IdFunc(n), QuineValueLiteral(QuineIdConstant(qid))) => (n, qid)
          case expressions.Equals(QuineValueLiteral(QuineIdConstant(qid)), IdFunc(n)) => (n, qid)
          case expressions.Equals(IdFunc(n), FunctionBasedIdPredicate(qid)) => (n, qid)
          case expressions.Equals(FunctionBasedIdPredicate(qid), IdFunc(n)) => (n, qid)
          case _ => return None
        })
    }

    // Visit a top-level predicate (meaning a predicate which is a top-level conjunct)
    def visitWhereExpr(constraint: expressions.Expression): Unit = constraint match {
      case expressions.And(lhs, rhs) =>
        visitWhereExpr(lhs)
        visitWhereExpr(rhs)

      case expressions.Ands(conjs) =>
        conjs.foreach(visitWhereExpr)

      case PropertyConstraint(v, propKey, propConstraint)
          if propertyConstraints.getOrElse(v, Map.empty).get(Symbol(propKey)).forall(_ == propConstraint) =>
        val previousConstraints = propertyConstraints.getOrElse(v, Map.empty)
        propertyConstraints(v) = previousConstraints + (Symbol(propKey) -> propConstraint)

      case IdConstraint(v, qidConstraint) if idConstraints.get(v).forall(_ == qidConstraint) =>
        idConstraints(v) = qidConstraint

      case constraint =>
        other += constraint
    }

    whereOpt.foreach(w => visitWhereExpr(w.expression))
    (propertyConstraints, idConstraints, other)
  }
}
