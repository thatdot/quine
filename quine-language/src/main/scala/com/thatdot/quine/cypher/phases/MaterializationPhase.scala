package com.thatdot.quine.cypher.phases

import cats.data.{IndexedState, OptionT, State}
import cats.implicits._

import com.thatdot.quine.cypher.ast.Query.SingleQuery
import com.thatdot.quine.cypher.ast._
import com.thatdot.quine.cypher.phases.MaterializationOutput.AggregationAccess
import com.thatdot.quine.cypher.phases.SymbolAnalysisModule.{PropertyAccess, SymbolTable, TypeEntry}
import com.thatdot.quine.language.ast.{BindingId, Expression}
import com.thatdot.quine.language.diagnostic.Diagnostic
import com.thatdot.quine.language.phases.CompilerPhase.{SimpleCompilerPhase, SimpleCompilerPhaseEffect}
import com.thatdot.quine.language.phases.TypeCheckingState
import com.thatdot.quine.language.types.Type
import com.thatdot.quine.language.types.Type.PrimitiveType

/** Materialization phase: rewrites field access expressions on graph element bindings
  * to synthetic identifier lookups, recording PropertyAccessEntry records in the symbol table.
  *
  * This phase runs after type checking. It uses resolved type information to determine
  * whether a binding is a graph element:
  * - NodeType bindings: field access is rewritten to a synthetic identifier lookup
  * - EdgeType bindings: field access is a compilation error (edge properties not supported)
  * - All other bindings: field access is left unchanged (runtime value access)
  */
private[phases] object MaterializationModule {

  case class MaterializationState(
    table: SymbolTable,
    typeEntries: List[TypeEntry],
    typeEnv: Map[Symbol, Type],
    currentFreshId: Int,
    diagnostics: List[Diagnostic],
    propertyAccesses: List[PropertyAccess],
    aggregationAccesses: List[AggregationAccess],
  )

  type MaterializationProgram[A] = State[MaterializationState, A]

  def pure[A](a: A): MaterializationProgram[A] = State.pure(a)
  def inspect[A](f: MaterializationState => A): MaterializationProgram[A] = State.inspect(f)
  def mod(f: MaterializationState => MaterializationState): MaterializationProgram[Unit] = State.modify(f)

  val freshId: MaterializationProgram[Int] =
    mod(st => st.copy(currentFreshId = st.currentFreshId + 1)) *> inspect(_.currentFreshId)

  def addDiagnostic(msg: String): MaterializationProgram[Unit] =
    mod(st => st.copy(diagnostics = Diagnostic.TypeCheckError(msg) :: st.diagnostics))

  /** Resolve a type through the type environment, following type variable bindings. */
  def resolveType(ty: Type, env: Map[Symbol, Type]): Type = ty match {
    case Type.TypeVariable(id, _) => env.get(id).map(resolveType(_, env)).getOrElse(ty)
    case other => other
  }

  /** Look up the resolved type of a binding by its integer ID. */
  def resolveBindingType(bindingId: Int): MaterializationProgram[Option[Type]] =
    inspect { st =>
      val id = BindingId(bindingId)
      st.typeEntries
        .find(_.identifier == id)
        .map(entry => resolveType(entry.ty, st.typeEnv))
    }

  def findPropertyAccess(onBinding: Int, property: Symbol): MaterializationProgram[Option[Int]] =
    inspect { st =>
      st.propertyAccesses.collectFirst {
        case PropertyAccess(synthId, b, p) if b == onBinding && p == property => synthId
      }
    }

  def addPropertyAccess(access: PropertyAccess): MaterializationProgram[Unit] =
    mod(st => st.copy(propertyAccesses = access :: st.propertyAccesses))

  def addAggregationAccess(access: AggregationAccess): MaterializationProgram[Unit] =
    mod(st => st.copy(aggregationAccesses = access :: st.aggregationAccesses))

  /** Known aggregation function names. */
  val aggregationFunctions: Set[Symbol] =
    Set(Symbol("count"), Symbol("sum"), Symbol("avg"), Symbol("min"), Symbol("max"), Symbol("collect"))

  /** Check if an expression is a top-level aggregation function call. */
  def isAggregation(expr: Expression): Boolean = expr match {
    case Expression.Apply(_, funcName, _, _) => aggregationFunctions.contains(funcName)
    case _ => false
  }

  /** Rewrite or reject a field access on a graph element binding, or leave it unchanged. */
  def rewriteGraphElementFieldAccess(
    fa: Expression.FieldAccess,
    materializedOf: Expression,
    bindingId: BindingId,
    bindingType: Type,
  ): MaterializationProgram[Expression] =
    bindingType match {
      case PrimitiveType.NodeType =>
        findPropertyAccess(bindingId.id, fa.fieldName).flatMap {
          case Some(existingSynthId) =>
            pure(Expression.Ident(fa.source, Right(BindingId(existingSynthId)), fa.ty): Expression)
          case None =>
            for {
              synthId <- freshId
              _ <- addPropertyAccess(PropertyAccess(synthId, bindingId.id, fa.fieldName))
            } yield Expression.Ident(fa.source, Right(BindingId(synthId)), fa.ty): Expression
        }
      case PrimitiveType.EdgeType =>
        addDiagnostic(
          s"Field access on edge binding is not supported: edge property '${fa.fieldName.name}' at ${fa.source}",
        ) *> pure(fa.copy(of = materializedOf): Expression)
      case _ =>
        pure(fa.copy(of = materializedOf): Expression)
    }

  def materializeFieldAccess(fa: Expression.FieldAccess): MaterializationProgram[Expression] =
    for {
      materializedOf <- materializeExpression(fa.of)
      result <- materializedOf match {
        case Expression.Ident(_, Right(bindingId), _) =>
          resolveBindingType(bindingId.id).flatMap {
            case Some(bindingType) =>
              rewriteGraphElementFieldAccess(fa, materializedOf, bindingId, bindingType)
            case None =>
              // No type information for this binding — leave as FieldAccess
              pure(fa.copy(of = materializedOf): Expression)
          }
        case _ =>
          pure(fa.copy(of = materializedOf): Expression)
      }
    } yield result

  def materializeExpression(expression: Expression): MaterializationProgram[Expression] =
    expression match {
      case fa: Expression.FieldAccess => materializeFieldAccess(fa)
      case bo: Expression.BinOp =>
        for {
          l <- materializeExpression(bo.lhs)
          r <- materializeExpression(bo.rhs)
        } yield bo.copy(lhs = l, rhs = r)
      case uo: Expression.UnaryOp =>
        for {
          e <- materializeExpression(uo.exp)
        } yield uo.copy(exp = e)
      case a: Expression.Apply =>
        for {
          args <- a.args.traverse(materializeExpression)
        } yield a.copy(args = args)
      case ll: Expression.ListLiteral =>
        for {
          exps <- ll.value.traverse(materializeExpression)
        } yield ll.copy(value = exps)
      case ml: Expression.MapLiteral =>
        for {
          entries <- ml.value.toList.traverse(p => materializeExpression(p._2).map(v => p._1 -> v))
        } yield ml.copy(value = entries.toMap)
      case caseBlock: Expression.CaseBlock =>
        for {
          cases <- caseBlock.cases.traverse { sc =>
            for {
              c <- materializeExpression(sc.condition)
              v <- materializeExpression(sc.value)
            } yield sc.copy(condition = c, value = v)
          }
          alt <- materializeExpression(caseBlock.alternative)
        } yield caseBlock.copy(cases = cases, alternative = alt)
      case isNull: Expression.IsNull =>
        for {
          e <- materializeExpression(isNull.of)
        } yield isNull.copy(of = e)
      case idx: Expression.IndexIntoArray =>
        for {
          o <- materializeExpression(idx.of)
          i <- materializeExpression(idx.index)
        } yield idx.copy(of = o, index = i)
      case synthesizeId: Expression.SynthesizeId =>
        for {
          args <- synthesizeId.from.traverse(materializeExpression)
        } yield synthesizeId.copy(from = args)
      case _: Expression.AtomicLiteral | _: Expression.Ident | _: Expression.Parameter | _: Expression.IdLookup =>
        pure(expression)
    }

  def materializeSortItem(sortItem: SortItem): MaterializationProgram[SortItem] =
    for {
      e <- materializeExpression(sortItem.expression)
    } yield sortItem.copy(expression = e)

  def materializeProjection(projection: Projection): MaterializationProgram[Projection] =
    for {
      e <- materializeExpression(projection.expression)
      result <-
        if (isAggregation(e))
          for {
            synthId <- freshId
            _ <- addAggregationAccess(AggregationAccess(synthId, e))
          } yield projection.copy(
            expression = Expression.Ident(projection.source, Right(BindingId(synthId)), None),
          )
        else
          pure(projection.copy(expression = e))
    } yield result

  def materializeNodePattern(np: NodePattern): MaterializationProgram[NodePattern] =
    for {
      props <- np.maybeProperties.traverse(materializeExpression)
    } yield np.copy(maybeProperties = props)

  def materializeConnection(conn: Connection): MaterializationProgram[Connection] =
    for {
      dest <- materializeNodePattern(conn.dest)
    } yield conn.copy(dest = dest)

  def materializeGraphPattern(gp: GraphPattern): MaterializationProgram[GraphPattern] =
    for {
      initial <- materializeNodePattern(gp.initial)
      path <- gp.path.traverse(materializeConnection)
    } yield gp.copy(initial = initial, path = path)

  def materializeEffect(effect: Effect): MaterializationProgram[Effect] = effect match {
    case foreach: Effect.Foreach =>
      for {
        e <- materializeExpression(foreach.in)
        effects <- foreach.effects.traverse(materializeEffect)
      } yield foreach.copy(in = e, effects = effects)
    case sp: Effect.SetProperty =>
      for {
        // SetProperty targets stay as FieldAccess (write targets are not rewritten)
        rewrittenOf <- materializeExpression(sp.property.of)
        v <- materializeExpression(sp.value)
      } yield sp.copy(property = sp.property.copy(of = rewrittenOf), value = v)
    case sps: Effect.SetProperties =>
      for {
        props <- materializeExpression(sps.properties)
      } yield sps.copy(properties = props)
    case _: Effect.SetLabel => pure(effect)
    case c: Effect.Create =>
      for {
        patterns <- c.patterns.traverse(materializeGraphPattern)
      } yield c.copy(patterns = patterns)
  }

  def materializeReadingClause(readingClause: ReadingClause): MaterializationProgram[ReadingClause] =
    readingClause match {
      case fp: ReadingClause.FromPatterns =>
        for {
          patterns <- fp.patterns.traverse(materializeGraphPattern)
          pred <- fp.maybePredicate.traverse(materializeExpression)
        } yield fp.copy(patterns = patterns, maybePredicate = pred)
      case fu: ReadingClause.FromUnwind =>
        for {
          list <- materializeExpression(fu.list)
        } yield fu.copy(list = list)
      case fp: ReadingClause.FromProcedure =>
        for {
          args <- fp.args.traverse(materializeExpression)
        } yield fp.copy(args = args)
      case fs: ReadingClause.FromSubquery =>
        for {
          sq <- materializeQuery(fs.subquery)
        } yield fs.copy(subquery = sq)
    }

  def materializeWithClause(withClause: WithClause): MaterializationProgram[WithClause] =
    for {
      bindings <- withClause.bindings.traverse(materializeProjection)
      pred <- withClause.maybePredicate.traverse(materializeExpression)
      orderBy <- withClause.orderBy.traverse(materializeSortItem)
      skip <- withClause.maybeSkip.traverse(materializeExpression)
      limit <- withClause.maybeLimit.traverse(materializeExpression)
    } yield withClause.copy(
      bindings = bindings,
      maybePredicate = pred,
      orderBy = orderBy,
      maybeSkip = skip,
      maybeLimit = limit,
    )

  def materializeQueryPart(queryPart: QueryPart): MaterializationProgram[QueryPart] =
    queryPart match {
      case rcp: QueryPart.ReadingClausePart =>
        for {
          rc <- materializeReadingClause(rcp.readingClause)
        } yield rcp.copy(readingClause = rc)
      case wcp: QueryPart.WithClausePart =>
        for {
          wc <- materializeWithClause(wcp.withClause)
        } yield wcp.copy(withClause = wc)
      case ep: QueryPart.EffectPart =>
        for {
          e <- materializeEffect(ep.effect)
        } yield ep.copy(effect = e)
    }

  def materializeSimpleQuery(
    query: SingleQuery.SinglepartQuery,
  ): MaterializationProgram[SingleQuery.SinglepartQuery] =
    for {
      parts <- query.queryParts.traverse(materializeQueryPart)
      bindings <- query.bindings.traverse(materializeProjection)
      orderBy <- query.orderBy.traverse(materializeSortItem)
      skip <- query.maybeSkip.traverse(materializeExpression)
      limit <- query.maybeLimit.traverse(materializeExpression)
    } yield query.copy(
      queryParts = parts,
      bindings = bindings,
      orderBy = orderBy,
      maybeSkip = skip,
      maybeLimit = limit,
    )

  def materializeSingleQuery(query: SingleQuery): MaterializationProgram[SingleQuery] = query match {
    case complex: SingleQuery.MultipartQuery =>
      for {
        parts <- complex.queryParts.traverse(materializeQueryPart)
        into <- materializeSimpleQuery(complex.into)
      } yield complex.copy(queryParts = parts, into = into)
    case simple: SingleQuery.SinglepartQuery =>
      materializeSimpleQuery(simple).widen[SingleQuery]
  }

  def materializeQuery(query: Query): MaterializationProgram[Query] = query match {
    case union: Query.Union =>
      for {
        lhs <- materializeQuery(union.lhs)
        rhs <- materializeSingleQuery(union.rhs)
      } yield union.copy(lhs = lhs, rhs = rhs)
    case single: Query.SingleQuery =>
      materializeSingleQuery(single).widen[Query]
  }
}

/** Data types produced by the materialization phase.
  *
  * These record how the materializer rewrote the AST — which property accesses
  * and aggregation expressions were replaced with synthetic binding references.
  * The query planner consumes these to wire up LocalProperty watches and Aggregate nodes.
  */
object MaterializationOutput {

  import cats.Monoid

  /** A materialized aggregation: an aggregation expression like `count(x)` is
    * rewritten to a reference to synthetic identifier `synthId`.
    * The original expression is preserved so the planner can extract the aggregation type.
    */
  case class AggregationAccess(synthId: Int, expression: Expression)

  /** Records which synthetic identifiers correspond to which aggregation computations. */
  case class AggregationAccessMapping(entries: List[AggregationAccess]) {
    def isEmpty: Boolean = entries.isEmpty
    def nonEmpty: Boolean = entries.nonEmpty
  }

  object AggregationAccessMapping {
    val empty: AggregationAccessMapping = AggregationAccessMapping(Nil)
  }

  implicit val AggregationAccessMappingMonoid: Monoid[AggregationAccessMapping] =
    new Monoid[AggregationAccessMapping] {
      override def empty: AggregationAccessMapping = AggregationAccessMapping.empty
      override def combine(x: AggregationAccessMapping, y: AggregationAccessMapping): AggregationAccessMapping =
        AggregationAccessMapping(x.entries ::: y.entries)
    }
}

object MaterializationPhase extends SimpleCompilerPhase[TypeCheckingState, Query, Query] {
  override def process(
    query: Query,
  ): SimpleCompilerPhaseEffect[TypeCheckingState, Query] = OptionT {
    IndexedState { tcState =>
      import MaterializationModule._
      import MaterializationOutput.AggregationAccessMapping
      import SymbolAnalysisModule.PropertyAccessMapping

      val initialState = MaterializationState(
        table = tcState.symbolTable,
        typeEntries = tcState.symbolTable.typeVars,
        typeEnv = tcState.typeEnv,
        currentFreshId = tcState.freshId,
        diagnostics = Nil,
        propertyAccesses = Nil,
        aggregationAccesses = Nil,
      )

      val (finalState, rewrittenQuery) =
        materializeQuery(query).run(initialState).value

      val resultState = tcState.copy(
        symbolTable = finalState.table,
        freshId = finalState.currentFreshId,
        diagnostics = finalState.diagnostics ::: tcState.diagnostics,
        propertyAccessMapping = PropertyAccessMapping(finalState.propertyAccesses),
        aggregationAccessMapping = AggregationAccessMapping(finalState.aggregationAccesses),
      )

      (resultState, Some(rewrittenQuery))
    }
  }
}
