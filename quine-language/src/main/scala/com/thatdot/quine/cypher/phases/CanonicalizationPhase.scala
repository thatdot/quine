package com.thatdot.quine.cypher.phases

import cats.data.{IndexedState, OptionT, State}
import cats.implicits._

import com.thatdot.quine.cypher.ast.Query.SingleQuery
import com.thatdot.quine.cypher.ast._
import com.thatdot.quine.cypher.phases.SymbolAnalysisModule.{SymbolTable, SymbolTableEntry}
import com.thatdot.quine.language.ast.{Expression, QuineIdentifier}
import com.thatdot.quine.language.phases.CompilerPhase.{SimpleCompilerPhase, SimpleCompilerPhaseEffect}

/** Canonicalization phase: rewrites field access expressions on graph element bindings
  * to synthetic identifier lookups, recording PropertyAccessEntry records in the symbol table.
  *
  * This phase runs after symbol analysis. It uses the same `isGraphElementBinding` heuristic
  * (checking for NodeEntry/EdgeEntry in the symbol table) that symbol analysis previously used.
  * In a later step, this will be replaced with type-based decisions from the type checker.
  */
private[phases] object CanonicalizationModule {

  case class CanonState(
    table: SymbolTable,
    currentFreshId: Int,
  )

  type CanonProgram[A] = State[CanonState, A]

  def pure[A](a: A): CanonProgram[A] = State.pure(a)
  def inspect[A](f: CanonState => A): CanonProgram[A] = State.inspect(f)
  def mod(f: CanonState => CanonState): CanonProgram[Unit] = State.modify(f)

  val freshId: CanonProgram[Int] =
    mod(st => st.copy(currentFreshId = st.currentFreshId + 1)) *> inspect(_.currentFreshId)

  def isGraphElementBinding(identifier: Int): CanonProgram[Boolean] =
    inspect { st =>
      st.table.references.exists { entry =>
        entry.identifier == identifier && (entry.isInstanceOf[SymbolTableEntry.NodeEntry] ||
        entry.isInstanceOf[SymbolTableEntry.EdgeEntry])
      }
    }

  def findPropertyAccessEntry(onBinding: Int, property: Symbol): CanonProgram[Option[Int]] =
    inspect { st =>
      st.table.references.collectFirst {
        case SymbolTableEntry.PropertyAccessEntry(_, synthId, b, p) if b == onBinding && p == property =>
          synthId
      }
    }

  def addEntry(entry: SymbolTableEntry.PropertyAccessEntry): CanonProgram[Unit] =
    mod(st => st.copy(table = st.table.copy(references = entry :: st.table.references)))

  def canonicalizeFieldAccess(fa: Expression.FieldAccess): CanonProgram[Expression] =
    for {
      canonOf <- canonicalizeExpression(fa.of)
      result <- canonOf match {
        case Expression.Ident(_, Right(quineId), _) =>
          isGraphElementBinding(quineId.name).flatMap { isGraphElement =>
            if (isGraphElement) {
              findPropertyAccessEntry(quineId.name, fa.fieldName).flatMap {
                case Some(existingSynthId) =>
                  pure(Expression.Ident(fa.source, Right(QuineIdentifier(existingSynthId)), fa.ty): Expression)
                case None =>
                  for {
                    synthId <- freshId
                    _ <- addEntry(
                      SymbolTableEntry.PropertyAccessEntry(fa.source, synthId, quineId.name, fa.fieldName),
                    )
                  } yield Expression.Ident(fa.source, Right(QuineIdentifier(synthId)), fa.ty): Expression
              }
            } else {
              pure(fa.copy(of = canonOf): Expression)
            }
          }
        case _ =>
          pure(fa.copy(of = canonOf): Expression)
      }
    } yield result

  def canonicalizeExpression(expression: Expression): CanonProgram[Expression] =
    expression match {
      case fa: Expression.FieldAccess => canonicalizeFieldAccess(fa)
      case bo: Expression.BinOp =>
        for {
          l <- canonicalizeExpression(bo.lhs)
          r <- canonicalizeExpression(bo.rhs)
        } yield bo.copy(lhs = l, rhs = r)
      case uo: Expression.UnaryOp =>
        for {
          e <- canonicalizeExpression(uo.exp)
        } yield uo.copy(exp = e)
      case a: Expression.Apply =>
        for {
          args <- a.args.traverse(canonicalizeExpression)
        } yield a.copy(args = args)
      case ll: Expression.ListLiteral =>
        for {
          exps <- ll.value.traverse(canonicalizeExpression)
        } yield ll.copy(value = exps)
      case ml: Expression.MapLiteral =>
        for {
          entries <- ml.value.toList.traverse(p => canonicalizeExpression(p._2).map(v => p._1 -> v))
        } yield ml.copy(value = entries.toMap)
      case caseBlock: Expression.CaseBlock =>
        for {
          cases <- caseBlock.cases.traverse { sc =>
            for {
              c <- canonicalizeExpression(sc.condition)
              v <- canonicalizeExpression(sc.value)
            } yield sc.copy(condition = c, value = v)
          }
          alt <- canonicalizeExpression(caseBlock.alternative)
        } yield caseBlock.copy(cases = cases, alternative = alt)
      case isNull: Expression.IsNull =>
        for {
          e <- canonicalizeExpression(isNull.of)
        } yield isNull.copy(of = e)
      case idx: Expression.IndexIntoArray =>
        for {
          o <- canonicalizeExpression(idx.of)
          i <- canonicalizeExpression(idx.index)
        } yield idx.copy(of = o, index = i)
      case synthesizeId: Expression.SynthesizeId =>
        for {
          args <- synthesizeId.from.traverse(canonicalizeExpression)
        } yield synthesizeId.copy(from = args)
      case _: Expression.AtomicLiteral | _: Expression.Ident | _: Expression.Parameter | _: Expression.IdLookup =>
        pure(expression)
    }

  def canonicalizeSortItem(sortItem: SortItem): CanonProgram[SortItem] =
    for {
      e <- canonicalizeExpression(sortItem.expression)
    } yield sortItem.copy(expression = e)

  def canonicalizeProjection(projection: Projection): CanonProgram[Projection] =
    for {
      e <- canonicalizeExpression(projection.expression)
    } yield projection.copy(expression = e)

  def canonicalizeNodePattern(np: NodePattern): CanonProgram[NodePattern] =
    for {
      props <- np.maybeProperties.traverse(canonicalizeExpression)
    } yield np.copy(maybeProperties = props)

  def canonicalizeConnection(conn: Connection): CanonProgram[Connection] =
    for {
      dest <- canonicalizeNodePattern(conn.dest)
    } yield conn.copy(dest = dest)

  def canonicalizeGraphPattern(gp: GraphPattern): CanonProgram[GraphPattern] =
    for {
      initial <- canonicalizeNodePattern(gp.initial)
      path <- gp.path.traverse(canonicalizeConnection)
    } yield gp.copy(initial = initial, path = path)

  def canonicalizeEffect(effect: Effect): CanonProgram[Effect] = effect match {
    case foreach: Effect.Foreach =>
      for {
        e <- canonicalizeExpression(foreach.in)
        effects <- foreach.effects.traverse(canonicalizeEffect)
      } yield foreach.copy(in = e, effects = effects)
    case sp: Effect.SetProperty =>
      for {
        // SetProperty targets stay as FieldAccess (write targets are not rewritten)
        rewrittenOf <- canonicalizeExpression(sp.property.of)
        v <- canonicalizeExpression(sp.value)
      } yield sp.copy(property = sp.property.copy(of = rewrittenOf), value = v)
    case sps: Effect.SetProperties =>
      for {
        props <- canonicalizeExpression(sps.properties)
      } yield sps.copy(properties = props)
    case _: Effect.SetLabel => pure(effect)
    case c: Effect.Create =>
      for {
        patterns <- c.patterns.traverse(canonicalizeGraphPattern)
      } yield c.copy(patterns = patterns)
  }

  def canonicalizeReadingClause(readingClause: ReadingClause): CanonProgram[ReadingClause] =
    readingClause match {
      case fp: ReadingClause.FromPatterns =>
        for {
          patterns <- fp.patterns.traverse(canonicalizeGraphPattern)
          pred <- fp.maybePredicate.traverse(canonicalizeExpression)
        } yield fp.copy(patterns = patterns, maybePredicate = pred)
      case fu: ReadingClause.FromUnwind =>
        for {
          list <- canonicalizeExpression(fu.list)
        } yield fu.copy(list = list)
      case fp: ReadingClause.FromProcedure =>
        for {
          args <- fp.args.traverse(canonicalizeExpression)
        } yield fp.copy(args = args)
      case fs: ReadingClause.FromSubquery =>
        for {
          sq <- canonicalizeQuery(fs.subquery)
        } yield fs.copy(subquery = sq)
    }

  def canonicalizeWithClause(withClause: WithClause): CanonProgram[WithClause] =
    for {
      bindings <- withClause.bindings.traverse(canonicalizeProjection)
      pred <- withClause.maybePredicate.traverse(canonicalizeExpression)
      orderBy <- withClause.orderBy.traverse(canonicalizeSortItem)
      skip <- withClause.maybeSkip.traverse(canonicalizeExpression)
      limit <- withClause.maybeLimit.traverse(canonicalizeExpression)
    } yield withClause.copy(
      bindings = bindings,
      maybePredicate = pred,
      orderBy = orderBy,
      maybeSkip = skip,
      maybeLimit = limit,
    )

  def canonicalizeQueryPart(queryPart: QueryPart): CanonProgram[QueryPart] =
    queryPart match {
      case rcp: QueryPart.ReadingClausePart =>
        for {
          rc <- canonicalizeReadingClause(rcp.readingClause)
        } yield rcp.copy(readingClause = rc)
      case wcp: QueryPart.WithClausePart =>
        for {
          wc <- canonicalizeWithClause(wcp.withClause)
        } yield wcp.copy(withClause = wc)
      case ep: QueryPart.EffectPart =>
        for {
          e <- canonicalizeEffect(ep.effect)
        } yield ep.copy(effect = e)
    }

  def canonicalizeSimpleQuery(
    query: SingleQuery.SinglepartQuery,
  ): CanonProgram[SingleQuery.SinglepartQuery] =
    for {
      parts <- query.queryParts.traverse(canonicalizeQueryPart)
      bindings <- query.bindings.traverse(canonicalizeProjection)
      orderBy <- query.orderBy.traverse(canonicalizeSortItem)
      skip <- query.maybeSkip.traverse(canonicalizeExpression)
      limit <- query.maybeLimit.traverse(canonicalizeExpression)
    } yield query.copy(
      queryParts = parts,
      bindings = bindings,
      orderBy = orderBy,
      maybeSkip = skip,
      maybeLimit = limit,
    )

  def canonicalizeSingleQuery(query: SingleQuery): CanonProgram[SingleQuery] = query match {
    case complex: SingleQuery.MultipartQuery =>
      for {
        parts <- complex.queryParts.traverse(canonicalizeQueryPart)
        into <- canonicalizeSimpleQuery(complex.into)
      } yield complex.copy(queryParts = parts, into = into)
    case simple: SingleQuery.SinglepartQuery =>
      canonicalizeSimpleQuery(simple).widen[SingleQuery]
  }

  def canonicalizeQuery(query: Query): CanonProgram[Query] = query match {
    case union: Query.Union =>
      for {
        lhs <- canonicalizeQuery(union.lhs)
        rhs <- canonicalizeSingleQuery(union.rhs)
      } yield union.copy(lhs = lhs, rhs = rhs)
    case single: Query.SingleQuery =>
      canonicalizeSingleQuery(single).widen[Query]
  }
}

object CanonicalizationPhase extends SimpleCompilerPhase[SymbolAnalysisState, Query, Query] {
  override def process(
    query: Query,
  ): SimpleCompilerPhaseEffect[SymbolAnalysisState, Query] = OptionT {
    IndexedState { saState =>
      import CanonicalizationModule._

      val initialCanonState = CanonState(
        table = saState.symbolTable,
        currentFreshId = saState.nextFreshId,
      )

      val (finalCanonState, rewrittenQuery) =
        canonicalizeQuery(query).run(initialCanonState).value

      val resultState = saState.copy(
        symbolTable = finalCanonState.table,
        nextFreshId = finalCanonState.currentFreshId,
      )

      (resultState, Some(rewrittenQuery))
    }
  }
}
