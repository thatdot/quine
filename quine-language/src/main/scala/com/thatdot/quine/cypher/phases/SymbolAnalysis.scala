package com.thatdot.quine.cypher.phases

import scala.collection.immutable.Queue

import cats.Monoid
import cats.data.{IndexedState, OptionT, State}
import cats.implicits._

import com.thatdot.quine.cypher.ast.Query.SingleQuery
import com.thatdot.quine.cypher.ast.{
  Connection,
  EdgePattern,
  Effect,
  GraphPattern,
  NodePattern,
  Projection,
  Query,
  QueryPart,
  ReadingClause,
  WithClause,
  YieldItem,
}
import com.thatdot.quine.cypher.phases.SymbolAnalysisModule.{SymbolTable, SymbolTableState}
import com.thatdot.quine.language.ast.{CypherIdentifier, Direction, Expression, QuineIdentifier, Source}
import com.thatdot.quine.language.diagnostic.Diagnostic
import com.thatdot.quine.language.phases.CompilerPhase.{SimpleCompilerPhase, SimpleCompilerPhaseEffect}
import com.thatdot.quine.language.phases.CompilerState
import com.thatdot.quine.language.types.Type

object SymbolAnalysisModule {

  sealed trait SymbolTableEntry {
    val source: Source
    val identifier: Int
  }

  object SymbolTableEntry {
    case class NodeEntry(source: Source, identifier: Int, labels: Set[Symbol], maybeProperties: Option[Expression])
        extends SymbolTableEntry
    case class EdgeEntry(source: Source, identifier: Int, edgeType: Symbol, direction: Direction)
        extends SymbolTableEntry
    case class UnwindEntry(source: Source, identifier: Int, from: Expression) extends SymbolTableEntry
    case class ForeachEntry(source: Source, identifier: Int, from: Expression) extends SymbolTableEntry
    case class ProcedureYieldEntry(source: Source, identifier: Int, procedureName: Symbol, resultField: Symbol)
        extends SymbolTableEntry
    case class ExpressionEntry(source: Source, identifier: Int, exp: Expression) extends SymbolTableEntry

    case class QuineToCypherIdEntry(source: Source, identifier: Int, cypherIdentifier: Symbol) extends SymbolTableEntry
  }

  case class TypeEntry(source: Source = Source.NoSource, identifier: String, ty: Type)

  case class SymbolTable(references: List[SymbolTableEntry], typeVars: List[TypeEntry])

  object SymbolTable {
    def empty: SymbolTable = SymbolTable(Nil, Nil)
  }

  implicit val TableMonoid: Monoid[SymbolTable] = new Monoid[SymbolTable] {
    override def empty: SymbolTable = SymbolTable(Nil, Nil)

    override def combine(x: SymbolTable, y: SymbolTable): SymbolTable =
      SymbolTable(
        references = x.references ::: y.references,
        typeVars = x.typeVars ::: y.typeVars,
      )
  }

  case class SymbolTableState(
    table: SymbolTable,
    errors: Queue[String],
    warnings: Queue[String],
    currentScope: Set[(Int, Symbol)],
    currentFreshId: Int,
  )

  type SymbolProgram[A] = State[SymbolTableState, A]

  /** An alias for State.modify that fixes the `State` type to be that of a
    * SymbolProgram
    *
    * @param update Function to update a SymbolTableState
    * @return A SymbolProgram that, when run, performs the update defined by `update`
    */
  def mod(update: SymbolTableState => SymbolTableState): SymbolProgram[Unit] =
    State.modify(update)

  def inspect[A](view: SymbolTableState => A): SymbolProgram[A] =
    State.inspect(view)

  def pure[A](a: A): SymbolProgram[A] = State.pure(a)

  val freshId: SymbolProgram[Int] =
    mod(st => st.copy(currentFreshId = st.currentFreshId + 1)) *> inspect(_.currentFreshId)

  def findInScopeByInt(id: Int): SymbolProgram[Option[Symbol]] =
    inspect(_.currentScope.find(_._1 == id).map(_._2))

  def findScopeEntryByInt(id: Int): SymbolProgram[Option[(Int, Symbol)]] =
    inspect(_.currentScope.find(_._1 == id))

  def findInScopeByName(name: Symbol): SymbolProgram[Option[Int]] =
    inspect(_.currentScope.find(_._2 == name).map(_._1))

  def intro(name: Symbol, source: Source): SymbolProgram[QuineIdentifier] = for {
    id <- freshId
    _ <- mod(st => st.copy(currentScope = st.currentScope + ((id, name))))
    _ <- addEntry(SymbolTableEntry.QuineToCypherIdEntry(source, id, name))
  } yield QuineIdentifier(id)

  def freshScope(imports: Set[QuineIdentifier]): SymbolProgram[Unit] =
    for {
      maybeNewScope <- imports.toList.traverse(qid => findScopeEntryByInt(qid.name))
      newScope <- maybeNewScope.foldM(Set.empty[(Int, Symbol)]) { (acc, maybeEntry) =>
        maybeEntry match {
          case Some(entry) => pure(acc + entry)
          // If `findScopeEntryById` returned an Either, we could build a better diagnostic here
          case None => addError(s"Unable to find an entry in the old symbol table.") *> pure(acc)
        }
      }
      _ <- mod(st => st.copy(currentScope = newScope))
    } yield ()

  def rewriteId(name: Symbol, source: Source = Source.NoSource): SymbolProgram[QuineIdentifier] =
    for {
      maybeId <- findInScopeByName(name)
      rewrittenId <- maybeId match {
        case Some(id) => pure(QuineIdentifier(id))
        case None => intro(name, source)
      }
    } yield rewrittenId

  /** Creates a program that, when run, checks for the existence
    * of an entry in the current context for a given identifier.
    *
    * @param identifier
    * @return A program that, when run, returns <code>true</code> if the identifier has an entry in the current contexts table
    */
  def entryExists(identifier: Int): SymbolProgram[Boolean] =
    inspect(_.table.references.exists(_.identifier == identifier))

  /** Adds an error to the current state of a SymbolProgram
    *
    * @param msg Diagnostic message
    * @return A SymbolProgram that, when run, adds the provided error
    */
  def addError(msg: String): SymbolProgram[Unit] =
    mod(st => st.copy(errors = st.errors.enqueue(msg)))

  /** Adds a warning to the current state of a SymbolProgram
    *
    * @param msg Diagnostic message
    * @return A SymbolProgram that, when run, adds the provided warning
    */
  def addWarning(msg: String): SymbolProgram[Unit] =
    mod(st => st.copy(warnings = st.warnings.enqueue(msg)))

  /** Looks up an identifier by name in the current scope.
    * Unlike `rewriteId`, this function is for reference sites where the variable
    * must already be defined. If the variable is not found, an error diagnostic
    * is recorded and a fresh identifier is returned to allow analysis to continue.
    *
    * @param name   The symbol name to look up
    * @param source Source location for error reporting
    * @return A program that returns the QuineIdentifier if found, or a fresh one with an error if not
    */
  def lookupId(name: Symbol, source: Source): SymbolProgram[QuineIdentifier] =
    for {
      maybeId <- findInScopeByName(name)
      result <- maybeId match {
        case Some(id) => pure(QuineIdentifier(id))
        case None =>
          // Variable not in scope - this is an error at reference sites
          addError(s"Undefined variable '${name.name}' at $source") *>
            // Return a fresh ID to allow analysis to continue and catch more errors
            freshId.map(QuineIdentifier(_))
      }
    } yield result

  /** Checks if an entry of the same type already exists for a given identifier.
    * This is used to detect actual redefinitions (e.g., two NodeEntries for the same binding)
    * rather than different entry types for the same identifier (which is expected).
    */
  def sameTypeEntryExists(entry: SymbolTableEntry): SymbolProgram[Boolean] =
    inspect { st =>
      st.table.references.exists { existing =>
        existing.identifier == entry.identifier &&
        existing.getClass == entry.getClass
      }
    }

  def addEntry(entry: SymbolTableEntry): SymbolProgram[Unit] =
    for {
      alreadyDefined <- sameTypeEntryExists(entry)
      _ <- mod(st =>
        st.copy(
          table = st.table.copy(references = entry :: st.table.references),
        ),
      )
      _ <- addError(
        s"Symbol ${entry.identifier} at ${entry.source} already defined!",
      ).whenA(alreadyDefined)
    } yield ()

  def analyzeMapLiteral(
    ml: Expression.MapLiteral,
  ): SymbolProgram[Expression.MapLiteral] =
    for {
      rewrittenExps <- ml.value.toList
        .traverse(p => analyzeExpression(p._2).map(v => p._1 -> v))
    } yield ml.copy(value = rewrittenExps.toMap)

  def analyzeFieldAccess(
    fa: Expression.FieldAccess,
  ): SymbolProgram[Expression.FieldAccess] =
    for {
      rewrittenOf <- analyzeExpression(fa.of)
    } yield fa.copy(of = rewrittenOf)

  def analyzeExpression(expression: Expression): SymbolProgram[Expression] =
    expression match {
      case lookup: Expression.IdLookup =>
        for {
          rewrittenId <- lookup.nodeIdentifier match {
            case Left(value) => lookupId(value.name, lookup.source)
            case Right(value) => pure(value)
          }
        } yield lookup.copy(nodeIdentifier = Right(rewrittenId))
      case synthesizeId: Expression.SynthesizeId =>
        for {
          rewrittenArgs <- synthesizeId.from.traverse(analyzeExpression)
        } yield synthesizeId.copy(from = rewrittenArgs)
      case al: Expression.AtomicLiteral => pure(al)
      case ll: Expression.ListLiteral =>
        for {
          rewrittenExps <- ll.value.traverse(analyzeExpression)
        } yield ll.copy(value = rewrittenExps)
      case ml: Expression.MapLiteral => analyzeMapLiteral(ml).widen[Expression]
      case id: Expression.Ident =>
        (id.identifier match {
          case Left(value) => lookupId(value.name, id.source)
          case Right(value) => pure(value)
        }).map(rid => id.copy(identifier = Right(rid)))
      case p: Expression.Parameter => pure(p)
      case a: Expression.Apply =>
        for {
          rewrittenArgs <- a.args.traverse(analyzeExpression)
        } yield a.copy(args = rewrittenArgs)
      case uo: Expression.UnaryOp =>
        for {
          rewrittenExp <- analyzeExpression(uo.exp)
        } yield uo.copy(exp = rewrittenExp)
      case bo: Expression.BinOp =>
        for {
          rewrittenLeft <- analyzeExpression(bo.lhs)
          rewrittenRight <- analyzeExpression(bo.rhs)
        } yield bo.copy(lhs = rewrittenLeft, rhs = rewrittenRight)
      case fa: Expression.FieldAccess =>
        analyzeFieldAccess(fa).widen[Expression]
      case arrayIndex: Expression.IndexIntoArray =>
        for {
          rewrittenOf <- analyzeExpression(arrayIndex.of)
          rewrittenIndex <- analyzeExpression(arrayIndex.index)
        } yield arrayIndex.copy(of = rewrittenOf, index = rewrittenIndex)
      case isNull: Expression.IsNull =>
        for {
          rewrittenOf <- analyzeExpression(isNull.of)
        } yield isNull.copy(of = rewrittenOf)
      case caseBlock: Expression.CaseBlock =>
        for {
          rewrittenCases <- caseBlock.cases.traverse { sc =>
            for {
              rewrittenCondition <- analyzeExpression(sc.condition)
              rewrittenValue <- analyzeExpression(sc.value)
            } yield sc.copy(condition = rewrittenCondition, value = rewrittenValue)
          }
          rewrittenAlternative <- analyzeExpression(caseBlock.alternative)
        } yield caseBlock.copy(
          cases = rewrittenCases,
          alternative = rewrittenAlternative,
        )
    }

  def analyzeProjection(projection: Projection): SymbolProgram[Projection] =
    for {
      rewrittenExp <- analyzeExpression(projection.expression)
      rewrittenAs <- projection.as match {
        case Left(value) => rewriteId(value.name, projection.source)
        case Right(value) => pure(value)
      }
      _ <- addEntry(SymbolTableEntry.ExpressionEntry(projection.source, rewrittenAs.name, rewrittenExp))
    } yield projection.copy(expression = rewrittenExp, as = Right(rewrittenAs))

  /** Creates a program that, when run
    * <ul>
    * <li>Creates a new scope</li>
    * <li>Binds one or more expressions to names in that new scope</li>
    * </ul>
    *
    * </code>WITH</code> clauses can also optionally...
    * <ul>
    * <li>Import all bindings from a previous scope</li>
    * <li>Alias a binding from a previous scope</li>
    * </ul>
    *
    * @param withClause
    * @return A program that, when executed, updates the initial state with one or more bindings
    */
  def analyzeWithClause(withClause: WithClause): SymbolProgram[WithClause] =
    if (withClause.hasWildCard) {
      for {
        rewrittenProjections <- withClause.bindings.traverse(analyzeProjection)
        rewrittenWhere <- withClause.maybePredicate match {
          case Some(value) => analyzeExpression(value).map(e => Some(e))
          case None => pure(Option.empty[Expression])
        }
      } yield withClause.copy(
        bindings = rewrittenProjections,
        maybePredicate = rewrittenWhere,
      )
    } else {
      // For non-wildcard WITH: expressions must be analyzed in the OLD scope (to resolve
      // references like `m` from previous MATCH), then a fresh scope is created with only
      // the new alias bindings. This implements Cypher's barrier semantics.
      for {
        // Step 1: Analyze expressions in the OLD scope (resolve references to prior bindings)
        rewrittenExpressions <- withClause.bindings.traverse(p => analyzeExpression(p.expression))
        // Step 2: Clear the scope - only the aliases will be visible after WITH
        _ <- freshScope(Set())
        // Step 3: Introduce aliases into the new scope and combine with analyzed expressions
        rewrittenProjections <- withClause.bindings.zip(rewrittenExpressions).traverse { case (p, rewrittenExp) =>
          for {
            rewrittenAs <- p.as match {
              case Left(value) => rewriteId(value.name, p.source)
              case Right(value) => pure(value)
            }
            _ <- addEntry(SymbolTableEntry.ExpressionEntry(p.source, rewrittenAs.name, rewrittenExp))
          } yield p.copy(expression = rewrittenExp, as = Right(rewrittenAs))
        }
        rewrittenWhere <- withClause.maybePredicate match {
          case Some(value) => analyzeExpression(value).map(e => Some(e))
          case None => pure(Option.empty[Expression])
        }
      } yield withClause.copy(
        bindings = rewrittenProjections,
        maybePredicate = rewrittenWhere,
      )
    }

  def analyzeEdgePattern(pattern: EdgePattern): SymbolProgram[EdgePattern] =
    for {
      rewrittenId <- pattern.maybeBinding match {
        case Some(id) =>
          id match {
            case Left(value) => rewriteId(value.name, pattern.source).map(qid => Some(Right(qid)))
            case Right(value) => pure(Some(Right(value)))
          }
        // Anonymous edges (no binding) stay anonymous - don't generate an ID
        case None => pure(None)
      }
      // Add EdgeEntry for named edge bindings so downstream phases can look them up
      _ <- rewrittenId match {
        case Some(Right(qid)) =>
          addEntry(
            SymbolTableEntry.EdgeEntry(
              pattern.source,
              qid.name,
              pattern.edgeType,
              pattern.direction,
            ),
          )
        case _ => pure(())
      }
    } yield pattern.copy(maybeBinding = rewrittenId)

  def analyzeConnection(connection: Connection): SymbolProgram[Connection] =
    for {
      rewrittenEdgePattern <- analyzeEdgePattern(connection.edge)
      rewrittenNodePattern <- analyzeNodePattern(connection.dest)
    } yield connection.copy(edge = rewrittenEdgePattern, dest = rewrittenNodePattern)

  def analyzeNodePattern(pattern: NodePattern): SymbolProgram[NodePattern] =
    pattern match {
      case nodePattern: NodePattern =>
        for {
          rewrittenId <- nodePattern.maybeBinding match {
            case Some(id) =>
              id match {
                case Left(value) => rewriteId(value.name, nodePattern.source)
                case Right(value) => pure(value)
              }
            case None => freshId.map(QuineIdentifier)
          }
          rewrittenProps <- nodePattern.maybeProperties match {
            case Some(value) => analyzeExpression(value).map(p => Some(p))
            case None => pure(Option.empty)
          }
          _ <- addEntry(
            SymbolTableEntry.NodeEntry(
              nodePattern.source,
              rewrittenId.name,
              nodePattern.labels,
              rewrittenProps,
            ),
          )
        } yield nodePattern.copy(
          maybeBinding = Some(Right(rewrittenId)),
          maybeProperties = rewrittenProps,
        )
    }

  /** Adds the appropriate entry(s) to the SymbolTable in the current context based
    * on a pattern from a graph query.
    *
    * @param pattern A pattern to extract symbol table entries from
    * @return A program that, when executed, adds zero or more symbol table entries
    */
  def analyzePattern(pattern: GraphPattern): SymbolProgram[GraphPattern] =
    for {
      rewrittenInitial <- analyzeNodePattern(pattern.initial)
      rewrittenConnections <- pattern.path.traverse(analyzeConnection)
    } yield pattern.copy(initial = rewrittenInitial, path = rewrittenConnections)

  /** A partial function that extracts a `QuineIdentifier` from an `Either[CypherIdentifier, QuineIdentifier]`.
    * It matches and returns the right value if the input is a `Right` containing a `QuineIdentifier`.
    *
    * This captures at runtime an expected invariant which is that all identifiers are rewritten
    * to be `QuineIdentifier`s.
    */
  val collectRewrittenIds: PartialFunction[Either[CypherIdentifier, QuineIdentifier], QuineIdentifier] = {
    case Right(id) => id
  }

  def analyzeReadingClause(
    readingClause: ReadingClause,
  ): SymbolProgram[ReadingClause] = readingClause match {
    case fromPattern: ReadingClause.FromPatterns =>
      for {
        rewrittenPatterns <- fromPattern.patterns.traverse(analyzePattern)
        rewrittenPredicate <- fromPattern.maybePredicate match {
          case Some(value) => analyzeExpression(value).map(e => Some(e))
          case None => pure(None)
        }
      } yield fromPattern.copy(
        patterns = rewrittenPatterns,
        maybePredicate = rewrittenPredicate,
      )
    case fromProcedure: ReadingClause.FromProcedure =>
      for {
        rewriteExps <- fromProcedure.args.traverse(analyzeExpression)
        // Introduce each yield binding into scope
        rewrittenYields <- fromProcedure.yields.traverse { yieldItem =>
          for {
            rewrittenBoundAs <- yieldItem.boundAs match {
              case Left(cypherId) => rewriteId(cypherId.name, fromProcedure.source)
              case Right(quineId) => pure(quineId)
            }
            _ <- addEntry(
              SymbolTableEntry.ProcedureYieldEntry(
                fromProcedure.source,
                rewrittenBoundAs.name,
                fromProcedure.name,
                yieldItem.resultField,
              ),
            )
          } yield YieldItem(yieldItem.resultField, Right(rewrittenBoundAs))
        }
      } yield fromProcedure.copy(args = rewriteExps, yields = rewrittenYields)
    case fromUnwind: ReadingClause.FromUnwind =>
      for {
        rewrittenList <- analyzeExpression(fromUnwind.list)
        rewrittenAs <- fromUnwind.as match {
          case Left(value) => rewriteId(value.name, fromUnwind.source)
          case Right(value) => pure(value)
        }
        _ <- addEntry(
          SymbolTableEntry
            .UnwindEntry(fromUnwind.source, rewrittenAs.name, rewrittenList),
        )
      } yield fromUnwind.copy(list = rewrittenList, as = Right(rewrittenAs))
    case fromSq: ReadingClause.FromSubquery =>
      for {
        rewrittenBindings <- fromSq.bindings.traverse(binding =>
          binding match {
            case Left(value) => rewriteId(value.name)
            case Right(value) => pure(value)
          },
        )
        oldScope <- inspect(_.currentScope)
        rewrittenQuery <- analyzeQuery(fromSq.subquery, rewrittenBindings.toSet)
        imports = rewrittenQuery match {
          case query: Query.SingleQuery =>
            query match {
              case s: SingleQuery.SinglepartQuery => s.bindings.map(_.as).toSet.collect(collectRewrittenIds)
              case c: SingleQuery.MultipartQuery => c.into.bindings.map(_.as).toSet.collect(collectRewrittenIds)
            }
          case _: Query.Union =>
            // Union subqueries not yet supported - return empty set and the warning will be added below
            Set.empty[QuineIdentifier]
        }
        _ <- rewrittenQuery match {
          case _: Query.Union =>
            addWarning("Union subqueries are not yet supported; bindings from this subquery will not be available.")
          case _ => pure(())
        }
        newIntros <- imports.toList.traverse(qid =>
          findInScopeByInt(qid.name).map(maybeId => maybeId.map(name => (qid.name -> name))),
        )
        validIntros = newIntros.collect { case Some(intro) =>
          intro
        }
        _ <- mod(st => st.copy(currentScope = oldScope ++ validIntros.toSet))
      } yield fromSq.copy(
        bindings = rewrittenBindings.map(Right(_)),
        subquery = rewrittenQuery,
      )
  }

  def analyzeEffect(effect: Effect): SymbolProgram[Effect] = effect match {
    case foreach: Effect.Foreach =>
      for {
        rewrittenExpression <- analyzeExpression(foreach.in)
        // Save current scope before introducing FOREACH binding
        oldScope <- inspect(_.currentScope)
        // Introduce the FOREACH binding into scope so nested effects can reference it
        rewrittenBinding <- intro(foreach.binding, foreach.source)
        // Add ForeachEntry so downstream phases can look up the binding
        _ <- addEntry(
          SymbolTableEntry.ForeachEntry(
            foreach.source,
            rewrittenBinding.name,
            rewrittenExpression,
          ),
        )
        rewrittenEffects <- foreach.effects.traverse(analyzeEffect)
        // Restore the old scope (FOREACH binding goes out of scope)
        _ <- mod(st => st.copy(currentScope = oldScope))
      } yield foreach.copy(in = rewrittenExpression, effects = rewrittenEffects)
    case sp: Effect.SetProperty =>
      for {
        rewrittenExpression <- analyzeExpression(sp.value)
        rewrittenProperty <- analyzeFieldAccess(sp.property)
      } yield sp.copy(
        property = rewrittenProperty,
        value = rewrittenExpression,
      )
    case sps: Effect.SetProperties =>
      for {
        rewrittenProperties <- analyzeExpression(sps.properties)
        // SET on a node/edge requires the identifier to already be defined
        rewrittenIdent <- sps.of match {
          case Left(value) => lookupId(value.name, sps.source)
          case Right(value) => pure(value)
        }
      } yield sps.copy(of = Right(rewrittenIdent), properties = rewrittenProperties)
    case sl: Effect.SetLabel =>
      for {
        // SET label on a node requires the identifier to already be defined
        rewrittenIdent <- sl.on match {
          case Left(value) => lookupId(value.name, sl.source)
          case Right(value) => pure(value)
        }
      } yield sl.copy(on = Right(rewrittenIdent))
    case c: Effect.Create =>
      for {
        rewrittenPatterns <- c.patterns.traverse(analyzePattern)
      } yield c.copy(patterns = rewrittenPatterns)
  }

  def analyzeQueryPart(queryPart: QueryPart): SymbolProgram[QueryPart] =
    queryPart match {
      case rcp: QueryPart.ReadingClausePart =>
        for {
          rewrittenReadingClause <- analyzeReadingClause(rcp.readingClause)
        } yield rcp.copy(readingClause = rewrittenReadingClause)
      case wcp: QueryPart.WithClausePart =>
        for {
          rewrittenWithClause <- analyzeWithClause(wcp.withClause)
        } yield wcp.copy(withClause = rewrittenWithClause)
      case ep: QueryPart.EffectPart =>
        for {
          rewrittenEffect <- analyzeEffect(ep.effect)
        } yield ep.copy(effect = rewrittenEffect)
    }

  def analyzeSimpleQuery(
    query: SingleQuery.SinglepartQuery,
  ): SymbolProgram[SingleQuery.SinglepartQuery] =
    for {
      rewrittenQueryParts <- query.queryParts.traverse(analyzeQueryPart)
      rewrittenProjection <- query.bindings.traverse(analyzeProjection)
    } yield query.copy(
      queryParts = rewrittenQueryParts,
      bindings = rewrittenProjection,
    )

  def analyzeSingleQuery(
    query: SingleQuery,
    imports: Set[QuineIdentifier] = Set.empty,
  ): SymbolProgram[SingleQuery] = query match {
    case complex: SingleQuery.MultipartQuery =>
      for {
        _ <- freshScope(imports)
        rewrittenParts <- complex.queryParts.traverse(analyzeQueryPart)
        rewrittenInto <- analyzeSimpleQuery(complex.into)
      } yield complex.copy(queryParts = rewrittenParts, into = rewrittenInto)
    case simple: SingleQuery.SinglepartQuery => freshScope(imports) *> analyzeSimpleQuery(simple).widen[SingleQuery]
  }

  def analyzeQuery(query: Query, imports: Set[QuineIdentifier] = Set.empty): SymbolProgram[Query] =
    query match {
      case union: Query.Union =>
        for {
          rewrittenLeft <- analyzeSingleQuery(union.lhs, imports)
          rewrittenRight <- analyzeQuery(union.rhs, imports)
        } yield union.copy(lhs = rewrittenLeft, rhs = rewrittenRight)
      // Pass imports to single queries so that subquery imports (CALL { WITH x ... })
      // correctly make imported variables available inside the subquery scope.
      case single: Query.SingleQuery => analyzeSingleQuery(single, imports).widen[Query]
    }
}

case class SymbolAnalysisState(diagnostics: List[Diagnostic], symbolTable: SymbolTable, cypherText: String)
    extends CompilerState

/** This compiler phase does two things.
  * <ol>
  *   <li>Rewrites all identifiers</li>
  *   <li>Builds a symbol table</li>
  * </ol>
  *
  * The IDs have the ScopeID appended to them. This enables the query planner
  * to correctly build a dependency graph without having to understand the
  * shadowing (or lack thereof) rules within Cypher.
  */
object SymbolAnalysisPhase extends SimpleCompilerPhase[SymbolAnalysisState, Query, Query] {
  override def process(
    query: Query,
  ): SimpleCompilerPhaseEffect[SymbolAnalysisState, Query] = OptionT {
    IndexedState { symbolAnalysisState =>
      val (finalState, rewrittenQuery) = SymbolAnalysisModule
        .analyzeQuery(query)
        .run(SymbolTableState(SymbolTable.empty, Queue.empty, Queue.empty, Set.empty, 0))
        .value

      val errorDiagnostics = finalState.errors.toList.map(Diagnostic.SymbolAnalysisError)
      val warningDiagnostics = finalState.warnings.toList.map(Diagnostic.SymbolAnalysisWarning)

      val resultState = symbolAnalysisState.copy(
        diagnostics = errorDiagnostics ::: warningDiagnostics ::: symbolAnalysisState.diagnostics,
        symbolTable = finalState.table,
      )

      (resultState, Some(rewrittenQuery))
    }
  }

}
