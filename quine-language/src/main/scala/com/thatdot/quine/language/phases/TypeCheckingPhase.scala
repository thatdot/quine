package com.thatdot.quine.language.phases

import cats.data.{IndexedStateT, NonEmptyList, OptionT}
import cats.implicits._

import com.thatdot.quine.cypher.ast.Query.SingleQuery
import com.thatdot.quine.cypher.ast._
import com.thatdot.quine.cypher.phases.SymbolAnalysisModule.SymbolTableEntry.{ExpressionEntry, NodeEntry, UnwindEntry}
import com.thatdot.quine.cypher.phases.SymbolAnalysisModule.{SymbolTable, TypeEntry}
import com.thatdot.quine.language.ast.{CypherIdentifier, Expression, Operator, QuineIdentifier, Value}
import com.thatdot.quine.language.diagnostic.Diagnostic
import com.thatdot.quine.language.phases.CompilerPhase.{SimpleCompilerPhase, SimpleCompilerPhaseEffect}
import com.thatdot.quine.language.types.Type.{PrimitiveType, TypeConstructor, TypeVariable}
import com.thatdot.quine.language.types.{Constraint, Type}

/** State maintained during the type checking phase.
  *
  * @param diagnostics Accumulated type errors and warnings. New diagnostics are prepended
  *                    (most recent first). Type errors do NOT cause phase failure - they
  *                    accumulate here while processing continues.
  * @param symbolTable The symbol table from the SymbolAnalysis phase, containing variable
  *                    bindings and scope information. This phase adds type information
  *                    via TypeEntry records.
  * @param typeEnv     The type environment mapping type variable symbols to their resolved
  *                    types. When a TypeVariable is unified with a concrete type or another
  *                    variable, the binding is recorded here. Use `deref` to follow chains
  *                    of bindings to find the resolved type.
  * @param freshId     Counter for generating unique type variable names. Incremented each
  *                    time `freshen` is called to ensure globally unique type variable IDs.
  */
case class TypeCheckingState(
  diagnostics: List[Diagnostic],
  symbolTable: SymbolTable,
  typeEnv: Map[Symbol, Type],
  freshId: Int,
) extends CompilerState

/** Type checking phase for Cypher queries using Hindley-Milner style type inference.
  *
  * == Overview ==
  *
  * This phase performs bidirectional type checking with constraint-based unification.
  * It traverses the query AST, assigns type annotations to all expressions, and
  * verifies type consistency through unification.
  *
  * == Algorithm ==
  *
  * The type checker uses a standard unification-based approach:
  *
  * 1. '''Annotation''': Each expression is assigned a type. Literals get concrete types
  *    (Integer, Boolean, etc.), while unknowns get fresh TypeVariables.
  *
  * 2. '''Unification''': When two types must be equal (e.g., both sides of `=`),
  *    `unify(t1, t2)` is called. This either:
  *    - Succeeds silently if types are compatible
  *    - Binds a TypeVariable to a concrete type in `typeEnv`
  *    - Records a diagnostic if types are incompatible
  *
  * 3. '''Constraint Checking''': TypeVariables may carry constraints (Numeric, Semigroup).
  *    Before binding a variable, the constraint is checked against the target type.
  *
  * == Type System ==
  *
  * Types are defined in [[com.thatdot.quine.language.types.Type]]:
  *
  * - '''PrimitiveType''': Integer, Real, Boolean, String, NodeType
  * - '''TypeVariable''': An unknown type to be resolved through unification.
  *   May have a constraint (Numeric for arithmetic, Semigroup for `+`).
  * - '''TypeConstructor''': Parameterized types like List[T] or Map[K,V]
  * - '''Any''': Top type, unifies with everything
  * - '''Null''': Null type, also unifies with everything
  * - '''Error''': Indicates a type error occurred
  *
  * == Constraints ==
  *
  * TypeVariables can be constrained:
  *
  * - '''Constraint.None''': No constraint, can unify with any type
  * - '''Constraint.Numeric''': Must be Integer or Real (for -, *, /, %)
  * - '''Constraint.Semigroup''': Must support concatenation (for +): Integer, Real, or String
  *
  * == Error Handling ==
  *
  * Type errors are accumulated in `diagnostics` rather than causing phase failure.
  * This allows the phase to report multiple type errors in a single pass rather than
  * stopping at the first error. The phase always returns `Some(query)` with annotated
  * types (possibly containing Error types where checking failed).
  *
  * == Input/Output ==
  *
  * - '''Input''': Query AST with symbolTable populated from SymbolAnalysis phase
  * - '''Output''': Same Query AST with `ty: Option[Type]` fields populated on all expressions
  * - '''State Change''': Adds TypeEntry records to symbolTable for projected/bound identifiers
  *
  * @param initialTypeEnv Pre-populated type bindings (e.g., for built-in functions)
  */
class TypeCheckingPhase(initialTypeEnv: Map[Symbol, Type])
    extends SimpleCompilerPhase[TypeCheckingState, Query, Query] {

  /** Convenience type alias for the state monad transformer stack used throughout this phase. */
  type TCEffect[A] = SimpleCompilerPhaseEffect[TypeCheckingState, A]

  // === Core State Operations ===

  def mod(update: TypeCheckingState => TypeCheckingState): TCEffect[Unit] =
    OptionT.liftF(IndexedStateT.modify(update))

  def inspect[A](view: TypeCheckingState => A): TCEffect[A] =
    OptionT.liftF(IndexedStateT.inspect(view))

  def addDiagnostic(msg: String): TCEffect[Unit] =
    mod(st => st.copy(diagnostics = Diagnostic.TypeCheckError(msg) :: st.diagnostics))

  def checkEnv(id: Symbol): TCEffect[Option[Type]] =
    inspect(_.typeEnv.get(id))

  def typeOfSymbol(id: String): TCEffect[Option[Type]] =
    inspect(_.symbolTable.typeVars.find(_.identifier == id).map(_.ty))

  def addTableEntry(id: String, ty: Type): TCEffect[Unit] =
    mod(st =>
      st.copy(symbolTable =
        st.symbolTable.copy(typeVars = TypeEntry(identifier = id, ty = ty) :: st.symbolTable.typeVars),
      ),
    )

  def addTableEntryForId(id: Either[CypherIdentifier, QuineIdentifier], ty: Type): TCEffect[Unit] = {
    val idStr = id match {
      case Left(cypher) => cypher.name.name
      case Right(quine) => s"_q${quine.name}"
    }
    addTableEntry(idStr, ty)
  }

  /** Generates a fresh, unique type variable symbol.
    *
    * Each call increments the `freshId` counter to ensure uniqueness.
    * The optional hint is incorporated into the symbol name for debugging.
    *
    * @param hint Optional hint to make the generated name more descriptive
    *             (e.g., "list_elem" -> 'list_elem_42)
    * @return A globally unique Symbol for use in a TypeVariable
    */
  def freshen(hint: Option[String]): TCEffect[Symbol] =
    for {
      _ <- mod(st => st.copy(freshId = st.freshId + 1))
      idSuffix <- inspect(_.freshId)
    } yield hint match {
      case Some(value) => Symbol(s"${value}_$idSuffix")
      case None => Symbol(s"type_$idSuffix")
    }

  // === Type Operations ===

  /** Binds a type variable to a type in the type environment.
    *
    * If the variable is already bound, a diagnostic warning is recorded but the
    * new binding proceeds (overwriting the old). This is a form of destructive
    * update that may indicate a bug in the unification logic.
    *
    * @param variable The type variable to bind
    * @param to       The type to bind it to
    */
  def bind(variable: TypeVariable, to: Type): TCEffect[Unit] = for {
    currentBinding <- checkEnv(variable.id)
    _ <- mod(st => st.copy(typeEnv = st.typeEnv + (variable.id -> to)))
    _ <- addDiagnostic(s"Binding ${variable.id} to $to in the type environment is overriding ${currentBinding}")
      .whenA(currentBinding.isDefined)
  } yield ()

  /** Dereferences a type by following type variable bindings in the environment.
    *
    * If `ty` is a TypeVariable bound to another type, recursively dereference
    * until reaching either an unbound variable or a concrete type.
    *
    * @param ty The type to dereference
    * @return The resolved type (either a concrete type or an unbound TypeVariable)
    */
  def deref(ty: Type): TCEffect[Type] =
    ty match {
      case typeVar: TypeVariable =>
        checkEnv(typeVar.id) >>= {
          case Some(ref) => deref(ref)
          case None => pure(ty)
        }
      case _ => pure(ty)
    }

  /** Checks whether a type satisfies a constraint.
    *
    * Constraints restrict what types a TypeVariable can be unified with:
    * - Numeric: Only Integer, Real, Any, or another Numeric-constrained variable
    * - Semigroup: Integer, Real, String, Any, or compatible constrained variable
    * - None: Any type satisfies this constraint
    *
    * @param constraint The constraint to check
    * @param ty         The type to check against the constraint
    * @return true if the type satisfies the constraint, false otherwise
    */
  def checkConstraint(constraint: Constraint, ty: Type): TCEffect[Boolean] =
    constraint match {
      case Constraint.None => pure(true)
      case Constraint.Numeric =>
        ty match {
          case Type.Effectful(valueType) => checkConstraint(constraint, valueType)
          case TypeVariable(_, oc) =>
            oc match {
              case Constraint.None => pure(true)
              case Constraint.Numeric => pure(true)
              case Constraint.Semigroup => pure(false)
            }
          case PrimitiveType.Integer => pure(true)
          case PrimitiveType.Real => pure(true)
          case Type.Any => pure(true)
          case _ => pure(false)
        }
      case Constraint.Semigroup =>
        ty match {
          case TypeVariable(_, oc) =>
            oc match {
              case Constraint.None => pure(true)
              case Constraint.Semigroup => pure(true)
              case Constraint.Numeric => pure(true)
            }
          case PrimitiveType.Integer => pure(true)
          case PrimitiveType.Real => pure(true)
          case PrimitiveType.String => pure(true)
          case Type.Any => pure(true)
          case _ => pure(false)
        }
    }

  /** Unifies two types, binding type variables as needed.
    *
    * Unification is the core operation of the type checker. Given two types that
    * must be equal, it either succeeds (possibly binding variables) or records
    * a diagnostic error.
    *
    * == Unification Rules ==
    *
    * 1. '''Identical primitives''': Always succeed (Int ~ Int)
    * 2. '''Two TypeVariables''': Create a fresh variable bound to both,
    *    checking that constraints are compatible
    * 3. '''TypeVariable ~ Type''': Bind variable to type if constraint is satisfied
    * 4. '''TypeConstructors''': Must have same ID and arity, then unify args pairwise
    * 5. '''Any ~ T''' or '''T ~ Any''': Always succeed (Any is the top type)
    * 6. '''Null ~ T''' or '''T ~ Null''': Always succeed (Null is compatible with all)
    * 7. '''Otherwise''': Record a type mismatch diagnostic
    *
    * @param lhs The first type to unify
    * @param rhs The second type to unify
    */
  def unify(lhs: Type, rhs: Type): TCEffect[Unit] = for {
    derefLHS <- deref(lhs)
    derefRHS <- deref(rhs)
    _ <- (derefLHS, derefRHS) match {
      case (p1: PrimitiveType, p2: PrimitiveType) =>
        if (p1 == p2) pure(()) else addDiagnostic(s"Type mismatch: cannot unify $p1 and $p2")
      case (v1: TypeVariable, v2: TypeVariable) =>
        if (v1 == v2) pure(())
        else {
          checkConstraint(v1.constraint, v2) >>= {
            case true =>
              freshen(Some(s"${v1.id.name}_${v2.id.name}")) >>= { tv =>
                bind(v1, TypeVariable(tv, Constraint.None)) *> bind(v2, TypeVariable(tv, Constraint.None))
              }
            case false => addDiagnostic(s"Failed to unify $v1 with $v2: constraint mismatch")
          }
        }
      case (v: TypeVariable, o: Type) =>
        checkConstraint(v.constraint, o) >>= {
          case true => bind(v, o)
          case false => addDiagnostic(s"Failed to unify $v with $o: constraint ${v.constraint} not satisfied")
        }
      case (o: Type, v: TypeVariable) =>
        checkConstraint(v.constraint, o) >>= {
          case true => bind(v, o)
          case false => addDiagnostic(s"Failed to unify $v with $o: constraint ${v.constraint} not satisfied")
        }
      case (tc1: TypeConstructor, tc2: TypeConstructor) =>
        if (tc1.id == tc2.id && tc1.args.length == tc2.args.length) {
          tc1.args.zip(tc2.args).toList.traverse_(p => unify(p._1, p._2))
        } else {
          addDiagnostic(s"Failed to unify $tc1 with $tc2: type constructor mismatch")
        }
      case (Type.Any, _) => pure(())
      case (_, Type.Any) => pure(())
      case (Type.Null, _) => pure(())
      case (_, Type.Null) => pure(())
      case (_, _) =>
        addDiagnostic(s"Unable to unify $derefLHS and $derefRHS")
    }
  } yield ()

  def getType(expression: Expression): TCEffect[Type] =
    expression.ty match {
      case Some(value) => pure(value)
      case None =>
        addDiagnostic(s"Expected expression to have a type annotation, but it did not: $expression") *>
          pure(Type.error)
    }

  def computeTypeForId(identifier: Either[CypherIdentifier, QuineIdentifier]): TCEffect[Type] = {
    val idStr = identifier match {
      case Left(cypher) => cypher.name.name
      case Right(quine) => s"_q${quine.name}"
    }
    typeOfSymbol(idStr).flatMap {
      case Some(ty) => pure(ty)
      case None =>
        freshen(Some(idStr)).map(tv => TypeVariable(tv, Constraint.None))
    }
  }

  // === Expression Type Checking ===

  /** Annotates an expression with its type and checks type consistency.
    *
    * This is the main expression type-checking function. It recursively traverses
    * the expression tree, assigning types bottom-up and unifying as needed.
    *
    * == Type Assignment Rules ==
    *
    * - '''Literals''': Get their natural type (42 -> Integer, "hi" -> String)
    * - '''Identifiers''': Look up existing type or create fresh TypeVariable
    * - '''Binary ops''': Unify operand types, constrain by operator (+ needs Semigroup)
    * - '''Comparisons''': Unify operands, result is Boolean
    * - '''Field access''': Result is fresh TypeVariable (field type unknown statically)
    * - '''Function calls''': Arguments checked, result is fresh TypeVariable
    *
    * @param expression The expression to type-check
    * @return The same expression with `ty` field populated
    */
  def annotateAndCheckExpression(expression: Expression): TCEffect[Expression] =
    expression match {
      case atomic: Expression.AtomicLiteral =>
        atomic.value match {
          case Value.Null => pure(atomic.copy(ty = Some(Type.Null)))
          case Value.True => pure(atomic.copy(ty = Some(PrimitiveType.Boolean)))
          case Value.False => pure(atomic.copy(ty = Some(PrimitiveType.Boolean)))
          case _: Value.Integer => pure(atomic.copy(ty = Some(PrimitiveType.Integer)))
          case _: Value.Real => pure(atomic.copy(ty = Some(PrimitiveType.Real)))
          case _: Value.Text => pure(atomic.copy(ty = Some(PrimitiveType.String)))
          case _: Value.Bytes => pure(atomic.copy(ty = Some(Type.Any)))
          case _: Value.Duration => pure(atomic.copy(ty = Some(Type.Any)))
          case _: Value.Date => pure(atomic.copy(ty = Some(Type.Any)))
          case _: Value.DateTime => pure(atomic.copy(ty = Some(Type.Any)))
          case _: Value.DateTimeLocal => pure(atomic.copy(ty = Some(Type.Any)))
          case _: Value.List => pure(atomic.copy(ty = Some(Type.Any)))
          case _: Value.Map => pure(atomic.copy(ty = Some(Type.Any)))
          case _: Value.NodeId => pure(atomic.copy(ty = Some(PrimitiveType.NodeType)))
          case _: Value.Node => pure(atomic.copy(ty = Some(PrimitiveType.NodeType)))
          case _: Value.Relationship => pure(atomic.copy(ty = Some(Type.Any)))
        }

      case list: Expression.ListLiteral =>
        for {
          annotatedExps <- list.value.traverse(annotateAndCheckExpression)
          freshName <- freshen(Some("list_elem"))
          elemType = TypeVariable(freshName, Constraint.None)
          _ <- annotatedExps.traverse_ { exp =>
            getType(exp).flatMap(unify(elemType, _))
          }
        } yield list.copy(
          value = annotatedExps,
          ty = Some(TypeConstructor(Symbol("List"), NonEmptyList.of(elemType))),
        )

      case ident: Expression.Ident =>
        for {
          typeOf <- computeTypeForId(ident.identifier)
        } yield ident.copy(ty = Some(typeOf))

      case param: Expression.Parameter =>
        for {
          varName <- freshen(Some(param.name.name))
        } yield param.copy(ty = Some(TypeVariable(varName, Constraint.None)))

      case apply: Expression.Apply =>
        for {
          annotatedArgs <- apply.args.traverse(annotateAndCheckExpression)
          freshName <- freshen(Some("apply_result"))
        } yield apply.copy(args = annotatedArgs, ty = Some(TypeVariable(freshName, Constraint.None)))

      case unary: Expression.UnaryOp =>
        for {
          annotatedExp <- annotateAndCheckExpression(unary.exp)
          expType <- getType(annotatedExp)
          resultType <- unary.op match {
            case Operator.Not =>
              unify(expType, PrimitiveType.Boolean) *> pure(PrimitiveType.Boolean)
            case Operator.Minus =>
              freshen(Some("neg")).map(tv => TypeVariable(tv, Constraint.Numeric))
            case Operator.Plus =>
              freshen(Some("pos")).map(tv => TypeVariable(tv, Constraint.Numeric))
            case _ =>
              freshen(Some("unary")).map(tv => TypeVariable(tv, Constraint.None))
          }
        } yield unary.copy(exp = annotatedExp, ty = Some(resultType))

      case binop: Expression.BinOp =>
        for {
          annotatedLeft <- annotateAndCheckExpression(binop.lhs)
          annotatedRight <- annotateAndCheckExpression(binop.rhs)
          leftType <- getType(annotatedLeft)
          rightType <- getType(annotatedRight)
          freshVarName <- freshen(Some("OpResult"))
          resultType <- binop.op match {
            case Operator.Plus =>
              val ty = TypeVariable(freshVarName, Constraint.Semigroup)
              unify(leftType, rightType) *> unify(leftType, ty) *> pure(ty)
            case Operator.Minus | Operator.Asterisk | Operator.Slash | Operator.Percent | Operator.Carat =>
              val ty = TypeVariable(freshVarName, Constraint.Numeric)
              unify(leftType, rightType) *> unify(leftType, ty) *> pure(ty)
            case Operator.Equals | Operator.NotEquals =>
              unify(leftType, rightType) *> pure(PrimitiveType.Boolean)
            case Operator.LessThan | Operator.LessThanEqual | Operator.GreaterThan | Operator.GreaterThanEqual =>
              unify(leftType, rightType) *> pure(PrimitiveType.Boolean)
            case Operator.And | Operator.Or | Operator.Xor =>
              unify(leftType, PrimitiveType.Boolean) *>
                unify(rightType, PrimitiveType.Boolean) *>
                pure(PrimitiveType.Boolean)
            case Operator.Not =>
              // Not is unary, shouldn't appear in BinOp but handle gracefully
              pure(PrimitiveType.Boolean)
          }
        } yield binop.copy(
          lhs = annotatedLeft,
          rhs = annotatedRight,
          ty = Some(resultType),
        )

      case arrayIndex: Expression.IndexIntoArray =>
        for {
          annotatedOf <- annotateAndCheckExpression(arrayIndex.of)
          annotatedIndex <- annotateAndCheckExpression(arrayIndex.index)
          indexType <- getType(annotatedIndex)
          _ <- unify(indexType, PrimitiveType.Integer)
          freshName <- freshen(Some("element"))
        } yield arrayIndex.copy(
          of = annotatedOf,
          index = annotatedIndex,
          ty = Some(TypeVariable(freshName, Constraint.None)),
        )

      case isNull: Expression.IsNull =>
        for {
          annotatedOf <- annotateAndCheckExpression(isNull.of)
        } yield isNull.copy(of = annotatedOf, ty = Some(PrimitiveType.Boolean))

      case caseBlock: Expression.CaseBlock =>
        for {
          annotatedCases <- caseBlock.cases.traverse { sc =>
            for {
              annotatedCondition <- annotateAndCheckExpression(sc.condition)
              conditionType <- getType(annotatedCondition)
              _ <- unify(conditionType, PrimitiveType.Boolean)
              annotatedValue <- annotateAndCheckExpression(sc.value)
            } yield sc.copy(condition = annotatedCondition, value = annotatedValue)
          }
          annotatedAlternative <- annotateAndCheckExpression(caseBlock.alternative)
          freshResultName <- freshen(Some("case_result"))
          resultType = TypeVariable(freshResultName, Constraint.None)
          alternativeType <- getType(annotatedAlternative)
          _ <- unify(resultType, alternativeType)
          _ <- annotatedCases.traverse_ { c =>
            getType(c.value).flatMap(unify(resultType, _))
          }
        } yield caseBlock.copy(
          cases = annotatedCases,
          alternative = annotatedAlternative,
          ty = Some(resultType),
        )

      case lookup: Expression.IdLookup =>
        for {
          typeOf <- computeTypeForId(lookup.nodeIdentifier)
        } yield lookup.copy(ty = Some(typeOf))

      case synthesize: Expression.SynthesizeId =>
        for {
          annotatedFrom <- synthesize.from.traverse(annotateAndCheckExpression)
        } yield synthesize.copy(from = annotatedFrom, ty = Some(Type.Any))

      case fa: Expression.FieldAccess => annotateFieldAccess(fa).widen[Expression]

      case map: Expression.MapLiteral => annotateMapLiteral(map).widen[Expression]
    }

  def annotateFieldAccess(fa: Expression.FieldAccess): TCEffect[Expression.FieldAccess] = for {
    annotatedOf <- annotateAndCheckExpression(fa.of)
    freshName <- freshen(Some(s"field_${fa.fieldName.name}"))
  } yield fa.copy(
    of = annotatedOf,
    ty = Some(TypeVariable(freshName, Constraint.None)),
  )

  def annotateMapLiteral(map: Expression.MapLiteral): TCEffect[Expression.MapLiteral] = for {
    annotatedPairs <- map.value.toList.traverse(p => annotateAndCheckExpression(p._2).map(e => p._1 -> e))
  } yield map.copy(
    value = annotatedPairs.toMap,
    ty = Some(TypeConstructor(Symbol("Map"), NonEmptyList.of(PrimitiveType.String, Type.Any))),
  )

  // === Query Traversal ===

  def annotateQuery(query: Query): TCEffect[Query] = query match {
    case union: Query.Union =>
      for {
        annotatedLhs <- annotateSingleQuery(union.lhs)
        annotatedRhs <- annotateQuery(union.rhs)
      } yield union.copy(lhs = annotatedLhs, rhs = annotatedRhs)

    case single: SingleQuery => annotateSingleQuery(single).widen[Query]
  }

  def annotateSingleQuery(query: SingleQuery): TCEffect[SingleQuery] = query match {
    case multi: SingleQuery.MultipartQuery =>
      for {
        annotatedParts <- multi.queryParts.traverse(annotateQueryPart)
        annotatedInto <- annotateSinglepartQuery(multi.into)
      } yield multi.copy(queryParts = annotatedParts, into = annotatedInto)

    case single: SingleQuery.SinglepartQuery =>
      annotateSinglepartQuery(single).widen[SingleQuery]
  }

  def annotateSinglepartQuery(query: SingleQuery.SinglepartQuery): TCEffect[SingleQuery.SinglepartQuery] = for {
    annotatedParts <- query.queryParts.traverse(annotateQueryPart)
    annotatedBindings <- query.bindings.traverse(annotateProjection)
  } yield query.copy(queryParts = annotatedParts, bindings = annotatedBindings)

  def annotateQueryPart(queryPart: QueryPart): TCEffect[QueryPart] = queryPart match {
    case rcp: QueryPart.ReadingClausePart =>
      annotateReadingClause(rcp.readingClause).map(rc => rcp.copy(readingClause = rc))
    case wcp: QueryPart.WithClausePart =>
      annotateWithClause(wcp.withClause).map(wc => wcp.copy(withClause = wc))
    case ep: QueryPart.EffectPart =>
      annotateEffect(ep.effect).map(e => ep.copy(effect = e))
  }

  def annotateReadingClause(readingClause: ReadingClause): TCEffect[ReadingClause] = readingClause match {
    case fp: ReadingClause.FromPatterns =>
      for {
        annotatedPatterns <- fp.patterns.traverse(annotatePattern)
        annotatedPredicate <- fp.maybePredicate.traverse { pred =>
          for {
            annotated <- annotateAndCheckExpression(pred)
            predType <- getType(annotated)
            _ <- unify(predType, PrimitiveType.Boolean)
          } yield annotated
        }
      } yield fp.copy(patterns = annotatedPatterns, maybePredicate = annotatedPredicate)

    case fu: ReadingClause.FromUnwind =>
      for {
        annotatedList <- annotateAndCheckExpression(fu.list)
        idName = fu.as match {
          case Left(cypher) => cypher.name.name
          case Right(quine) => s"_q${quine.name}"
        }
        freshName <- freshen(Some(idName))
        elementType = TypeVariable(freshName, Constraint.None)
        _ <- addTableEntryForId(fu.as, elementType)
      } yield fu.copy(list = annotatedList)

    case fp: ReadingClause.FromProcedure =>
      for {
        annotatedArgs <- fp.args.traverse(annotateAndCheckExpression)
        // Add type entries for each yield binding (procedure results have unknown types)
        _ <- fp.yields.traverse { yieldItem =>
          for {
            freshName <- freshen(None)
            yieldType = TypeVariable(freshName, Constraint.None)
            _ <- addTableEntryForId(yieldItem.boundAs, yieldType)
          } yield ()
        }
      } yield fp.copy(args = annotatedArgs)

    case fs: ReadingClause.FromSubquery =>
      for {
        annotatedSubquery <- annotateQuery(fs.subquery)
      } yield fs.copy(subquery = annotatedSubquery)
  }

  def annotateWithClause(withClause: WithClause): TCEffect[WithClause] = for {
    annotatedBindings <- withClause.bindings.traverse(annotateProjection)
    annotatedPredicate <- withClause.maybePredicate.traverse { pred =>
      for {
        annotated <- annotateAndCheckExpression(pred)
        predType <- getType(annotated)
        _ <- unify(predType, PrimitiveType.Boolean)
      } yield annotated
    }
  } yield withClause.copy(bindings = annotatedBindings, maybePredicate = annotatedPredicate)

  def annotateEffect(effect: Effect): TCEffect[Effect] = effect match {
    case foreach: Effect.Foreach =>
      for {
        annotatedIn <- annotateAndCheckExpression(foreach.in)
        annotatedEffects <- foreach.effects.traverse(annotateEffect)
      } yield foreach.copy(in = annotatedIn, effects = annotatedEffects)

    case sp: Effect.SetProperty =>
      for {
        annotatedProperty <- annotateFieldAccess(sp.property)
        annotatedValue <- annotateAndCheckExpression(sp.value)
      } yield sp.copy(
        property = annotatedProperty,
        value = annotatedValue,
      )

    case sps: Effect.SetProperties =>
      for {
        annotatedProperties <- annotateAndCheckExpression(sps.properties)
      } yield sps.copy(properties = annotatedProperties)

    case sl: Effect.SetLabel => pure(sl)

    case c: Effect.Create =>
      for {
        annotatedPatterns <- c.patterns.traverse(annotatePattern)
      } yield c.copy(patterns = annotatedPatterns)
  }

  def annotateProjection(projection: Projection): TCEffect[Projection] = for {
    annotatedExp <- annotateAndCheckExpression(projection.expression)
    expType <- getType(annotatedExp)
    _ <- addTableEntryForId(projection.as, expType)
  } yield projection.copy(expression = annotatedExp)

  def annotatePattern(pattern: GraphPattern): TCEffect[GraphPattern] = for {
    annotatedInitial <- annotateNodePattern(pattern.initial)
    annotatedPath <- pattern.path.traverse(annotateConnection)
  } yield pattern.copy(initial = annotatedInitial, path = annotatedPath)

  def annotateConnection(connection: Connection): TCEffect[Connection] = for {
    annotatedDest <- annotateNodePattern(connection.dest)
  } yield connection.copy(dest = annotatedDest)

  def annotateNodePattern(pattern: NodePattern): TCEffect[NodePattern] = for {
    annotatedProps <- pattern.maybeProperties.traverse { props =>
      annotateAndCheckExpression(props)
    }
    _ <- pattern.maybeBinding.traverse_ { id =>
      addTableEntryForId(id, PrimitiveType.NodeType)
    }
  } yield pattern.copy(maybeProperties = annotatedProps)

  // === Symbol Table Initialization ===

  def recordTypesFromSymbolTable: TCEffect[Unit] = for {
    entries <- inspect(_.symbolTable.references)
    _ <- entries.traverse_ {
      case node: NodeEntry =>
        addTableEntry(s"_q${node.identifier}", PrimitiveType.NodeType)
      case _: UnwindEntry =>
        // Type will be inferred when processing the unwind expression
        pure(())
      case _: ExpressionEntry =>
        // Expression types are inferred during traversal
        pure(())
      case _ =>
        // Handle any other entry types
        pure(())
    }
  } yield ()

  // === Entry Point ===

  /** Main entry point for type checking a query.
    *
    * Execution flow:
    * 1. Initialize type environment with any pre-supplied bindings
    * 2. Extract known types from symbol table (e.g., node bindings -> NodeType)
    * 3. Recursively annotate all expressions in the query with types
    *
    * After this phase completes, every expression in the query will have its
    * `ty` field populated with a Type. Type errors are recorded in diagnostics
    * but do not cause phase failure.
    *
    * @param input The Query AST from the parser/symbol analysis phases
    * @return The same Query with type annotations on all expressions
    */
  override def process(input: Query): TCEffect[Query] = for {
    _ <- mod(st => st.copy(typeEnv = initialTypeEnv))
    _ <- recordTypesFromSymbolTable
    annotatedQuery <- annotateQuery(input)
  } yield annotatedQuery
}

object TypeCheckingPhase {
  def apply(initialTypeEnv: Map[Symbol, Type] = Map.empty): TypeCheckingPhase =
    new TypeCheckingPhase(initialTypeEnv)
}
