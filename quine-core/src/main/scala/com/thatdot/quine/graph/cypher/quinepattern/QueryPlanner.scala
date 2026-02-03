package com.thatdot.quine.graph.cypher.quinepattern

import com.thatdot.quine.cypher.phases.SymbolAnalysisModule
import com.thatdot.quine.cypher.{ast => Cypher}
import com.thatdot.quine.language.ast.Direction
import com.thatdot.quine.language.{ast => Pattern}
import com.thatdot.quine.model.EdgeDirection

/** Query Planner for QuinePattern.
  *
  * Converts Cypher AST to QueryPlan, a tree-structured algebra
  * designed for efficient incremental evaluation. The tree property
  * ensures that changes propagate only to ancestors, enabling O(depth)
  * update propagation rather than O(total matches).
  */
object QueryPlanner {

  // ============================================================
  // IDENTIFIER HELPERS
  // ============================================================
  //
  // After symbol analysis, all identifiers are Right(QuineIdentifier) where
  // QuineIdentifier.name is a monotonic Int that correctly handles scoping.
  // We use this Int directly for all internal planner operations.
  // Only at output boundaries do we translate to human-readable names.
  // ============================================================

  /** Exception thrown when an unresolved CypherIdentifier is encountered.
    * This indicates a bug in the compilation pipeline - all identifiers should
    * be resolved to QuineIdentifiers by the symbol analysis phase.
    */
  class UnresolvedIdentifierException(cypherIdent: Pattern.CypherIdentifier)
      extends RuntimeException(
        s"Encountered unresolved CypherIdentifier '${cypherIdent.name}' - " +
        "this indicates a bug in the symbol analysis phase",
      )

  /** Extract the QuineIdentifier from an Either.
    * Use this when you need to create synthetic expressions that reference a binding.
    *
    * @throws UnresolvedIdentifierException if a Left(CypherIdentifier) is encountered
    */
  private def getQuineId(ident: Either[Pattern.CypherIdentifier, Pattern.QuineIdentifier]): Pattern.QuineIdentifier =
    ident match {
      case Right(quineId) => quineId
      case Left(cypherIdent) => throw new UnresolvedIdentifierException(cypherIdent)
    }

  /** Extract the unique Int identifier for internal planner use.
    *
    * After symbol analysis, all identifiers should be Right(QuineIdentifier).
    * The QuineIdentifier.name (Int) is a monotonic ID that correctly handles
    * scoping and shadowing. Use this for all internal planner operations
    * (dependency tracking, pattern matching, etc.).
    *
    * @throws UnresolvedIdentifierException if a Left(CypherIdentifier) is encountered
    */
  private def identInt(ident: Either[Pattern.CypherIdentifier, Pattern.QuineIdentifier]): Int =
    getQuineId(ident).name

  /** Create an Expression.Ident that references the given binding ID.
    * Use this when creating synthetic expressions that reference a binding.
    */
  private def makeIdentExpr(bindingId: Int): Pattern.Expression =
    Pattern.Expression.Ident(
      Pattern.Source.NoSource,
      Right(Pattern.QuineIdentifier(bindingId)),
      None,
    )

  /** Create an Expression.IdLookup for the given binding ID.
    * Used for diamond join conditions: id(renamed) = id(original)
    */
  private def makeIdLookupExpr(bindingId: Int): Pattern.Expression =
    Pattern.Expression.IdLookup(
      Pattern.Source.NoSource,
      Right(Pattern.QuineIdentifier(bindingId)),
      None,
    )

  /** Create a join filter predicate for diamond bindings.
    * For each rename (original, renamed), creates: id(renamed) = id(original)
    * Multiple renames are combined with AND.
    */
  private def makeDiamondJoinPredicate(renames: List[BindingRename]): Option[Pattern.Expression] =
    renames match {
      case Nil => None
      case _ =>
        val predicates: List[Pattern.Expression] = renames.map { case BindingRename(original, renamed) =>
          Pattern.Expression.BinOp(
            Pattern.Source.NoSource,
            Pattern.Operator.Equals,
            makeIdLookupExpr(renamed),
            makeIdLookupExpr(original),
            None,
          )
        }
        // Combine with AND if multiple predicates
        Some(predicates.reduceLeft { (acc, pred) =>
          Pattern.Expression.BinOp(
            Pattern.Source.NoSource,
            Pattern.Operator.And,
            acc,
            pred,
            None,
          )
        })
    }

  /** Convert an Int binding ID to a Symbol for use in QueryPlan nodes.
    * This format matches identKey in QuinePatternExpressionInterpreter for consistent lookup.
    * We use the raw integer from symbol analysis directly - no prefix needed since these
    * are guaranteed unique within a query. Human-readable names are applied at output
    * boundaries via outputNameMapping.
    */
  private def bindingSymbol(bindingId: Int): Symbol = Symbol(bindingId.toString)

  /** Look up the human-readable name for an identifier from the symbol table.
    * Use this for user-facing output (RETURN column names) where users should
    * see the names they used in their query.
    */
  private def identDisplayName(
    ident: Either[Pattern.CypherIdentifier, Pattern.QuineIdentifier],
    symbolTable: SymbolAnalysisModule.SymbolTable,
  ): Symbol =
    ident match {
      case Left(cypherIdent) => cypherIdent.name
      case Right(quineId) =>
        symbolTable.references
          .collectFirst {
            case entry: SymbolAnalysisModule.SymbolTableEntry.QuineToCypherIdEntry
                if entry.identifier == quineId.name =>
              entry.cypherIdentifier
          }
          .getOrElse(Symbol(s"anon_${quineId.name}"))
    }

  /** Exception thrown when a node pattern is missing a binding after symbol analysis.
    * This indicates a bug in the symbol analysis phase - all node patterns should
    * have bindings assigned (even anonymous nodes get fresh IDs).
    */
  class MissingBindingException(pattern: Cypher.NodePattern)
      extends RuntimeException(
        s"Node pattern at ${pattern.source} is missing a binding - " +
        "this indicates a bug in the symbol analysis phase",
      )

  /** Extract the binding Int from a node pattern.
    *
    * After symbol analysis, all node patterns should have bindings assigned
    * (even anonymous nodes get fresh IDs). This function extracts the binding
    * and throws an error if it's missing.
    *
    * @throws MissingBindingException if maybeBinding is None
    * @throws UnresolvedIdentifierException if the binding is Left(CypherIdentifier)
    */
  private def nodeBindingInt(pattern: Cypher.NodePattern): Int =
    pattern.maybeBinding match {
      case Some(ident) => identInt(ident)
      case None => throw new MissingBindingException(pattern)
    }

  // ============================================================
  // NODE DEPENDENCIES
  // ============================================================

  /** What a query needs from a particular node */
  sealed trait NodeDep
  object NodeDep {
    case object Id extends NodeDep
    case class Property(name: Symbol) extends NodeDep
    case object Labels extends NodeDep
    case object AllProperties extends NodeDep
    case object Node extends NodeDep // Full node value (id + labels + properties)
  }

  /** Map from node binding ID (Int) to its dependencies */
  type NodeDeps = Map[Int, Set[NodeDep]]

  object NodeDeps {
    val empty: NodeDeps = Map.empty

    def combine(a: NodeDeps, b: NodeDeps): NodeDeps =
      (a.keySet ++ b.keySet).map { key =>
        key -> (a.getOrElse(key, Set.empty) ++ b.getOrElse(key, Set.empty))
      }.toMap
  }

  // ============================================================
  // ID LOOKUPS (WHERE id(n) = ...)
  // ============================================================

  /** Records an ID constraint from WHERE clause */
  case class IdLookup(forName: Int, exp: Pattern.Expression) {

    /** Get the variable references in this lookup's expression.
      * These are the bindings (as Int IDs) that must be in scope before this lookup can be evaluated.
      */
    def dependencies: Set[Int] = extractVariableRefs(exp)
  }

  /** Extract variable references from an expression (not parameters).
    * Returns the set of binding IDs (Int) referenced in the expression.
    * Note: Expressions with Left(CypherIdentifier) (e.g., FOREACH bindings that weren't
    * resolved by symbol analysis) are skipped since they don't have Int IDs.
    */
  def extractVariableRefs(expr: Pattern.Expression): Set[Int] = expr match {
    case Pattern.Expression.Ident(_, ident, _) =>
      ident match {
        case Right(quineId) => Set(quineId.name)
        case Left(_) => Set.empty // FOREACH bindings etc. - not tracked as Int IDs
      }

    case Pattern.Expression.FieldAccess(_, on, _, _) =>
      extractVariableRefs(on)

    case Pattern.Expression.BinOp(_, _, lhs, rhs, _) =>
      extractVariableRefs(lhs) ++ extractVariableRefs(rhs)

    case Pattern.Expression.Apply(_, _, args, _) =>
      args.flatMap(extractVariableRefs).toSet

    case Pattern.Expression.SynthesizeId(_, args, _) =>
      // idFrom(...) - extract refs from arguments
      args.flatMap(extractVariableRefs).toSet

    case Pattern.Expression.UnaryOp(_, _, operand, _) =>
      extractVariableRefs(operand)

    case Pattern.Expression.IsNull(_, of, _) =>
      extractVariableRefs(of)

    case Pattern.Expression.ListLiteral(_, elements, _) =>
      elements.flatMap(extractVariableRefs).toSet

    case Pattern.Expression.MapLiteral(_, entries, _) =>
      entries.values.flatMap(extractVariableRefs).toSet

    case _: Pattern.Expression.Parameter => Set.empty // Parameters are not variable refs
    case _: Pattern.Expression.AtomicLiteral => Set.empty
    case _: Pattern.Expression.IdLookup => Set.empty // id(n) is handled separately
    case _ => Set.empty
  }

  /** Extract ID lookups from WHERE predicates.
    *
    * An IdLookup is only created when one side is id(node) and the other side
    * is a computable expression (idFrom, parameter, literal). When both sides
    * are id(node) expressions like `id(a) = id(m)`, this is a join condition
    * (same-node constraint) not an anchor computation, so we don't create an IdLookup.
    */
  def extractIdLookups(query: Cypher.Query): List[IdLookup] = {
    def fromExpression(expr: Pattern.Expression): List[IdLookup] = expr match {
      case Pattern.Expression.BinOp(_, Pattern.Operator.Equals, lhs, rhs, _) =>
        (lhs, rhs) match {
          // Skip id(a) = id(b) - this is a join condition, not an anchor computation
          case (_: Pattern.Expression.IdLookup, _: Pattern.Expression.IdLookup) =>
            Nil
          case (Pattern.Expression.IdLookup(_, nodeId, _), value) =>
            List(IdLookup(identInt(nodeId), value))
          case (value, Pattern.Expression.IdLookup(_, nodeId, _)) =>
            List(IdLookup(identInt(nodeId), value))
          case _ => Nil
        }
      case Pattern.Expression.BinOp(_, Pattern.Operator.And, lhs, rhs, _) =>
        fromExpression(lhs) ++ fromExpression(rhs)
      case _ => Nil
    }

    def fromQueryPart(part: Cypher.QueryPart): List[IdLookup] = part match {
      case Cypher.QueryPart.ReadingClausePart(readingClause) =>
        readingClause match {
          case patterns: Cypher.ReadingClause.FromPatterns =>
            patterns.maybePredicate.toList.flatMap(fromExpression)
          case _ => Nil
        }
      case _ => Nil
    }

    query match {
      case single: Cypher.Query.SingleQuery =>
        single match {
          case spq: Cypher.Query.SingleQuery.SinglepartQuery =>
            spq.queryParts.flatMap(fromQueryPart)
          case mpq: Cypher.Query.SingleQuery.MultipartQuery =>
            val allParts = mpq.queryParts ++ mpq.into.queryParts
            allParts.flatMap(fromQueryPart)
        }
      case _: Cypher.Query.Union => Nil // TODO: Handle union
    }
  }

  // ============================================================
  // DEPENDENCY EXTRACTION
  // ============================================================

  /** Check if a binding ID refers to a node or edge (graph element) in the symbol table.
    * Only graph elements have properties that can be watched via LocalProperty.
    * Other bindings (expressions, UNWIND, FOREACH) need runtime field access.
    */
  private def isGraphElementBinding(
    bindingId: Int,
    symbolTable: SymbolAnalysisModule.SymbolTable,
  ): Boolean =
    symbolTable.references.exists { entry =>
      entry.identifier == bindingId && (entry match {
        case _: SymbolAnalysisModule.SymbolTableEntry.NodeEntry => true
        case _: SymbolAnalysisModule.SymbolTableEntry.EdgeEntry => true
        case _ => false
      })
    }

  /** Extract node dependencies from an expression.
    *
    * @param expr The expression to analyze
    * @param symbolTable The symbol table from symbol analysis, used to determine binding types
    */
  def extractDepsFromExpr(
    expr: Pattern.Expression,
    symbolTable: SymbolAnalysisModule.SymbolTable,
  ): NodeDeps = expr match {
    case Pattern.Expression.IdLookup(_, nodeId, _) =>
      Map(identInt(nodeId) -> Set[NodeDep](NodeDep.Id))

    case Pattern.Expression.FieldAccess(_, on, fieldName, _) =>
      on match {
        case Pattern.Expression.Ident(_, ident, _) =>
          val bindingId = identInt(ident)
          // Always emit NodeDep.Property for field access on an identifier.
          // At runtime, if the identifier is not a node, this will be a no-op.
          // This conservative approach ensures we always watch properties for nodes.
          Map(bindingId -> Set[NodeDep](NodeDep.Property(fieldName)))
        case _ => extractDepsFromExpr(on, symbolTable)
      }

    case Pattern.Expression.BinOp(_, op, lhs, rhs, _) =>
      // Check if this is an anchoring equality: id(...) = expr (but NOT id(a) = id(b))
      // id(a) = id(b) is a JOIN condition and needs both NodeDep.Ids
      // id(a) = idFrom(...) is ANCHORING and doesn't need the node ID in context
      val lhsIsIdLookup = lhs.isInstanceOf[Pattern.Expression.IdLookup]
      val rhsIsIdLookup = rhs.isInstanceOf[Pattern.Expression.IdLookup]
      val isJoinCondition = lhsIsIdLookup && rhsIsIdLookup
      val isAnchoringEquality = op == Pattern.Operator.Equals && (lhsIsIdLookup || rhsIsIdLookup) && !isJoinCondition

      if (isAnchoringEquality) {
        // Extract deps but skip the IdLookup - it's for anchoring, not context
        val lhsDeps = lhs match {
          case _: Pattern.Expression.IdLookup => NodeDeps.empty
          case _ => extractDepsFromExpr(lhs, symbolTable)
        }
        val rhsDeps = rhs match {
          case _: Pattern.Expression.IdLookup => NodeDeps.empty
          case _ => extractDepsFromExpr(rhs, symbolTable)
        }
        NodeDeps.combine(lhsDeps, rhsDeps)
      } else {
        NodeDeps.combine(extractDepsFromExpr(lhs, symbolTable), extractDepsFromExpr(rhs, symbolTable))
      }

    case Pattern.Expression.Apply(_, name, args, _) =>
      val argDeps = args.map(extractDepsFromExpr(_, symbolTable)).foldLeft(NodeDeps.empty)(NodeDeps.combine)
      // strId needs node identity - add NodeDep.Id for the argument if it's a node binding
      if (name.name == "strId" && args.nonEmpty) {
        args.head match {
          case Pattern.Expression.Ident(_, ident, _) =>
            ident match {
              case Right(bindingId) if isGraphElementBinding(bindingId.name, symbolTable) =>
                NodeDeps.combine(argDeps, Map(bindingId.name -> Set[NodeDep](NodeDep.Id)))
              case _ => argDeps
            }
          case _ => argDeps
        }
      } else {
        argDeps
      }

    case Pattern.Expression.UnaryOp(_, _, operand, _) =>
      extractDepsFromExpr(operand, symbolTable)

    case Pattern.Expression.IsNull(_, of, _) =>
      extractDepsFromExpr(of, symbolTable)

    case Pattern.Expression.CaseBlock(_, cases, alternative, _) =>
      val caseDeps = cases
        .flatMap { c =>
          List(extractDepsFromExpr(c.condition, symbolTable), extractDepsFromExpr(c.value, symbolTable))
        }
        .foldLeft(NodeDeps.empty)(NodeDeps.combine)
      val altDeps = extractDepsFromExpr(alternative, symbolTable)
      NodeDeps.combine(caseDeps, altDeps)

    case Pattern.Expression.ListLiteral(_, elements, _) =>
      elements.map(extractDepsFromExpr(_, symbolTable)).foldLeft(NodeDeps.empty)(NodeDeps.combine)

    case Pattern.Expression.MapLiteral(_, entries, _) =>
      entries.values.toList.map(extractDepsFromExpr(_, symbolTable)).foldLeft(NodeDeps.empty)(NodeDeps.combine)

    // Bare identifier referencing a node/edge binding requires full node value
    // This handles RETURN n where n is a node - we need id + labels + properties
    case Pattern.Expression.Ident(_, ident, _) =>
      ident match {
        case Right(bindingId) if isGraphElementBinding(bindingId.name, symbolTable) =>
          Map(bindingId.name -> Set[NodeDep](NodeDep.Node))
        case _ => NodeDeps.empty
      }

    case _ => NodeDeps.empty
  }

  /** Extract dependencies from a query */
  def getNodeDeps(query: Cypher.Query, symbolTable: SymbolAnalysisModule.SymbolTable): NodeDeps = {
    def fromQueryPart(part: Cypher.QueryPart): NodeDeps = part match {
      case Cypher.QueryPart.ReadingClausePart(readingClause) =>
        readingClause match {
          case patterns: Cypher.ReadingClause.FromPatterns =>
            patterns.maybePredicate.map(extractDepsFromExpr(_, symbolTable)).getOrElse(NodeDeps.empty)
          case _ => NodeDeps.empty
        }
      case Cypher.QueryPart.WithClausePart(withClause) =>
        // Extract dependencies from WITH clause projections (e.g., n.prop in WITH n.prop AS x)
        val bindingDeps = withClause.bindings
          .map(p => extractDepsFromExpr(p.expression, symbolTable))
          .foldLeft(NodeDeps.empty)(NodeDeps.combine)
        // Also extract from WHERE predicate if present
        val predicateDeps = withClause.maybePredicate.map(extractDepsFromExpr(_, symbolTable)).getOrElse(NodeDeps.empty)
        NodeDeps.combine(bindingDeps, predicateDeps)
      case Cypher.QueryPart.EffectPart(effect) =>
        // Extract dependencies from effect expressions (e.g., SET n.prop = expr)
        extractDepsFromEffect(effect, symbolTable)
      case _ => NodeDeps.empty
    }

    def fromProjections(projections: List[Cypher.Projection]): NodeDeps =
      projections.map(p => extractDepsFromExpr(p.expression, symbolTable)).foldLeft(NodeDeps.empty)(NodeDeps.combine)

    query match {
      case single: Cypher.Query.SingleQuery =>
        single match {
          case spq: Cypher.Query.SingleQuery.SinglepartQuery =>
            val partDeps = spq.queryParts.map(fromQueryPart).foldLeft(NodeDeps.empty)(NodeDeps.combine)
            val bindingDeps = fromProjections(spq.bindings)
            NodeDeps.combine(partDeps, bindingDeps)
          case mpq: Cypher.Query.SingleQuery.MultipartQuery =>
            val allParts = mpq.queryParts ++ mpq.into.queryParts
            val partDeps = allParts.map(fromQueryPart).foldLeft(NodeDeps.empty)(NodeDeps.combine)
            val bindingDeps = fromProjections(mpq.into.bindings)
            NodeDeps.combine(partDeps, bindingDeps)
        }
      case _: Cypher.Query.Union => NodeDeps.empty
    }
  }

  /** Extract dependencies from effect clauses */
  private def extractDepsFromEffect(effect: Cypher.Effect, symbolTable: SymbolAnalysisModule.SymbolTable): NodeDeps =
    effect match {
      case Cypher.Effect.SetProperty(_, _, value) => extractDepsFromExpr(value, symbolTable)
      case Cypher.Effect.SetProperties(_, _, props) => extractDepsFromExpr(props, symbolTable)
      case Cypher.Effect.Create(_, patterns) =>
        // CREATE edges need node identity for both source and destination bindings
        // Extract all node bindings involved in CREATE patterns
        val bindingIds: List[Int] = patterns.flatMap { pattern =>
          val initial = pattern.initial.maybeBinding.flatMap(_.toOption).map(_.name)
          val path = pattern.path.flatMap(conn => conn.dest.maybeBinding.flatMap(_.toOption).map(_.name))
          initial.toList ++ path
        }
        // Add NodeDep.Id for each binding involved in CREATE
        bindingIds.foldLeft(NodeDeps.empty) { (deps, bindingId) =>
          NodeDeps.combine(deps, Map(bindingId -> Set[NodeDep](NodeDep.Id)))
        }
      case Cypher.Effect.Foreach(_, _, listExpr, nestedEffects) =>
        // Check if nested effects contain CREATE - if so, list elements need NodeDep.Id
        val hasCreate = nestedEffects.exists {
          case _: Cypher.Effect.Create => true
          case _ => false
        }

        // Extract deps from the list expression
        // If there's a CREATE in the body, node bindings in the list need Id (for edge targets)
        val listDeps = if (hasCreate) {
          // Add NodeDep.Id for any node binding identifiers in the list
          def extractIdDeps(expr: Pattern.Expression): NodeDeps = expr match {
            case Pattern.Expression.Ident(_, ident, _) =>
              ident match {
                case Right(bindingId) if isGraphElementBinding(bindingId.name, symbolTable) =>
                  Map(bindingId.name -> Set[NodeDep](NodeDep.Id))
                case _ => NodeDeps.empty
              }
            case Pattern.Expression.ListLiteral(_, elements, _) =>
              elements.map(extractIdDeps).foldLeft(NodeDeps.empty)(NodeDeps.combine)
            case _ => extractDepsFromExpr(expr, symbolTable)
          }
          extractIdDeps(listExpr)
        } else {
          extractDepsFromExpr(listExpr, symbolTable)
        }

        // Recursively extract deps from nested effects
        val nestedDeps =
          nestedEffects.map(extractDepsFromEffect(_, symbolTable)).foldLeft(NodeDeps.empty)(NodeDeps.combine)
        NodeDeps.combine(listDeps, nestedDeps)
      case _ => NodeDeps.empty
    }

  /** Extract alias mappings from WITH clauses: destination binding ID -> source binding ID.
    * This tracks when a WITH clause renames/aliases a binding, e.g., WITH m AS movie.
    */
  def extractWithAliases(query: Cypher.Query): Map[Int, Int] = {
    def fromProjection(proj: Cypher.Projection): Option[(Int, Int)] =
      // If the expression is just an identifier, this is an alias
      proj.expression match {
        case Pattern.Expression.Ident(_, ident, _) =>
          ident match {
            case Right(sourceId) =>
              proj.as match {
                case Right(destId) if destId.name != sourceId.name =>
                  Some(destId.name -> sourceId.name)
                case _ => None
              }
            case _ => None
          }
        case _ => None
      }

    def fromQueryPart(part: Cypher.QueryPart): Map[Int, Int] = part match {
      case Cypher.QueryPart.WithClausePart(withClause) =>
        withClause.bindings.flatMap(fromProjection).toMap
      case _ => Map.empty
    }

    query match {
      case single: Cypher.Query.SingleQuery =>
        single match {
          case spq: Cypher.Query.SingleQuery.SinglepartQuery =>
            spq.queryParts.flatMap(p => fromQueryPart(p).toList).toMap
          case mpq: Cypher.Query.SingleQuery.MultipartQuery =>
            val allParts = mpq.queryParts ++ mpq.into.queryParts
            allParts.flatMap(p => fromQueryPart(p).toList).toMap
        }
      case _ => Map.empty
    }
  }

  /** Propagate NodeDep.Id back through WITH alias chains.
    * If binding 6 has NodeDep.Id and 6 is an alias for 3 which is an alias for 2,
    * then 3 and 2 should also get NodeDep.Id.
    */
  def propagateIdDepsBackward(deps: NodeDeps, aliases: Map[Int, Int]): NodeDeps = {
    // For each binding with NodeDep.Id, trace back through aliases
    val idBindings = deps.filter(_._2.contains(NodeDep.Id)).keys.toSet

    def traceBack(binding: Int, visited: Set[Int]): Set[Int] =
      if (visited.contains(binding)) visited
      else {
        aliases.get(binding) match {
          case Some(source) => traceBack(source, visited + binding)
          case None => visited + binding
        }
      }

    val allBindingsNeedingId = idBindings.flatMap(b => traceBack(b, Set.empty))

    // Add NodeDep.Id to all bindings in the chain
    val additionalDeps: NodeDeps = allBindingsNeedingId.map(b => b -> Set[NodeDep](NodeDep.Id)).toMap
    NodeDeps.combine(deps, additionalDeps)
  }

  // ============================================================
  // GRAPH PATTERN TREE
  // ============================================================

  /** Connection from one node to another via edge */
  case class ConnectionInfo(edgeLabel: Symbol, direction: Direction, tree: GraphPatternTree)

  /** Tree representation of graph pattern for planning */
  sealed trait GraphPatternTree
  object GraphPatternTree {
    case class Branch(binding: Int, labels: Set[Symbol], children: List[ConnectionInfo]) extends GraphPatternTree
    case object Empty extends GraphPatternTree
  }

  /** Build pattern tree from Cypher graph pattern */
  def buildPatternTree(pattern: Cypher.GraphPattern): GraphPatternTree = {
    val initBinding = nodeBindingInt(pattern.initial)
    val initLabels = pattern.initial.labels

    def loop(binding: Int, labels: Set[Symbol], path: List[Cypher.Connection]): GraphPatternTree =
      path match {
        case Nil =>
          GraphPatternTree.Branch(binding, labels, Nil)

        case head :: tail =>
          val destBinding = nodeBindingInt(head.dest)
          val destLabels = head.dest.labels
          val edgeLabel = head.edge.edgeType

          val childTree = loop(destBinding, destLabels, tail)
          val connection = ConnectionInfo(edgeLabel, head.edge.direction, childTree)

          GraphPatternTree.Branch(binding, labels, List(connection))
      }

    loop(initBinding, initLabels, pattern.path)
  }

  /** Collect all bindings (as Int IDs) from a tree (not just the root) */
  def collectBindings(tree: GraphPatternTree): Set[Int] = tree match {
    case GraphPatternTree.Branch(binding, _, children) =>
      Set(binding) ++ children.flatMap(c => collectBindings(c.tree))
    case GraphPatternTree.Empty => Set.empty
  }

  /** Records that a binding was renamed to avoid diamond conflicts.
    * The Filter `id(renamed) == id(original)` must be applied.
    */
  case class BindingRename(original: Int, renamed: Int)

  /** Deduplicate bindings within a tree to handle diamond patterns.
    *
    * When merging trees that share bindings, the same binding can appear in multiple
    * subtrees (e.g., `p2` in both `f<-e2<-p2` and `f<-e3<-p2->e4->ip`). This creates
    * a "diamond" where the same logical node appears in multiple branches of a CrossProduct.
    *
    * This function:
    * 1. Traverses the tree tracking seen bindings
    * 2. When a binding is seen a second time, assigns it a fresh internal binding
    * 3. Returns the transformed tree and a list of (original, renamed) pairs
    *
    * The caller must then add Filter nodes to verify that renamed bindings have the
    * same ID as their original bindings.
    *
    * @param tree The tree to deduplicate
    * @param seen Bindings already seen (for recursive calls)
    * @param nextFresh Next available fresh binding ID
    * @return (transformed tree, updated seen set, updated nextFresh, list of renames)
    */
  def deduplicateBindings(
    tree: GraphPatternTree.Branch,
    seen: Set[Int] = Set.empty,
    nextFresh: Int = 10000, // Start high to avoid conflicts with real bindings
  ): (GraphPatternTree.Branch, Set[Int], Int, List[BindingRename]) = {
    // Check if this binding is a duplicate
    val (newBinding, newSeen, newFresh, rootRename) =
      if (seen.contains(tree.binding)) {
        // Duplicate! Assign fresh binding and record the rename
        (nextFresh, seen + nextFresh, nextFresh + 1, List(BindingRename(tree.binding, nextFresh)))
      } else {
        // First occurrence - keep the binding
        (tree.binding, seen + tree.binding, nextFresh, Nil)
      }

    // Recursively process children
    val (newChildren, finalSeen, finalFresh, childRenames) =
      tree.children.foldLeft((List.empty[ConnectionInfo], newSeen, newFresh, List.empty[BindingRename])) {
        case ((accChildren, accSeen, accFresh, accRenames), conn) =>
          conn.tree match {
            case branch: GraphPatternTree.Branch =>
              val (dedupedBranch, nextSeen, nextFreshId, branchRenames) =
                deduplicateBindings(branch, accSeen, accFresh)
              val newConn = ConnectionInfo(conn.edgeLabel, conn.direction, dedupedBranch)
              (accChildren :+ newConn, nextSeen, nextFreshId, accRenames ++ branchRenames)
            case GraphPatternTree.Empty =>
              (accChildren :+ conn, accSeen, accFresh, accRenames)
          }
      }

    val newTree = GraphPatternTree.Branch(newBinding, tree.labels, newChildren)
    (newTree, finalSeen, finalFresh, rootRename ++ childRenames)
  }

  /** Reverse edge direction */
  private def reverseDirection(dir: Direction): Direction = dir match {
    case Direction.Left => Direction.Right
    case Direction.Right => Direction.Left
  }

  /** Re-root a tree to a different binding.
    *
    * Given a tree like: a -[e1]-> b -[e2]-> c
    * And newRoot = b, produces: b -[e1 reversed]-> a
    *                              -[e2]-> c
    *
    * Returns None if the binding is not in the tree.
    */
  def rerootTree(tree: GraphPatternTree.Branch, newRoot: Int): Option[GraphPatternTree.Branch] =
    if (tree.binding == newRoot) {
      // Already rooted at the target
      Some(tree)
    } else {
      // Find path from current root to newRoot
      findPathAndReroot(tree, newRoot, None)
    }

  /** Helper: Find newRoot in tree and reroot, carrying parent info for edge reversal */
  private def findPathAndReroot(
    tree: GraphPatternTree.Branch,
    newRoot: Int,
    parentInfo: Option[(Int, Set[Symbol], Symbol, Direction)], // (parentBinding, parentLabels, edgeLabel, edgeDir)
  ): Option[GraphPatternTree.Branch] =
    if (tree.binding == newRoot) {
      // Found the target - build new tree with this as root
      // Add reversed edge to parent (if any)
      val parentConnection: List[ConnectionInfo] = parentInfo.toList.map {
        case (parentBinding, parentLabels, edgeLabel, edgeDir) =>
          ConnectionInfo(
            edgeLabel,
            reverseDirection(edgeDir),
            GraphPatternTree.Branch(parentBinding, parentLabels, Nil),
          )
      }
      Some(GraphPatternTree.Branch(tree.binding, tree.labels, parentConnection ++ tree.children))
    } else {
      // Search children for newRoot
      tree.children.view.flatMap { conn =>
        conn.tree match {
          case childBranch: GraphPatternTree.Branch =>
            // Try to find newRoot in this subtree
            val result = findPathAndReroot(
              childBranch,
              newRoot,
              Some((tree.binding, tree.labels, conn.edgeLabel, conn.direction)),
            )
            result.map { rerootedChild =>
              // The child was rerooted - now we need to extend tree.binding (our node)
              // with its other children and a connection to its parent (if any).
              // tree.binding was created with Nil children at the deepest level,
              // so we need to update it now with the full information.
              val otherChildren = tree.children.filterNot(_ eq conn)

              // Build the parent edge if we have a parent
              val parentEdge: List[ConnectionInfo] = parentInfo.toList.map {
                case (parentBinding, parentLabels, edgeLabel, edgeDir) =>
                  ConnectionInfo(
                    edgeLabel,
                    reverseDirection(edgeDir),
                    GraphPatternTree.Branch(parentBinding, parentLabels, Nil),
                  )
              }

              // Find tree.binding in the rerooted tree and extend it
              def extendAncestor(t: GraphPatternTree.Branch): GraphPatternTree.Branch =
                if (t.binding == tree.binding) {
                  // Found our node - add siblings and parent edge
                  t.copy(children = t.children ++ otherChildren ++ parentEdge)
                } else {
                  // Recurse into children to find our node
                  val updatedChildren = t.children.map { c =>
                    c.tree match {
                      case b: GraphPatternTree.Branch =>
                        ConnectionInfo(c.edgeLabel, c.direction, extendAncestor(b))
                      case _ => c
                    }
                  }
                  t.copy(children = updatedChildren)
                }

              extendAncestor(rerootedChild)
            }
          case GraphPatternTree.Empty => None
        }
      }.headOption
    }

  /** Merge pattern trees that share the same root binding */
  def mergeTrees(trees: List[GraphPatternTree.Branch]): GraphPatternTree.Branch =
    trees.reduce { (a, b) =>
      require(a.binding == b.binding, s"Cannot merge trees with different roots: ${a.binding} vs ${b.binding}")
      GraphPatternTree.Branch(a.binding, a.labels ++ b.labels, a.children ++ b.children)
    }

  /** Merge multiple pattern trees that may share bindings.
    *
    * If trees share a binding (even if not at the root), this will:
    * 1. Find the shared binding(s)
    * 2. Re-root all trees containing a shared binding to that binding
    * 3. Merge the re-rooted trees
    *
    * Trees that don't share any bindings remain separate (will become CrossProduct).
    *
    * @param trees The pattern trees to merge
    * @param idLookups ID lookups from WHERE clause - used to prefer anchored bindings as root
    */
  def mergeTreesWithSharedBindings(
    trees: List[GraphPatternTree.Branch],
    idLookups: List[IdLookup] = Nil,
  ): List[GraphPatternTree.Branch] = {
    if (trees.size <= 1) return trees

    // Collect all bindings from each tree
    val treeBindings: List[(GraphPatternTree.Branch, Set[Int])] =
      trees.map(t => (t, collectBindings(t)))

    // Find bindings that appear in multiple trees
    val allBindings: List[Int] = treeBindings.flatMap(_._2)
    val bindingCounts: Map[Int, Int] = allBindings.groupBy(identity).view.mapValues(_.size).toMap
    val sharedBindings: Set[Int] = bindingCounts.filter(_._2 > 1).keySet

    if (sharedBindings.isEmpty) {
      // No shared bindings - return trees as-is (will become CrossProduct)
      trees
    } else {
      // Pick a shared binding as the common root
      // PRIORITY ORDER:
      // 1. Bindings with ID lookups (enables Computed anchor instead of AllNodes)
      // 2. Bindings that appear in the most trees (for maximum merging)
      // 3. Smallest binding ID (for deterministic behavior)
      val idLookupBindings = idLookups.map(_.forName).toSet
      val commonRoot: Int = sharedBindings.toList.sortBy { b =>
        val hasIdLookup = if (idLookupBindings.contains(b)) 0 else 1 // 0 = has lookup (preferred)
        val negCount = -bindingCounts(b) // negative so higher counts sort first
        (hasIdLookup, negCount, b) // tertiary sort by binding ID for determinism
      }.head

      // Partition trees into those containing the shared binding and those that don't
      val (containingShared, notContainingShared) = treeBindings.partition(_._2.contains(commonRoot))

      // Re-root trees containing the shared binding
      val rerooted: List[GraphPatternTree.Branch] = containingShared.flatMap { case (tree, _) =>
        rerootTree(tree, commonRoot)
      }

      // Merge all re-rooted trees (they now share the same root)
      val merged: GraphPatternTree.Branch = if (rerooted.size == 1) {
        rerooted.head
      } else {
        mergeTrees(rerooted)
      }

      // Recursively process remaining trees (they might share bindings among themselves)
      val remainingTrees = notContainingShared.map(_._1)
      if (remainingTrees.isEmpty) {
        List(merged)
      } else {
        merged :: mergeTreesWithSharedBindings(remainingTrees, idLookups)
      }
    }
  }

  // ============================================================
  // PLAN GENERATION
  // ============================================================

  /** Generate watch operators for a node's dependencies.
    *
    * MVSQ-style approach:
    * - LocalId: provides the node binding with ID and extracted properties
    * - LocalLabels: watches for label constraints (e.g., MATCH (n:Person))
    * - LocalProperty: watches for property constraints (e.g., WHERE n.prop IS NOT NULL)
    *
    * Property constraints from IS NOT NULL predicates become LocalProperty watches
    * with Any constraint, which only emits when the property exists.
    *
    * @param binding The binding ID for this node
    * @param labels Labels from the pattern (e.g., :Person)
    * @param deps Node dependencies from expression analysis
    * @param isNotNullConstraints IS NOT NULL constraints as (bindingId, property) pairs
    * @param propertyEqualities Property equality constraints for predicate pushdown
    */
  def generateWatches(
    binding: Int,
    labels: Set[Symbol],
    deps: NodeDeps,
    isNotNullConstraints: List[(Int, Symbol)] = Nil,
    propertyEqualities: List[PropertyEquality] = Nil,
  ): List[QueryPlan] = {
    val myDeps = deps.getOrElse(binding, Set.empty)

    // Properties with IS NOT NULL constraints for this binding
    val isNotNullProps: Set[Symbol] = isNotNullConstraints.collect {
      case (b, prop) if b == binding => prop
    }.toSet

    // Property equality constraints for this binding: property -> value
    val equalityConstraints: Map[Symbol, Pattern.Value] = propertyEqualities.collect {
      case PropertyEquality(b, prop, value) if b == binding => prop -> value
    }.toMap

    // All properties accessed in expressions for this binding
    val accessedProperties: Set[Symbol] = myDeps.collect { case NodeDep.Property(name) => name }

    // Convert binding ID to Symbol for QueryPlan nodes
    val bindingSym = bindingSymbol(binding)

    // LocalId binds just the node ID - only emit when needed:
    // 1. Explicit id(n) usage in expressions (NodeDep.Id)
    // 2. Diamond patterns where bindings need identity comparison (added via depsWithRenames)
    // 3. CREATE effects that need node identity for edge creation (added via extractDepsFromEffect)
    val idWatch: List[QueryPlan] =
      if (myDeps.contains(NodeDep.Id)) List(QueryPlan.LocalId(bindingSym))
      else Nil

    // Label constraints for pattern matching (e.g., MATCH (n:Person))
    val labelWatch: List[QueryPlan] =
      if (labels.nonEmpty) {
        List(QueryPlan.LocalLabels(None, LabelConstraint.Contains(labels)))
      } else if (myDeps.contains(NodeDep.Labels)) {
        List(QueryPlan.LocalLabels(Some(bindingSym), LabelConstraint.Unconditional))
      } else {
        Nil
      }

    // When a query uses properties(n) function, emit LocalAllProperties
    // This provides just the properties as a Map (excluding labelsProperty)
    val allPropertiesWatch: List[QueryPlan] =
      if (myDeps.contains(NodeDep.AllProperties)) {
        List(QueryPlan.LocalAllProperties(bindingSym))
      } else {
        Nil
      }

    // When a query needs the full node value (e.g., RETURN n where n is a node),
    // emit LocalNode to provide id + labels + properties as a complete Value.Node
    val nodeWatch: List[QueryPlan] =
      if (myDeps.contains(NodeDep.Node)) {
        List(QueryPlan.LocalNode(bindingSym))
      } else {
        Nil
      }

    // Generate LocalProperty for each accessed property with appropriate constraint:
    // 1. If property has equality constraint: Equal(value)
    // 2. If property has IS NOT NULL constraint: Any
    // 3. Otherwise: Unconditional (just extract the value)
    val propertyWatches: List[QueryPlan] = accessedProperties.toList.map { prop =>
      val constraint = equalityConstraints.get(prop) match {
        case Some(value) =>
          // Convert Pattern.Value to language.ast.Value for PropertyConstraint.Equal
          PropertyConstraint.Equal(value)
        case None if isNotNullProps.contains(prop) =>
          PropertyConstraint.Any
        case None =>
          PropertyConstraint.Unconditional
      }
      // Always alias the property value so it's accessible in expressions
      // Format: "{bindingId}.{propName}" e.g., "1.name" for node binding 1's name property
      QueryPlan.LocalProperty(prop, aliasAs = Some(Symbol(s"${binding}.${prop.name}")), constraint)
    }

    // Also generate LocalProperty for IS NOT NULL properties not otherwise accessed
    val additionalIsNotNullWatches: List[QueryPlan] = (isNotNullProps -- accessedProperties).toList.map { prop =>
      QueryPlan.LocalProperty(prop, aliasAs = None, PropertyConstraint.Any)
    }

    // Also generate LocalProperty for equality constraints not otherwise accessed
    val additionalEqualityWatches: List[QueryPlan] = (equalityConstraints.keySet -- accessedProperties).toList.map {
      prop =>
        QueryPlan.LocalProperty(prop, aliasAs = None, PropertyConstraint.Equal(equalityConstraints(prop)))
    }

    idWatch ++ labelWatch ++ allPropertiesWatch ++ nodeWatch ++ propertyWatches ++ additionalIsNotNullWatches ++ additionalEqualityWatches
  }

  /** Convert a pattern tree to a query plan.
    *
    * @param tree The pattern tree to plan
    * @param idLookups ID constraints from WHERE clause
    * @param nodeDeps Node dependencies from expression analysis
    * @param propertyConstraints IS NOT NULL constraints as (binding, property) pairs
    * @param propertyEqualities Property equality constraints for predicate pushdown
    * @param isRoot Whether this is a root pattern (needs anchor) vs child reached via Expand
    */
  def planTree(
    tree: GraphPatternTree,
    idLookups: List[IdLookup],
    nodeDeps: NodeDeps,
    propertyConstraints: List[(Int, Symbol)] = Nil,
    propertyEqualities: List[PropertyEquality] = Nil,
    isRoot: Boolean = true,
  ): QueryPlan = tree match {
    case GraphPatternTree.Branch(binding, labels, children) =>
      // Generate watches for this node, including property constraints and equalities
      val watches: List[QueryPlan] = generateWatches(binding, labels, nodeDeps, propertyConstraints, propertyEqualities)

      // Plan child expansions (children are NOT roots - they're reached via Expand)
      val expansions: List[QueryPlan] = children.map { conn =>
        val childPlan: QueryPlan =
          planTree(conn.tree, idLookups, nodeDeps, propertyConstraints, propertyEqualities, isRoot = false)
        val direction = conn.direction match {
          case Direction.Left => EdgeDirection.Incoming
          case Direction.Right => EdgeDirection.Outgoing
        }
        QueryPlan.Expand(Some(conn.edgeLabel), direction, childPlan)
      }

      // Combine watches and expansions
      val allOps: List[QueryPlan] = watches ++ expansions
      val combined: QueryPlan = allOps match {
        case Nil => QueryPlan.Unit
        case single :: Nil => single
        case multiple => QueryPlan.CrossProduct(multiple)
      }

      // Root patterns need an anchor; child patterns (reached via Expand) don't
      if (isRoot) {
        idLookups.find(_.forName == binding) match {
          case Some(lookup) =>
            QueryPlan.Anchor(AnchorTarget.Computed(lookup.exp), combined)
          case None =>
            // Root pattern without explicit ID - anchor on AllNodes
            QueryPlan.Anchor(AnchorTarget.AllNodes, combined)
        }
      } else {
        // Child pattern reached via edge traversal - no anchor needed
        combined
      }

    case GraphPatternTree.Empty =>
      QueryPlan.Unit
  }

  /** Filter out ID lookups from predicate (they're handled by Anchor).
    *
    * Note: id(a) = id(b) constraints are NOT filtered out - they are join conditions
    * that need to be applied as filters after both nodes are bound.
    */
  def filterOutIdLookups(expr: Pattern.Expression): Option[Pattern.Expression] = expr match {
    case Pattern.Expression.BinOp(_, Pattern.Operator.Equals, lhs, rhs, _) =>
      (lhs, rhs) match {
        // Keep id(a) = id(b) - this is a join condition, not an anchor computation
        case (_: Pattern.Expression.IdLookup, _: Pattern.Expression.IdLookup) => Some(expr)
        case (_: Pattern.Expression.IdLookup, _) => None
        case (_, _: Pattern.Expression.IdLookup) => None
        case _ => Some(expr)
      }

    case Pattern.Expression.BinOp(src, Pattern.Operator.And, lhs, rhs, typ) =>
      (filterOutIdLookups(lhs), filterOutIdLookups(rhs)) match {
        case (None, None) => None
        case (Some(l), None) => Some(l)
        case (None, Some(r)) => Some(r)
        case (Some(l), Some(r)) =>
          Some(Pattern.Expression.BinOp(src, Pattern.Operator.And, l, r, typ))
      }

    case _ => Some(expr)
  }

  /** Filter out property IS NOT NULL predicates that are already handled by LocalProperty constraints.
    *
    * When we generate a LocalProperty watch with constraint = Any, it already ensures the property exists.
    * So predicates like `n.prop IS NOT NULL` are redundant and can be removed from the filter.
    *
    * @param expr The predicate expression to filter
    * @param nodeDeps The node dependencies (used to identify which properties are watched)
    * @return The filtered predicate, or None if entirely redundant
    */
  def filterOutPropertyExistenceChecks(expr: Pattern.Expression, nodeDeps: NodeDeps): Option[Pattern.Expression] = {
    // Check if an expression is `node.prop IS NOT NULL` where node.prop is in our watches
    def isRedundantExistenceCheck(e: Pattern.Expression): Boolean = e match {
      // Pattern: NOT(IsNull(FieldAccess(node.prop)))
      case Pattern.Expression.UnaryOp(_, Pattern.Operator.Not, Pattern.Expression.IsNull(_, fieldAccess, _), _) =>
        isWatchedProperty(fieldAccess, nodeDeps)
      // Also handle direct IS NOT NULL patterns (they might compile differently)
      case _ => false
    }

    def isWatchedProperty(e: Pattern.Expression, deps: NodeDeps): Boolean = e match {
      case Pattern.Expression.FieldAccess(_, on, fieldName, _) =>
        on match {
          case Pattern.Expression.Ident(_, ident, _) =>
            deps.getOrElse(identInt(ident), Set.empty).contains(NodeDep.Property(fieldName))
          case _ => false
        }
      case _ => false
    }

    expr match {
      case e if isRedundantExistenceCheck(e) => None

      case Pattern.Expression.BinOp(src, Pattern.Operator.And, lhs, rhs, typ) =>
        (filterOutPropertyExistenceChecks(lhs, nodeDeps), filterOutPropertyExistenceChecks(rhs, nodeDeps)) match {
          case (None, None) => None
          case (Some(l), None) => Some(l)
          case (None, Some(r)) => Some(r)
          case (Some(l), Some(r)) =>
            Some(Pattern.Expression.BinOp(src, Pattern.Operator.And, l, r, typ))
        }

      case _ => Some(expr)
    }
  }

  /** Extract IS NOT NULL constraints from a predicate as (bindingId, property) pairs.
    * These will become LocalProperty watches with Any constraint (which requires property to exist).
    */
  def extractIsNotNullConstraints(expr: Pattern.Expression): List[(Int, Symbol)] = {
    def extractFromExpr(e: Pattern.Expression): List[(Int, Symbol)] = e match {
      // Pattern: NOT(IsNull(FieldAccess(node.prop))) means node.prop IS NOT NULL
      case Pattern.Expression.UnaryOp(_, Pattern.Operator.Not, Pattern.Expression.IsNull(_, fieldAccess, _), _) =>
        fieldAccess match {
          case Pattern.Expression.FieldAccess(_, on, fieldName, _) =>
            on match {
              case Pattern.Expression.Ident(_, ident, _) =>
                List((identInt(ident), fieldName))
              case _ => Nil
            }
          case _ => Nil
        }
      case Pattern.Expression.BinOp(_, Pattern.Operator.And, lhs, rhs, _) =>
        extractFromExpr(lhs) ++ extractFromExpr(rhs)
      case _ => Nil
    }
    extractFromExpr(expr)
  }

  /** Extract property equality constraints from a predicate.
    * Constraints like `e1.type = "WRITE"` become (bindingId, property, value) tuples.
    * These will become LocalProperty watches with Equal constraint (predicate pushdown).
    */
  case class PropertyEquality(binding: Int, property: Symbol, value: Pattern.Value)

  def extractPropertyEqualities(expr: Pattern.Expression): List[PropertyEquality] = {
    def extractFromExpr(e: Pattern.Expression): List[PropertyEquality] = e match {
      // Pattern: node.prop = literalValue
      case Pattern.Expression.BinOp(_, Pattern.Operator.Equals, lhs, rhs, _) =>
        (lhs, rhs) match {
          // node.prop = literal
          case (Pattern.Expression.FieldAccess(_, on, fieldName, _), Pattern.Expression.AtomicLiteral(_, value, _)) =>
            on match {
              case Pattern.Expression.Ident(_, ident, _) =>
                List(PropertyEquality(identInt(ident), fieldName, value))
              case _ => Nil
            }
          // literal = node.prop
          case (Pattern.Expression.AtomicLiteral(_, value, _), Pattern.Expression.FieldAccess(_, on, fieldName, _)) =>
            on match {
              case Pattern.Expression.Ident(_, ident, _) =>
                List(PropertyEquality(identInt(ident), fieldName, value))
              case _ => Nil
            }
          case _ => Nil
        }
      case Pattern.Expression.BinOp(_, Pattern.Operator.And, lhs, rhs, _) =>
        extractFromExpr(lhs) ++ extractFromExpr(rhs)
      case _ => Nil
    }
    extractFromExpr(expr)
  }

  /** Extract property equality constraints from inline node properties.
    * Inline properties like `MATCH (n {foo: "bar"})` become (bindingId, property, value) tuples,
    * equivalent to `MATCH (n) WHERE n.foo = "bar"`.
    * These will become LocalProperty watches with Equal constraint (predicate pushdown).
    *
    * Uses the same fresh binding generation logic as buildPatternTree to ensure anonymous
    * nodes get consistent bindings.
    */
  def extractInlinePropertyEqualities(patterns: List[Cypher.GraphPattern]): List[PropertyEquality] = {

    def extractFromPattern(pattern: Cypher.GraphPattern): List[PropertyEquality] = {
      def extractFromNode(node: Cypher.NodePattern): List[PropertyEquality] = {
        val binding = nodeBindingInt(node)
        node.maybeProperties match {
          case Some(mapLit: Pattern.Expression.MapLiteral) =>
            mapLit.value.toList.flatMap { case (propName, expr) =>
              expr match {
                case Pattern.Expression.AtomicLiteral(_, value, _) =>
                  List(PropertyEquality(binding, propName, value))
                case _ =>
                  // Non-literal property values (e.g., expressions, parameters) can't be pushed down
                  Nil
              }
            }
          case _ => Nil
        }
      }

      // Process in same order as buildPatternTree
      val initEqualities = extractFromNode(pattern.initial)

      val pathEqualities = pattern.path.flatMap { conn =>
        extractFromNode(conn.dest)
      }

      initEqualities ++ pathEqualities
    }

    patterns.flatMap(extractFromPattern)
  }

  /** Filter out property equality predicates that are pushed down to LocalProperty.
    * Returns the predicate with equality checks removed, or None if nothing remains.
    */
  def filterOutPropertyEqualities(
    expr: Pattern.Expression,
    equalities: List[PropertyEquality],
  ): Option[Pattern.Expression] = {
    val equalitySet: Set[(Int, Symbol)] = equalities.map(e => (e.binding, e.property)).toSet

    def filter(e: Pattern.Expression): Option[Pattern.Expression] = e match {
      case Pattern.Expression.BinOp(_, Pattern.Operator.Equals, lhs, rhs, _) =>
        val isPushedDown = (lhs, rhs) match {
          case (Pattern.Expression.FieldAccess(_, on, fieldName, _), _: Pattern.Expression.AtomicLiteral) =>
            on match {
              case Pattern.Expression.Ident(_, ident, _) => equalitySet.contains((identInt(ident), fieldName))
              case _ => false
            }
          case (_: Pattern.Expression.AtomicLiteral, Pattern.Expression.FieldAccess(_, on, fieldName, _)) =>
            on match {
              case Pattern.Expression.Ident(_, ident, _) => equalitySet.contains((identInt(ident), fieldName))
              case _ => false
            }
          case _ => false
        }
        if (isPushedDown) None else Some(e)

      case Pattern.Expression.BinOp(src, Pattern.Operator.And, lhs, rhs, typ) =>
        (filter(lhs), filter(rhs)) match {
          case (None, None) => None
          case (Some(l), None) => Some(l)
          case (None, Some(r)) => Some(r)
          case (Some(l), Some(r)) =>
            Some(Pattern.Expression.BinOp(src, Pattern.Operator.And, l, r, typ))
        }

      case _ => Some(e)
    }
    filter(expr)
  }

  /** Combined filter that removes ID lookups, IS NOT NULL predicates, and pushed-down property equalities.
    * IS NOT NULL predicates are handled by LocalProperty watches with Any constraint.
    * Property equalities are handled by LocalProperty watches with Equal constraint.
    */
  def filterOutRedundantPredicates(
    expr: Pattern.Expression,
    nodeDeps: NodeDeps,
    propertyEqualities: List[PropertyEquality] = Nil,
  ): Option[Pattern.Expression] =
    filterOutIdLookups(expr)
      .flatMap(filterOutPropertyExistenceChecks(_, nodeDeps))
      .flatMap(filterOutPropertyEqualities(_, propertyEqualities))

  /** Plan a MATCH clause with optional WHERE.
    *
    * Patterns that share a binding are merged into a single tree rooted at the shared node.
    * This avoids unnecessary CrossProduct operations and AllNodes scans.
    *
    * Remaining disjoint patterns are combined via:
    * - CrossProduct when they're independent
    * - Sequence when one pattern's anchor depends on another pattern's binding
    *
    * IS NOT NULL predicates are converted to LocalProperty watches with Any constraint,
    * following the MVSQ pattern where constraints are captured at the watch level.
    */
  def planMatch(
    patterns: List[Cypher.GraphPattern],
    maybePredicate: Option[Pattern.Expression],
    idLookups: List[IdLookup],
    nodeDeps: NodeDeps,
  ): QueryPlan = {
    // Extract IS NOT NULL constraints from the predicate
    // These become LocalProperty watches with Any constraint (requires property to exist)
    val propertyConstraints: List[(Int, Symbol)] = maybePredicate
      .map(extractIsNotNullConstraints)
      .getOrElse(Nil)

    // Extract property equality constraints for predicate pushdown
    // These become LocalProperty watches with Equal constraint (filters at the source)
    val whereClauseEqualities: List[PropertyEquality] = maybePredicate
      .map(extractPropertyEqualities)
      .getOrElse(Nil)

    // Extract inline property constraints from node patterns (e.g., MATCH (n {foo: "bar"}))
    // These are treated the same as WHERE clause property equalities
    val inlinePropertyEqualities: List[PropertyEquality] = extractInlinePropertyEqualities(patterns)

    // Combine all property equalities
    val propertyEqualities: List[PropertyEquality] = whereClauseEqualities ++ inlinePropertyEqualities

    // Build pattern trees from each Cypher pattern
    val initialTrees: List[GraphPatternTree.Branch] = patterns.flatMap { p =>
      buildPatternTree(p) match {
        case branch: GraphPatternTree.Branch => Some(branch)
        case GraphPatternTree.Empty => None
      }
    }

    // Merge trees that share bindings (e.g., MATCH (a)-[:X]-(b), (c)-[:Y]-(b) shares 'b')
    // This re-roots trees to shared bindings and merges them into single connected trees
    // Pass idLookups so we prefer bindings with ID lookups as the merge root (enables Computed anchor)
    val mergedTrees: List[GraphPatternTree.Branch] = mergeTreesWithSharedBindings(initialTrees, idLookups)

    // Deduplicate bindings within each merged tree to handle diamond patterns
    // (e.g., when node `p2` appears in multiple branches after merging)
    // This renames duplicate occurrences and returns the equality constraints needed
    val (dedupedTrees, allRenames): (List[GraphPatternTree.Branch], List[BindingRename]) = {
      val (trees, renames) = mergedTrees.foldLeft((List.empty[GraphPatternTree.Branch], List.empty[BindingRename])) {
        case ((accTrees, accRenames), tree) =>
          val (dedupedTree, _, _, treeRenames) = deduplicateBindings(tree)
          (accTrees :+ dedupedTree, accRenames ++ treeRenames)
      }
      (trees, renames)
    }

    // Add NodeDep.Id for all renamed bindings (both original and renamed)
    // This is needed for the diamond join filter: id(renamed) == id(original)
    val depsWithRenames: NodeDeps = allRenames.foldLeft(nodeDeps) { case (deps, BindingRename(original, renamed)) =>
      val withOriginal = NodeDeps.combine(deps, Map(original -> Set[NodeDep](NodeDep.Id)))
      NodeDeps.combine(withOriginal, Map(renamed -> Set[NodeDep](NodeDep.Id)))
    }

    // Build (tree, binding) pairs from deduplicated trees
    val treesWithBindings: List[(GraphPatternTree.Branch, Int)] =
      dedupedTrees.map(t => (t, t.binding))

    // For each pattern, find its ID lookup and dependencies
    case class PatternInfo(
      tree: GraphPatternTree.Branch,
      binding: Int,
      idLookup: Option[IdLookup],
      dependencies: Set[Int], // bindings this pattern's anchor depends on
    )

    val patternInfos = treesWithBindings.map { case (tree, binding) =>
      val lookup = idLookups.find(_.forName == binding)
      val deps = lookup.map(_.dependencies).getOrElse(Set.empty)
      PatternInfo(tree, binding, lookup, deps)
    }

    // Topological sort: patterns with no dependencies (or only external deps) come first
    val allBindings = patternInfos.map(_.binding).toSet

    def sortPatterns(remaining: List[PatternInfo], resolved: Set[Int]): List[PatternInfo] =
      if (remaining.isEmpty) Nil
      else {
        // Find patterns whose dependencies are all resolved (or external)
        val (ready, notReady) = remaining.partition { info =>
          (info.dependencies -- resolved).intersect(allBindings).isEmpty
        }
        if (ready.isEmpty && notReady.nonEmpty) {
          // Circular dependency - just take the first one
          notReady.head :: sortPatterns(notReady.tail, resolved + notReady.head.binding)
        } else {
          ready ++ sortPatterns(notReady, resolved ++ ready.map(_.binding))
        }
      }

    val sortedPatterns = sortPatterns(patternInfos, Set.empty)

    // Plan patterns in order, using Sequence when there are dependencies
    def planPatternsInOrder(
      infos: List[PatternInfo],
      inScope: Set[Int],
    ): QueryPlan = infos match {
      case Nil => QueryPlan.Unit

      case single :: Nil =>
        planTree(single.tree, idLookups, depsWithRenames, propertyConstraints, propertyEqualities, isRoot = true)

      case first :: rest =>
        val firstPlan =
          planTree(first.tree, idLookups, depsWithRenames, propertyConstraints, propertyEqualities, isRoot = true)
        val newScope = inScope + first.binding

        // Check if any remaining patterns depend on what we just added
        val hasDependents = rest.exists { info =>
          info.dependencies.intersect(Set(first.binding)).nonEmpty
        }

        val restPlan = planPatternsInOrder(rest, newScope)

        if (hasDependents) {
          // Dependent patterns need Sequence (context flows from first to rest)
          QueryPlan.Sequence(firstPlan, restPlan, ContextFlow.Extend)
        } else {
          // Independent patterns can use CrossProduct - flatten nested CrossProducts
          restPlan match {
            case QueryPlan.CrossProduct(restChildren, _) =>
              QueryPlan.CrossProduct(firstPlan :: restChildren)
            case _ =>
              QueryPlan.CrossProduct(List(firstPlan, restPlan))
          }
        }
    }

    val combinedPlan = planPatternsInOrder(sortedPatterns, Set.empty)

    // Apply diamond join filter if there were duplicate bindings
    // This ensures renamed bindings (from diamond patterns) match their original binding's ID
    val withDiamondJoin = makeDiamondJoinPredicate(allRenames) match {
      case Some(diamondPredicate) =>
        QueryPlan.Filter(diamondPredicate, combinedPlan)
      case None =>
        combinedPlan
    }

    // Apply WHERE predicate (minus ID lookups, property IS NOT NULL checks, and pushed-down equalities
    // which are already handled by Anchor and LocalProperty constraints)
    maybePredicate.flatMap(filterOutRedundantPredicates(_, nodeDeps, propertyEqualities)) match {
      case Some(predicate) =>
        QueryPlan.Filter(predicate, withDiamondJoin)
      case None =>
        withDiamondJoin
    }
  }

  /** Convert Cypher projections to QueryPlan Projections.
    * Uses Int-based binding format for consistency with expression interpreter lookups.
    * Human-readable names are applied at output time via outputNameMapping.
    */
  def convertProjections(
    projections: List[Cypher.Projection],
    @scala.annotation.unused symbolTable: SymbolAnalysisModule.SymbolTable,
  ): List[Projection] =
    projections.map { p =>
      val alias = p.as match {
        case Right(quineId) => bindingSymbol(quineId.name)
        case Left(cypherIdent) => cypherIdent.name // Fallback for unresolved identifiers
      }
      Projection(p.expression, alias)
    }

  /** Known aggregation function names */
  private val aggregationFunctions: Set[Symbol] =
    Set(Symbol("count"), Symbol("sum"), Symbol("avg"), Symbol("min"), Symbol("max"), Symbol("collect"))

  /** Check if an expression contains an aggregation function */
  private def containsAggregation(expr: Pattern.Expression): Boolean = expr match {
    case Pattern.Expression.Apply(_, funcName, _, _) =>
      aggregationFunctions.contains(funcName)
    case Pattern.Expression.BinOp(_, _, lhs, rhs, _) =>
      containsAggregation(lhs) || containsAggregation(rhs)
    case Pattern.Expression.UnaryOp(_, _, operand, _) =>
      containsAggregation(operand)
    case Pattern.Expression.FieldAccess(_, on, _, _) =>
      containsAggregation(on)
    case Pattern.Expression.CaseBlock(_, cases, alternative, _) =>
      cases.exists(c => containsAggregation(c.condition) || containsAggregation(c.value)) ||
        containsAggregation(alternative)
    case Pattern.Expression.ListLiteral(_, elements, _) =>
      elements.exists(containsAggregation)
    case Pattern.Expression.MapLiteral(_, entries, _) =>
      entries.values.exists(containsAggregation)
    case _ => false
  }

  /** Extract aggregation from an expression (returns the Aggregation and the alias) */
  private def extractAggregation(
    expr: Pattern.Expression,
    alias: Symbol,
  ): Option[(Aggregation, Symbol)] = expr match {
    case Pattern.Expression.Apply(_, funcName, args, _) =>
      funcName match {
        case Symbol("count") =>
          // count(*) or count(expr) - for now treat both as Count
          Some((Aggregation.Count(distinct = false), alias))
        case Symbol("sum") if args.nonEmpty =>
          Some((Aggregation.Sum(args.head), alias))
        case Symbol("avg") if args.nonEmpty =>
          Some((Aggregation.Avg(args.head), alias))
        case Symbol("min") if args.nonEmpty =>
          Some((Aggregation.Min(args.head), alias))
        case Symbol("max") if args.nonEmpty =>
          Some((Aggregation.Max(args.head), alias))
        case Symbol("collect") if args.nonEmpty =>
          Some((Aggregation.Collect(args.head, distinct = false), alias))
        case _ => None
      }
    case _ => None
  }

  /** Plan a RETURN or WITH clause */
  def planProjection(
    projections: List[Cypher.Projection],
    isDistinct: Boolean,
    input: QueryPlan,
    symbolTable: SymbolAnalysisModule.SymbolTable,
  ): QueryPlan = {
    // Check if any projection contains an aggregation function
    val hasAggregation = projections.exists(p => containsAggregation(p.expression))

    val projected = if (hasAggregation) {
      // Separate aggregations from non-aggregation projections
      val (aggProjections, nonAggProjections) = projections.partition(p => containsAggregation(p.expression))

      // Extract aggregations, keeping original identifier for creating references
      // Use Int-based format for aliases to match expression interpreter lookups
      val aggregationsWithIdent
        : List[(Aggregation, Either[Pattern.CypherIdentifier, Pattern.QuineIdentifier], Symbol)] =
        aggProjections.flatMap { p =>
          val alias = p.as match {
            case Right(quineId) => bindingSymbol(quineId.name)
            case Left(cypherIdent) => cypherIdent.name
          }
          extractAggregation(p.expression, alias).map { case (agg, al) =>
            (agg, p.as, al)
          }
        }

      // Non-aggregation projections become GROUP BY keys
      // Use Int-based format for consistency with expression interpreter
      val groupByKeys: List[Symbol] = nonAggProjections.map { p =>
        p.as match {
          case Right(quineId) => bindingSymbol(quineId.name)
          case Left(cypherIdent) => cypherIdent.name
        }
      }

      if (aggregationsWithIdent.nonEmpty) {
        // Create Aggregate operator
        // The aggregations list contains (Aggregation, original ident, alias) tuples
        // We need to wrap with Project to apply aliases
        val aggOps = aggregationsWithIdent.map(_._1)
        val aggregate = QueryPlan.Aggregate(aggOps, groupByKeys, input)

        // Project the results with proper aliases
        val allColumns = aggregationsWithIdent.map { case (_, originalIdent, alias) =>
          // After aggregation, the result is available under the alias
          // Use the original identifier to create a reference expression
          Projection(
            Pattern.Expression.Ident(Pattern.Source.NoSource, originalIdent, None),
            alias,
          )
        } ++ nonAggProjections.map { p =>
          val alias = p.as match {
            case Right(quineId) => bindingSymbol(quineId.name)
            case Left(cypherIdent) => cypherIdent.name
          }
          Projection(p.expression, alias)
        }

        if (allColumns.isEmpty) aggregate
        else QueryPlan.Project(allColumns, dropExisting = true, aggregate)
      } else {
        // Fallback to regular projection
        val columns = convertProjections(projections, symbolTable)
        if (columns.isEmpty) input
        else QueryPlan.Project(columns, dropExisting = true, input)
      }
    } else {
      // No aggregation - regular projection
      val columns = convertProjections(projections, symbolTable)
      if (columns.isEmpty) input
      else QueryPlan.Project(columns, dropExisting = true, input)
    }

    if (isDistinct) QueryPlan.Distinct(projected)
    else projected
  }

  /** Plan effects (CREATE, SET, etc.)
    *
    * @param effect The effect to plan
    * @param existingBindings Bindings that already exist in scope (from MATCH, etc.)
    *                         Used to avoid creating nodes that already exist
    * @param idLookups ID lookups from WHERE clause (used for CREATE edge destination expressions)
    * @param symbolTable Symbol table for resolving binding names to IDs
    */
  def planEffects(
    effect: Cypher.Effect,
    existingBindings: Set[Symbol],
    symbolTable: SymbolAnalysisModule.SymbolTable,
    idLookups: List[IdLookup] = Nil,
  ): List[LocalQueryEffect] = effect match {
    case Cypher.Effect.SetLabel(_, id, labels) =>
      // id is the target node identifier
      List(LocalQueryEffect.SetLabels(Some(bindingSymbol(identInt(id))), labels))

    case Cypher.Effect.SetProperties(_, id, properties) =>
      // id is the target node identifier
      List(LocalQueryEffect.SetProperties(Some(bindingSymbol(identInt(id))), properties))

    case Cypher.Effect.SetProperty(_, property, value) =>
      // property is a FieldAccess - extract the target node and property name
      // Handle both resolved (Right) and unresolved (Left) identifiers
      val targetBinding = QuinePatternHelpers.getRootId(property).toOption.flatMap {
        case Right(quineId) => Some(bindingSymbol(quineId.name))
        case Left(cypherIdent) => Some(cypherIdent.name) // Fallback for unresolved identifiers
      }
      List(LocalQueryEffect.SetProperty(targetBinding, property.fieldName, value))

    case Cypher.Effect.Create(_, patterns) =>
      // Extract node creations and edge creations from patterns
      // Pass existingBindings to skip CreateNode for already-defined bindings
      // Pass idLookups so CREATE edges can use idFrom expressions directly
      patterns.flatMap(extractCreateEffects(_, existingBindings, idLookups))

    case Cypher.Effect.Foreach(_, binding, listExpr, nestedEffects) =>
      // Convert the list expression and recursively plan nested effects
      val nestedPlanned = nestedEffects.flatMap(planEffects(_, existingBindings, symbolTable, idLookups))
      // Find the FOREACH binding's QuineIdentifier by looking at expressions inside the body.
      // When there are multiple FOREACHs with the same binding name, each gets its own
      // QuineIdentifier. The symbol table has multiple entries for the same name.
      // We find the correct one by looking at what QuineIdentifier the nested effects use.
      val foreachBindingSymbol: Symbol = {
        // Extract all QuineIdentifiers referenced in nested effects
        def findBindingReferences(effects: List[LocalQueryEffect]): Set[Int] = {
          def extractFromExpr(expr: Pattern.Expression): Set[Int] = expr match {
            case Pattern.Expression.Ident(_, Right(quineId), _) =>
              // Check if this identifier corresponds to the FOREACH binding
              symbolTable.references.collectFirst {
                case entry: SymbolAnalysisModule.SymbolTableEntry.QuineToCypherIdEntry
                    if entry.identifier == quineId.name && entry.cypherIdentifier == binding =>
                  quineId.name
              }.toSet
            case Pattern.Expression.FieldAccess(_, of, _, _) => extractFromExpr(of)
            case Pattern.Expression.BinOp(_, _, lhs, rhs, _) => extractFromExpr(lhs) ++ extractFromExpr(rhs)
            case Pattern.Expression.UnaryOp(_, _, operand, _) => extractFromExpr(operand)
            case Pattern.Expression.Apply(_, _, args, _) => args.flatMap(extractFromExpr).toSet
            case Pattern.Expression.ListLiteral(_, elements, _) => elements.flatMap(extractFromExpr).toSet
            case Pattern.Expression.CaseBlock(_, cases, alt, _) =>
              cases.flatMap(c => extractFromExpr(c.condition) ++ extractFromExpr(c.value)).toSet ++ extractFromExpr(alt)
            case _ => Set.empty
          }
          effects.flatMap {
            case LocalQueryEffect.SetProperty(_, _, value) => extractFromExpr(value)
            case LocalQueryEffect.SetProperties(_, props) => extractFromExpr(props)
            case LocalQueryEffect.CreateHalfEdge(_, _, _, dest) => extractFromExpr(dest)
            case LocalQueryEffect.Foreach(_, list, nested) => extractFromExpr(list) ++ findBindingReferences(nested)
            case _ => Set.empty
          }.toSet
        }

        val referencedIds = findBindingReferences(nestedPlanned)
        referencedIds.headOption.map(bindingSymbol).getOrElse {
          // Fallback: find entry in symbol table by name
          symbolTable.references
            .collectFirst {
              case entry: SymbolAnalysisModule.SymbolTableEntry.QuineToCypherIdEntry
                  if entry.cypherIdentifier == binding =>
                bindingSymbol(entry.identifier)
            }
            .getOrElse(binding)
        }
      }
      List(LocalQueryEffect.Foreach(foreachBindingSymbol, listExpr, nestedPlanned))
  }

  /** Extract CREATE effects from a graph pattern.
    *
    * For CREATE patterns like:
    *   CREATE (n:Label)                      -> CreateNode for n (if n not already bound)
    *   CREATE (a)-[:REL]->(b)                -> CreateHalfEdge on both a and b
    *   CREATE (n:Label)-[:REL]->(m:Label)    -> CreateNode for n, CreateNode for m, CreateHalfEdge on both
    *                                            (only for bindings not already in scope)
    *
    * Nodes with labels/properties that aren't already in context need to be created.
    * Nodes that already exist (from MATCH, etc.) should NOT have CreateNode generated -
    * the labels in CREATE are ignored for existing nodes (standard Cypher semantics).
    *
    * @param pattern The CREATE pattern
    * @param existingBindings Bindings that already exist (from MATCH, etc.)
    */
  private def extractCreateEffects(
    pattern: Cypher.GraphPattern,
    existingBindings: Set[Symbol],
    idLookups: List[IdLookup],
  ): List[LocalQueryEffect] = {

    /** Binding information for a node pattern.
      * For resolved bindings (Right): (symbol using Int format, Some(bindingId))
      * For FOREACH bindings (Left): (raw symbol, None)
      */
    sealed trait NodeBindingInfo {
      def symbol: Symbol
      def makeExpr: Pattern.Expression
    }
    case class ResolvedBinding(bindingId: Int) extends NodeBindingInfo {
      def symbol: Symbol = bindingSymbol(bindingId)
      def makeExpr: Pattern.Expression = makeIdentExpr(bindingId)
    }
    case class ForeachBinding(rawSymbol: Symbol) extends NodeBindingInfo {
      def symbol: Symbol = rawSymbol
      // FOREACH bindings reference the raw symbol using Left(CypherIdentifier)
      def makeExpr: Pattern.Expression =
        Pattern.Expression.Ident(Pattern.Source.NoSource, Left(Pattern.CypherIdentifier(rawSymbol)), None)
    }

    /** Extract node binding info and optional creation effect from a node pattern.
      * Returns (NodeBindingInfo, Option[CreateNode effect]).
      * Only creates CreateNode if:
      * - The binding is resolved (Right) - FOREACH bindings can't create nodes
      * - The binding doesn't already exist (not from MATCH, etc.)
      * - The node has labels (indicates creation intent)
      */
    def extractNodeEffect(
      nodePattern: Cypher.NodePattern,
    ): (NodeBindingInfo, Option[LocalQueryEffect.CreateNode]) = {
      val bindingInfo: NodeBindingInfo = nodePattern.maybeBinding match {
        case Some(Right(quineId)) => ResolvedBinding(quineId.name)
        case Some(Left(cypherIdent)) => ForeachBinding(cypherIdent.name)
        case None => throw new MissingBindingException(nodePattern)
      }
      val labels = nodePattern.labels
      val maybeProperties = nodePattern.maybeProperties
      // Only create node if:
      // 1. Binding is resolved (FOREACH bindings reference existing nodes)
      // 2. It has labels (indicates creation intent)
      // 3. The binding doesn't already exist (not from MATCH, etc.)
      val createEffect = bindingInfo match {
        case ResolvedBinding(bindingId) =>
          val binding = bindingSymbol(bindingId)
          if (labels.nonEmpty && !existingBindings.contains(binding))
            Some(LocalQueryEffect.CreateNode(binding, labels, maybeProperties))
          else None
        case _: ForeachBinding => None // FOREACH bindings reference existing nodes
      }
      (bindingInfo, createEffect)
    }

    val effects = scala.collection.mutable.ListBuffer.empty[LocalQueryEffect]
    val createdBindings = scala.collection.mutable.Set.empty[Symbol]

    // Handle initial node
    val (initialBindingInfo, initialCreateOpt) = extractNodeEffect(pattern.initial)
    initialCreateOpt.foreach { effect =>
      effects += effect
      createdBindings += effect.binding
    }

    // Handle connections
    var currentBindingInfo: NodeBindingInfo = initialBindingInfo
    pattern.path.foreach { conn =>
      val (destBindingInfo, destCreateOpt) = extractNodeEffect(conn.dest)

      // Create destination node if needed and not already created
      destCreateOpt.foreach { effect =>
        if (!createdBindings.contains(effect.binding)) {
          effects += effect
          createdBindings += effect.binding
        }
      }

      // Create half-edges on both sides
      val label = conn.edge.edgeType
      val (leftDir, rightDir) = conn.edge.direction match {
        case Pattern.Direction.Right => (EdgeDirection.Outgoing, EdgeDirection.Incoming)
        case Pattern.Direction.Left => (EdgeDirection.Incoming, EdgeDirection.Outgoing)
      }

      // Expression to reference the other node
      // For resolved bindings: Right(QuineIdentifier(N)) -> looks up Symbol("N")
      // For FOREACH bindings: Left(CypherIdentifier(sym)) -> looks up raw symbol
      val destExpr = destBindingInfo.makeExpr
      val sourceExpr = currentBindingInfo.makeExpr

      // Half-edge from source to dest
      effects += LocalQueryEffect.CreateHalfEdge(Some(currentBindingInfo.symbol), label, leftDir, destExpr)
      // Half-edge from dest to source (reciprocal)
      effects += LocalQueryEffect.CreateHalfEdge(Some(destBindingInfo.symbol), label, rightDir, sourceExpr)

      currentBindingInfo = destBindingInfo
    }

    effects.toList
  }

  /** Extract bindings defined by a query part.
    * Used to track which bindings exist when processing subsequent parts.
    * Returns Symbols (for QueryPlan output compatibility).
    */
  def extractBindingsFromPart(part: Cypher.QueryPart): Set[Symbol] = part match {
    case Cypher.QueryPart.ReadingClausePart(readingClause) =>
      readingClause match {
        case patterns: Cypher.ReadingClause.FromPatterns =>
          // Extract bindings from all graph patterns
          patterns.patterns.flatMap { pattern =>
            val initBinding = bindingSymbol(nodeBindingInt(pattern.initial))
            val pathBindings = pattern.path.map(conn => bindingSymbol(nodeBindingInt(conn.dest)))
            initBinding :: pathBindings
          }.toSet

        case unwind: Cypher.ReadingClause.FromUnwind =>
          Set(bindingSymbol(identInt(unwind.as)))

        case proc: Cypher.ReadingClause.FromProcedure =>
          // CALL procedure YIELD x, y, z -> binds x, y, z (using the boundAs name)
          proc.yields.map(yi => bindingSymbol(identInt(yi.boundAs))).toSet

        case _ => Set.empty
      }

    case Cypher.QueryPart.WithClausePart(withClause) =>
      // WITH establishes new bindings from its projections (use the alias name)
      withClause.bindings.map(p => bindingSymbol(identInt(p.as))).toSet

    case _ => Set.empty
  }

  /** Plan a single query part */
  def planQueryPart(
    part: Cypher.QueryPart,
    idLookups: List[IdLookup],
    nodeDeps: NodeDeps,
    symbolTable: SymbolAnalysisModule.SymbolTable,
    existingBindings: Set[Symbol] = Set.empty,
  ): QueryPlan = part match {
    case Cypher.QueryPart.ReadingClausePart(readingClause) =>
      readingClause match {
        case patterns: Cypher.ReadingClause.FromPatterns =>
          planMatch(patterns.patterns, patterns.maybePredicate, idLookups, nodeDeps)

        case proc: Cypher.ReadingClause.FromProcedure =>
          // CALL procedureName(args...) YIELD bindings
          // Create a Procedure plan with the subquery as Unit (will be wrapped by planQueryParts)
          // Convert YieldItems to (resultField, boundAs) pairs
          val yieldPairs = proc.yields.map { yi =>
            (yi.resultField, bindingSymbol(identInt(yi.boundAs)))
          }
          QueryPlan.Procedure(
            procedureName = proc.name,
            arguments = proc.args,
            yields = yieldPairs,
            subquery = QueryPlan.Unit,
          )

        case unwind: Cypher.ReadingClause.FromUnwind =>
          // UNWIND expression AS binding - use Int-based format to match expression interpreter
          QueryPlan.Unwind(unwind.list, bindingSymbol(identInt(unwind.as)), QueryPlan.Unit)

        case _: Cypher.ReadingClause.FromSubquery =>
          throw new QuinePatternUnimplementedException("Subqueries not yet supported in planner")
      }

    case Cypher.QueryPart.WithClausePart(withClause) =>
      // WITH clause creates a sequence point with projection
      // For now, just handle the projection part
      val columns = convertProjections(withClause.bindings, symbolTable)
      if (columns.isEmpty) QueryPlan.Unit
      else {
        val projected = QueryPlan.Project(columns, dropExisting = !withClause.hasWildCard, QueryPlan.Unit)
        // Apply WHERE if present (minus redundant predicates)
        withClause.maybePredicate.flatMap(filterOutRedundantPredicates(_, nodeDeps)) match {
          case Some(pred) => QueryPlan.Filter(pred, projected)
          case None => projected
        }
      }

    case Cypher.QueryPart.EffectPart(effect) =>
      val effects = planEffects(effect, existingBindings, symbolTable, idLookups)
      if (effects.isEmpty) QueryPlan.Unit
      else QueryPlan.LocalEffect(effects, QueryPlan.Unit)
  }

  /** Combine query parts into a sequence */
  def planQueryParts(
    parts: List[Cypher.QueryPart],
    idLookups: List[IdLookup],
    nodeDeps: NodeDeps,
    symbolTable: SymbolAnalysisModule.SymbolTable,
    existingBindings: Set[Symbol] = Set.empty,
  ): QueryPlan = parts match {
    case Nil => QueryPlan.Unit
    case single :: Nil => planQueryPart(single, idLookups, nodeDeps, symbolTable, existingBindings)
    case first :: rest =>
      // Track bindings established by this part for subsequent parts
      val bindingsFromFirst = extractBindingsFromPart(first)
      val accumulatedBindings = existingBindings ++ bindingsFromFirst

      // Special handling for UNWIND and PROCEDURE: the rest of the query becomes the subquery
      // These are like flatMap - for each element/result, evaluate the subquery
      first match {
        case Cypher.QueryPart.ReadingClausePart(unwind: Cypher.ReadingClause.FromUnwind) =>
          val restPlan = planQueryParts(rest, idLookups, nodeDeps, symbolTable, accumulatedBindings)
          QueryPlan.Unwind(unwind.list, bindingSymbol(identInt(unwind.as)), restPlan)

        case Cypher.QueryPart.ReadingClausePart(proc: Cypher.ReadingClause.FromProcedure) =>
          val restPlan = planQueryParts(rest, idLookups, nodeDeps, symbolTable, accumulatedBindings)
          // Convert YieldItems to (resultField, boundAs) pairs
          val yieldPairs = proc.yields.map { yi =>
            (yi.resultField, bindingSymbol(identInt(yi.boundAs)))
          }
          QueryPlan.Procedure(
            procedureName = proc.name,
            arguments = proc.args,
            yields = yieldPairs,
            subquery = restPlan,
          )

        case _ =>
          val firstPlan = planQueryPart(first, idLookups, nodeDeps, symbolTable, existingBindings)
          val restPlan = planQueryParts(rest, idLookups, nodeDeps, symbolTable, accumulatedBindings)
          // Determine if we need sequential binding flow between parts:
          // - Effects depend on prior match results
          // - WITH clauses create sequence points (receive bindings and establish new ones)
          // - Parts following WITH need the bindings it established
          val needsSequence = (first, rest.headOption) match {
            case (_, Some(_: Cypher.QueryPart.EffectPart)) => true // Effects need prior bindings
            case (_, Some(_: Cypher.QueryPart.WithClausePart)) => true // WITH needs prior bindings
            case (_: Cypher.QueryPart.WithClausePart, _) => true // Parts after WITH need its bindings
            case _ => false
          }
          if (needsSequence) {
            QueryPlan.Sequence(firstPlan, restPlan, ContextFlow.Extend)
          } else {
            // Truly independent reading clauses (rare - usually just multiple MATCHes without WITH)
            QueryPlan.CrossProduct(List(firstPlan, restPlan))
          }
      }
  }

  // ============================================================
  // PLAN POST-PROCESSING
  // ============================================================

  /** Extract binding symbol from an expression (for Anchor targets like Ident(n)) */
  private def extractBindingFromExpr(expr: Pattern.Expression): Option[Symbol] = expr match {
    case Pattern.Expression.Ident(_, ident, _) => Some(bindingSymbol(identInt(ident)))
    case _ => None
  }

  /** Find binding from expression by checking against IdLookups.
    * When an Anchor has Computed(expr), this checks if expr matches any IdLookup's expression,
    * and if so returns the binding that IdLookup is for.
    */
  private def findBindingFromIdLookups(expr: Pattern.Expression, idLookups: List[IdLookup]): Option[Symbol] =
    idLookups.find(_.exp == expr).map(l => bindingSymbol(l.forName))

  /** Find the node binding within a plan (to determine what binding an anchor provides).
    * Looks for LocalId, LocalAllProperties, or LocalNode which all bind a node.
    */
  private def findLocalIdBinding(plan: QueryPlan): Option[Symbol] = plan match {
    case QueryPlan.LocalId(binding) => Some(binding)
    case QueryPlan.LocalAllProperties(binding) => Some(binding)
    case QueryPlan.LocalNode(binding) => Some(binding)
    case QueryPlan.Sequence(first, _, _) => findLocalIdBinding(first)
    case QueryPlan.CrossProduct(children, _) => children.flatMap(findLocalIdBinding).headOption
    case QueryPlan.Filter(_, child) => findLocalIdBinding(child)
    case QueryPlan.Project(_, _, child) => findLocalIdBinding(child)
    case QueryPlan.Anchor(_, onTarget) => findLocalIdBinding(onTarget)
    case _ => None
  }

  /** Extract target binding from an effect */
  private def getEffectTarget(e: LocalQueryEffect): Option[Symbol] = e match {
    case LocalQueryEffect.SetProperty(target, _, _) => target
    case LocalQueryEffect.SetProperties(target, _) => target
    case LocalQueryEffect.SetLabels(target, _) => target
    case LocalQueryEffect.CreateHalfEdge(source, _, _, _) => source
    case LocalQueryEffect.Foreach(_, _, nestedEffects) =>
      // FOREACH runs on the node that its nested effects target
      nestedEffects.flatMap(getEffectTarget).headOption
    case _: LocalQueryEffect.CreateNode => None
  }

  /** Clear target from effect (it becomes implicit via anchor context) */
  private def clearEffectTarget(e: LocalQueryEffect): LocalQueryEffect = e match {
    case e: LocalQueryEffect.SetProperty => e.copy(target = None)
    case e: LocalQueryEffect.SetProperties => e.copy(target = None)
    case e: LocalQueryEffect.SetLabels => e.copy(target = None)
    case e: LocalQueryEffect.CreateHalfEdge => e.copy(source = None)
    case e: LocalQueryEffect.Foreach =>
      // For FOREACH, preserve CreateHalfEdge sources - the runtime needs to know
      // which node should create each half-edge, since the FOREACH may contain
      // edge effects targeting multiple nodes.
      e.copy(effects = e.effects.map(clearEffectTargetInForeach))
    case other => other
  }

  /** Clear target from effect inside FOREACH - preserves targets for effects
    * that may need to run on different nodes than the anchor.
    *
    * In FOREACH, effects like SET and CREATE can target nodes different from
    * the anchor node, so we preserve their target bindings for runtime dispatch.
    */
  private def clearEffectTargetInForeach(e: LocalQueryEffect): LocalQueryEffect = e match {
    // IMPORTANT: Keep targets for all effects in FOREACH - runtime needs them
    // to dispatch effects to the correct target nodes
    case e: LocalQueryEffect.SetProperty => e
    case e: LocalQueryEffect.SetProperties => e
    case e: LocalQueryEffect.SetLabels => e
    case e: LocalQueryEffect.CreateHalfEdge => e
    case e: LocalQueryEffect.Foreach =>
      e.copy(effects = e.effects.map(clearEffectTargetInForeach))
    case other => other
  }

  /** Create an Ident expression for a binding symbol */
  private def makeBindingExpr(binding: Symbol): Pattern.Expression =
    Pattern.Expression.Ident(Pattern.Source.NoSource, Left(Pattern.CypherIdentifier(binding)), None)

  /** Extract all LocalQueryEffects from a plan (recursively through Sequences and LocalEffects) */
  private def extractEffectsFromPlan(plan: QueryPlan): List[LocalQueryEffect] = plan match {
    case QueryPlan.LocalEffect(effects, child) =>
      effects ++ extractEffectsFromPlan(child)
    case QueryPlan.Sequence(first, andThen, _) =>
      extractEffectsFromPlan(first) ++ extractEffectsFromPlan(andThen)
    case _ => Nil
  }

  /** Extract binding references from an effect's value expression.
    * Returns the set of bindings (as Symbols) that the effect depends on (reads).
    */
  private def getEffectDependencies(effect: LocalQueryEffect): Set[Symbol] = effect match {
    case LocalQueryEffect.SetProperty(_, _, value) => extractVariableRefs(value).map(bindingSymbol)
    case LocalQueryEffect.SetProperties(_, props) => extractVariableRefs(props).map(bindingSymbol)
    case LocalQueryEffect.CreateHalfEdge(_, _, _, destExpr) => extractVariableRefs(destExpr).map(bindingSymbol)
    case LocalQueryEffect.Foreach(binding, listExpr, nested) =>
      // FOREACH binds `binding` from listExpr, so nested effects can use it
      val listDeps = extractVariableRefs(listExpr).map(bindingSymbol)
      val nestedDeps = nested.flatMap(getEffectDependencies).toSet - binding
      listDeps ++ nestedDeps
    case _ => Set.empty
  }

  /** Strip LocalEffect nodes from a plan, leaving the remainder */
  private def stripEffectsFromPlan(plan: QueryPlan): QueryPlan = plan match {
    case QueryPlan.LocalEffect(_, child) => stripEffectsFromPlan(child)
    case QueryPlan.Sequence(first, andThen, flow) =>
      val strippedFirst = stripEffectsFromPlan(first)
      val strippedAndThen = stripEffectsFromPlan(andThen)
      (strippedFirst, strippedAndThen) match {
        case (QueryPlan.Unit, QueryPlan.Unit) => QueryPlan.Unit
        case (QueryPlan.Unit, other) => other
        case (other, QueryPlan.Unit) => other
        case (f, a) => QueryPlan.Sequence(f, a, flow)
      }
    case other => other
  }

  /** Push nodes following an Anchor into the Anchor's onTarget.
    *
    * In a query like "MATCH (n) WHERE id(n) = ... SET n.foo = ... RETURN ...",
    * the SET and RETURN should run ON node n, not on the dispatcher.
    * This function pushes everything after an Anchor into the Anchor's onTarget.
    *
    * Key improvement: tracks anchor context to avoid creating redundant anchors.
    * If we're already inside an anchor for node n, effects targeting n run locally.
    *
    * Before: Sequence(Anchor(target, LocalId), LocalEffect(...))
    * After:  Anchor(target, Sequence(LocalId, LocalEffect(...)))  -- no nested anchor!
    */
  def pushIntoAnchors(plan: QueryPlan, idLookups: List[IdLookup] = Nil): QueryPlan = {
    // Inner function that tracks current anchor binding (which node we're "on")
    def push(plan: QueryPlan, anchorContext: Option[Symbol]): QueryPlan = plan match {
      // Main case: Sequence with Anchor followed by something else
      case QueryPlan.Sequence(QueryPlan.Anchor(target, onTarget), rest, flow) =>
        // Determine binding from this anchor
        // For Computed(Ident(n)), extract n directly
        // For Computed(SynthesizeId(...)) or similar, check IdLookups to find the binding
        // For Computed(Parameter(...)) or AllNodes, find the binding from LocalId inside
        val binding = target match {
          case AnchorTarget.Computed(expr) =>
            extractBindingFromExpr(expr)
              .orElse(findBindingFromIdLookups(expr, idLookups))
              .orElse(findLocalIdBinding(onTarget))
          case AnchorTarget.AllNodes => findLocalIdBinding(onTarget)
        }
        // Push the rest into the anchor's onTarget
        val newOnTarget = QueryPlan.Sequence(onTarget, rest, flow)
        // Process with this anchor's context
        QueryPlan.Anchor(target, push(newOnTarget, binding.orElse(anchorContext)))

      // Handle the case where Anchor is wrapped in Project
      case QueryPlan.Project(columns, dropExisting, child) =>
        push(child, anchorContext) match {
          case QueryPlan.Anchor(target, onTarget) =>
            val projectedOnTarget = QueryPlan.Project(columns, dropExisting, onTarget)
            QueryPlan.Anchor(target, projectedOnTarget)
          case other =>
            QueryPlan.Project(columns, dropExisting, other)
        }

      // Handle the case where Anchor is wrapped in Filter
      case QueryPlan.Filter(predicate, child) =>
        push(child, anchorContext) match {
          case QueryPlan.Anchor(target, onTarget) =>
            val filteredOnTarget = QueryPlan.Filter(predicate, onTarget)
            QueryPlan.Anchor(target, filteredOnTarget)
          case other =>
            QueryPlan.Filter(predicate, other)
        }

      // Handle Distinct wrapping Anchor
      case QueryPlan.Distinct(child) =>
        push(child, anchorContext) match {
          case QueryPlan.Anchor(target, onTarget) =>
            QueryPlan.Anchor(target, QueryPlan.Distinct(onTarget))
          case other =>
            QueryPlan.Distinct(other)
        }

      // Special case: CrossProduct followed by effects - push effects into anchor children
      // BUT only if the effect doesn't depend on bindings from other anchors
      // This case only applies when CrossProduct contains Anchor children (multiple nodes)
      // For CrossProduct containing leaf nodes (LocalId, LocalAllProperties), use normal Sequence handling
      case QueryPlan.Sequence(
            cp @ QueryPlan.CrossProduct(children, emitLazily),
            effectRest,
            flow,
          ) if children.exists(_.isInstanceOf[QueryPlan.Anchor]) =>
        // Extract all effects from the rest of the sequence
        val allEffects = extractEffectsFromPlan(effectRest)

        if (allEffects.nonEmpty) {
          // Build a map of binding -> anchor index for CrossProduct children
          val bindingToIndex: Map[Symbol, Int] = children.zipWithIndex.flatMap { case (child, idx) =>
            child match {
              case QueryPlan.Anchor(target, onTarget) =>
                val binding = target match {
                  case AnchorTarget.Computed(expr) =>
                    extractBindingFromExpr(expr)
                      .orElse(findBindingFromIdLookups(expr, idLookups))
                      .orElse(findLocalIdBinding(onTarget))
                  case AnchorTarget.AllNodes => findLocalIdBinding(onTarget)
                }
                binding.map(_ -> idx)
              case _ => None
            }
          }.toMap

          val allBindings = bindingToIndex.keySet

          // Check if an effect can be safely pushed into its target anchor
          // (i.e., its dependencies don't include other CrossProduct bindings)
          def canPushEffect(effect: LocalQueryEffect, targetBinding: Symbol): Boolean = {
            val deps = getEffectDependencies(effect)
            // Effect can be pushed if its dependencies don't include OTHER anchors' bindings
            val crossNodeDeps = deps.intersect(allBindings) - targetBinding
            crossNodeDeps.isEmpty
          }

          // Partition effects: pushable vs. needs-separate-anchor
          val effectsByBinding: Map[Option[Symbol], List[LocalQueryEffect]] =
            allEffects.groupBy(getEffectTarget)

          val (pushableByTarget, needsSeparateAnchor) = effectsByBinding
            .flatMap { case (targetOpt, effects) =>
              targetOpt match {
                case Some(target) if allBindings.contains(target) =>
                  val (pushable, notPushable) = effects.partition(canPushEffect(_, target))
                  List((Some(target), pushable, notPushable))
                case _ =>
                  // No target or target not in CrossProduct - can't push
                  List((targetOpt, Nil, effects))
              }
            }
            .foldLeft((Map.empty[Symbol, List[LocalQueryEffect]], List.empty[LocalQueryEffect])) {
              case ((pushMap, separate), (Some(target), pushable, notPushable)) =>
                val updated = pushMap.updated(target, pushMap.getOrElse(target, Nil) ++ pushable)
                (updated, separate ++ notPushable)
              case ((pushMap, separate), (None, _, notPushable)) =>
                (pushMap, separate ++ notPushable)
            }

          // Push pushable effects into CrossProduct children
          val injectedChildren: List[QueryPlan] = children.map { child =>
            child match {
              case QueryPlan.Anchor(target, onTarget) =>
                val binding = target match {
                  case AnchorTarget.Computed(expr) =>
                    extractBindingFromExpr(expr)
                      .orElse(findBindingFromIdLookups(expr, idLookups))
                      .orElse(findLocalIdBinding(onTarget))
                  case AnchorTarget.AllNodes => findLocalIdBinding(onTarget)
                }
                // Get pushable effects for this binding
                val effectsForBinding = binding.flatMap(pushableByTarget.get).getOrElse(Nil)
                if (effectsForBinding.nonEmpty) {
                  val clearedEffects = effectsForBinding.map(clearEffectTarget)
                  val newOnTarget = QueryPlan.Sequence(
                    onTarget,
                    QueryPlan.LocalEffect(clearedEffects, QueryPlan.Unit),
                    ContextFlow.Extend,
                  )
                  QueryPlan.Anchor(target, push(newOnTarget, binding.orElse(anchorContext)))
                } else {
                  push(child, anchorContext)
                }
              case _ => push(child, anchorContext)
            }
          }

          // Get the remainder after effects
          val remainder = stripEffectsFromPlan(effectRest)

          // Build the result
          val newCrossProduct = QueryPlan.CrossProduct(injectedChildren, emitLazily)
          val withRemainder =
            if (remainder != QueryPlan.Unit)
              QueryPlan.Sequence(newCrossProduct, push(remainder, anchorContext), flow)
            else newCrossProduct

          // Handle effects that couldn't be pushed (have cross-node dependencies)
          if (needsSeparateAnchor.nonEmpty) {
            // Try to restructure CrossProduct into nested Sequence to avoid visiting nodes twice
            // For example: MATCH (a), (b) SET a.x = b.y
            //   CrossProduct([Anchor(a), Anchor(b)]) + separate Anchor(a) for effect
            // Should become:
            //   Anchor(b) -> Sequence(LocalId(b), Anchor(a) -> Sequence(LocalId(a), LocalEffect(...)))

            val effectTargets = needsSeparateAnchor.flatMap(getEffectTarget).toSet
            val effectDeps = needsSeparateAnchor.flatMap(getEffectDependencies).toSet

            // Separate children into "dependency" children (visit first) and "target" children (visit last with effects)
            val (depChildren, targetChildren, otherChildren) = children.foldLeft(
              (List.empty[(QueryPlan, Symbol)], List.empty[(QueryPlan, Symbol)], List.empty[QueryPlan]),
            ) { case ((deps, targets, others), child) =>
              child match {
                case anchor @ QueryPlan.Anchor(target, onTarget) =>
                  val binding = target match {
                    case AnchorTarget.Computed(expr) =>
                      extractBindingFromExpr(expr)
                        .orElse(findBindingFromIdLookups(expr, idLookups))
                        .orElse(findLocalIdBinding(onTarget))
                    case AnchorTarget.AllNodes => findLocalIdBinding(onTarget)
                  }
                  binding match {
                    case Some(b) if effectTargets.contains(b) => (deps, (anchor, b) :: targets, others)
                    case Some(b) if effectDeps.contains(b) => ((anchor, b) :: deps, targets, others)
                    case _ => (deps, targets, child :: others)
                  }
                case _ => (deps, targets, child :: others)
              }
            }

            // Can we restructure? Need at least one dep child and one target child
            if (depChildren.nonEmpty && targetChildren.nonEmpty && otherChildren.isEmpty) {
              // Build nested structure: outer deps, inner targets with effects
              // Start from innermost: targets with their effects, then wrap with dep anchors

              // Group effects by target
              val effectsByTarget = needsSeparateAnchor.groupBy(getEffectTarget)

              // Build innermost plan: target anchors with effects pushed in
              val targetPlans = targetChildren.map { case (anchor, binding) =>
                anchor match {
                  case QueryPlan.Anchor(target, onTarget) =>
                    val effectsForTarget = effectsByTarget.getOrElse(Some(binding), Nil)
                    if (effectsForTarget.nonEmpty) {
                      val clearedEffects = effectsForTarget.map(clearEffectTarget)
                      val withEffect =
                        QueryPlan.Sequence(
                          onTarget,
                          QueryPlan.LocalEffect(clearedEffects, QueryPlan.Unit),
                          ContextFlow.Extend,
                        )
                      QueryPlan.Anchor(target, withEffect)
                    } else {
                      anchor
                    }
                  case other => other
                }
              }

              // Combine target plans (if multiple, use Sequence)
              val targetPlan = targetPlans match {
                case single :: Nil => single
                case multiple => multiple.reduceLeft((acc, p) => QueryPlan.Sequence(acc, p, ContextFlow.Extend))
              }

              // Add remainder after targets
              val withRemainder2 =
                if (remainder != QueryPlan.Unit)
                  QueryPlan.Sequence(targetPlan, push(remainder, anchorContext), flow)
                else targetPlan

              // Wrap with dependency anchors (outermost)
              val result = depChildren.foldLeft(withRemainder2) { case (inner, (depAnchor, _)) =>
                depAnchor match {
                  case QueryPlan.Anchor(target, onTarget) =>
                    val withInner = QueryPlan.Sequence(onTarget, inner, ContextFlow.Extend)
                    QueryPlan.Anchor(target, withInner)
                  case _ => QueryPlan.Sequence(depAnchor, inner, ContextFlow.Extend)
                }
              }

              push(result, anchorContext)
            } else {
              // Fallback: can't restructure, create separate anchors (existing behavior)
              val byTarget = needsSeparateAnchor.groupBy(getEffectTarget)
              var result = withRemainder
              byTarget.foreach { case (targetOpt, targetEffects) =>
                targetOpt match {
                  case Some(target) =>
                    val clearedEffects = targetEffects.map(clearEffectTarget)
                    val effectPlan = QueryPlan.LocalEffect(clearedEffects, QueryPlan.Unit)
                    val anchoredEffect = QueryPlan.Anchor(AnchorTarget.Computed(makeBindingExpr(target)), effectPlan)
                    result = QueryPlan.Sequence(result, anchoredEffect, ContextFlow.Extend)
                  case None =>
                    result = QueryPlan.Sequence(result, QueryPlan.LocalEffect(targetEffects, QueryPlan.Unit), flow)
                }
              }
              result
            }
          } else {
            withRemainder
          }
        } else {
          // No effects to push - normal processing
          QueryPlan.Sequence(push(cp, anchorContext), push(effectRest, anchorContext), flow)
        }

      // Recurse into other structures
      case QueryPlan.Sequence(first, andThen, flow) =>
        QueryPlan.Sequence(push(first, anchorContext), push(andThen, anchorContext), flow)

      case QueryPlan.Anchor(target, onTarget) =>
        // Determine binding from this anchor and pass it to children
        // For Computed(Ident(n)), extract n directly
        // For Computed(Parameter(...)) or AllNodes, find the binding from LocalId inside
        val binding = target match {
          case AnchorTarget.Computed(expr) =>
            extractBindingFromExpr(expr)
              .orElse(findBindingFromIdLookups(expr, idLookups))
              .orElse(findLocalIdBinding(onTarget))
          case AnchorTarget.AllNodes => findLocalIdBinding(onTarget)
        }
        QueryPlan.Anchor(target, push(onTarget, binding.orElse(anchorContext)))

      case QueryPlan.CrossProduct(children, emitLazily) =>
        QueryPlan.CrossProduct(children.map(push(_, anchorContext)), emitLazily)

      case QueryPlan.Expand(label, direction, onDest) =>
        QueryPlan.Expand(label, direction, push(onDest, anchorContext))

      case QueryPlan.Unwind(expr, binding, child) =>
        QueryPlan.Unwind(expr, binding, push(child, anchorContext))

      case QueryPlan.LocalEffect(effects, child) =>
        // Separate CreateNode effects (always run locally) from targeted effects
        val (createEffects, targetedEffects) = effects.partition {
          case _: LocalQueryEffect.CreateNode => true
          case _ => false
        }

        // Partition targeted effects by whether they match current anchor context
        val (localEffects, remoteEffects) = targetedEffects.partition { e =>
          getEffectTarget(e) match {
            case Some(target) => anchorContext.contains(target) // Target matches current anchor
            case None => true // No target = local (shouldn't happen for targeted effects)
          }
        }

        // Start with the processed child
        var result = push(child, anchorContext)

        // Group remote effects by their target binding and wrap in anchors
        val effectsByTarget: Map[Option[Symbol], List[LocalQueryEffect]] = remoteEffects.groupBy(getEffectTarget)
        effectsByTarget.foreach { case (targetOpt, targetEffects) =>
          targetOpt match {
            case Some(target) =>
              val clearedEffects = targetEffects.map(clearEffectTarget)
              val effectPlan = QueryPlan.LocalEffect(clearedEffects, result)
              result = QueryPlan.Anchor(AnchorTarget.Computed(makeBindingExpr(target)), effectPlan)
            case None =>
              result = QueryPlan.LocalEffect(targetEffects, result)
          }
        }

        // Add local effects without anchor (we're already on the target node!)
        if (localEffects.nonEmpty) {
          val clearedLocalEffects = localEffects.map(clearEffectTarget)
          result = QueryPlan.LocalEffect(clearedLocalEffects, result)
        }

        // Add CreateNode effects at the outermost level
        if (createEffects.nonEmpty) {
          result = QueryPlan.LocalEffect(createEffects, result)
        }

        result

      // Leaf nodes - no change
      case other => other
    }

    push(plan, None)
  }

  // ============================================================
  // MAIN ENTRY POINT
  // ============================================================

  /** Plan a Cypher query to QueryPlan.
    *
    * @param cypherAst The parsed Cypher AST
    * @param symbolTable Symbol analysis results (currently unused but may be needed for type info)
    * @return A QueryPlan ready for execution
    */
  /** Result of planning a query - includes the plan and output metadata.
    *
    * @param plan The transformed query plan ready for execution
    * @param returnColumns Internal column names from RETURN clause (for filtering)
    * @param outputNameMapping Maps internal binding IDs to human-readable output names
    */
  case class PlannedQuery(
    plan: QueryPlan,
    returnColumns: Option[Set[Symbol]],
    outputNameMapping: Map[Symbol, Symbol],
  )

  def plan(cypherAst: Cypher.Query, symbolTable: SymbolAnalysisModule.SymbolTable): QueryPlan =
    planWithMetadata(cypherAst, symbolTable).plan

  /** Plan a query and return both the plan and metadata (return columns).
    *
    * Use this when you need the return columns for output filtering.
    */
  def planWithMetadata(cypherAst: Cypher.Query, symbolTable: SymbolAnalysisModule.SymbolTable): PlannedQuery = {
    val idLookups = extractIdLookups(cypherAst)
    val rawNodeDeps = getNodeDeps(cypherAst, symbolTable)
    // Propagate NodeDep.Id back through WITH alias chains
    // This ensures that if CREATE needs id(m) where m was renamed through WITH clauses,
    // the original binding also gets NodeDep.Id
    val aliases = extractWithAliases(cypherAst)
    val nodeDeps = propagateIdDepsBackward(rawNodeDeps, aliases)

    val rawPlan = cypherAst match {
      case _: Cypher.Query.Union =>
        throw new QuinePatternUnimplementedException("UNION queries not yet supported in planner")

      case single: Cypher.Query.SingleQuery =>
        single match {
          case spq: Cypher.Query.SingleQuery.SinglepartQuery =>
            val bodyPlan = planQueryParts(spq.queryParts, idLookups, nodeDeps, symbolTable)
            planProjection(spq.bindings, isDistinct = spq.isDistinct, bodyPlan, symbolTable)

          case mpq: Cypher.Query.SingleQuery.MultipartQuery =>
            // Multipart queries have WITH clauses that create sequence points
            // IMPORTANT: Flatten all parts together before planning so that UNWIND
            // correctly captures subsequent parts (including "into" parts) in its subquery.
            // If we planned them separately, UNWIND in queryParts wouldn't include
            // the parts from "into", leading to incorrect plan structure.
            val allParts = mpq.queryParts ++ mpq.into.queryParts
            val bodyPlan = planQueryParts(allParts, idLookups, nodeDeps, symbolTable)
            planProjection(mpq.into.bindings, isDistinct = mpq.into.isDistinct, bodyPlan, symbolTable)
        }
    }

    // Extract return columns from the raw plan BEFORE pushIntoAnchors transformation
    // This is needed because pushIntoAnchors may push the Project inside an Anchor
    val returnColumns = extractReturnColumns(rawPlan)

    // Build output name mapping: internal binding IDs -> human-readable names
    // This is used at output time to present user-friendly column names
    val outputNameMapping = buildOutputNameMapping(cypherAst, symbolTable)

    // Post-process to push effects inside anchors
    // This ensures SET/CREATE effects run on the actual node, not the dispatcher
    val transformedPlan = pushIntoAnchors(rawPlan, idLookups)

    PlannedQuery(transformedPlan, returnColumns, outputNameMapping)
  }

  /** Build a mapping from internal binding IDs to human-readable output names.
    * This mapping is used at output time to convert internal names to user-facing names.
    */
  private def buildOutputNameMapping(
    cypherAst: Cypher.Query,
    symbolTable: SymbolAnalysisModule.SymbolTable,
  ): Map[Symbol, Symbol] = {
    // Get the final projections (RETURN clause bindings)
    val finalProjections: List[Cypher.Projection] = cypherAst match {
      case single: Cypher.Query.SingleQuery =>
        single match {
          case spq: Cypher.Query.SingleQuery.SinglepartQuery => spq.bindings
          case mpq: Cypher.Query.SingleQuery.MultipartQuery => mpq.into.bindings
        }
      case _: Cypher.Query.Union => Nil // TODO: Handle union
    }

    // Build mapping: internal name -> human-readable name
    finalProjections.flatMap { p =>
      p.as match {
        case Right(quineId) =>
          val internalName = bindingSymbol(quineId.name)
          val humanReadableName = identDisplayName(p.as, symbolTable)
          Some(internalName -> humanReadableName)
        case Left(cypherIdent) =>
          // Unresolved identifier - use as-is
          Some(cypherIdent.name -> cypherIdent.name)
      }
    }.toMap
  }

  /** Extract return columns from the outermost Project with dropExisting=true.
    * This represents the RETURN clause's projection.
    */
  private def extractReturnColumns(plan: QueryPlan): Option[Set[Symbol]] = plan match {
    case QueryPlan.Project(columns, dropExisting, _) if dropExisting =>
      Some(columns.map(_.as).toSet)
    case _ =>
      None
  }

  /** Convenience method to wrap a plan with AllNodes anchor for standing query deployment */
  def wrapForStandingQuery(plan: QueryPlan): QueryPlan =
    QueryPlan.Anchor(AnchorTarget.AllNodes, plan)
}
