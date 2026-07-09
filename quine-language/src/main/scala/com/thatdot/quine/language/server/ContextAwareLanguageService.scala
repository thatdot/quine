package com.thatdot.quine.language.server

import scala.annotation.tailrec
import scala.jdk.CollectionConverters._
import scala.jdk.OptionConverters._
import scala.util.Try

import com.strumenta.antlr4c3.CodeCompletionCore
import org.antlr.v4.runtime.{CharStreams, CommonTokenStream, Vocabulary}

import com.thatdot.quine.cypher.ast.{Projection, Query}
import com.thatdot.quine.cypher.parsing.{CypherLexer, CypherParser}
import com.thatdot.quine.cypher.phases.{LexerPhase, LexerState, ParserPhase, SymbolAnalysisPhase, SymbolAnalysisState}
import com.thatdot.quine.cypher.visitors.semantic.QueryVisitor
import com.thatdot.quine.language.diagnostic.Diagnostic
import com.thatdot.quine.language.functions.FunctionDocRegistry
import com.thatdot.quine.language.phases.{Phase, TypeCheckingPhase, TypeCheckingState}
import com.thatdot.quine.language.procedures.{ProcedureDocRegistry, ProcedureRegistry, ProcedureSignature}
import com.thatdot.quine.language.semantic.SemanticToken
import com.thatdot.quine.language.types.Type

/** Hover documentation for a function or procedure invocation, located in ANTLR conventions so
  * the LSP layer can convert to protocol positions.
  *
  * @param markdown the hover body (Markdown)
  * @param startLine 1-based line of the invoked name's first character
  * @param startColumn 0-based code-point column of the invoked name's first character
  * @param endLine 1-based line of the invoked name's end
  * @param endColumn 0-based code-point column just past the invoked name's last character
  */
final case class InvocationHover(
  markdown: String,
  startLine: Int,
  startColumn: Int,
  endLine: Int,
  endColumn: Int,
)

/** One name or variable completion item, in ANTLR/Java-friendly types so the LSP layer can build a
  * `CompletionItem` from it directly.
  *
  * @param label the full canonical name shown in the popup, e.g. `reify.time` or `idFrom`
  * @param insertText the text accepting the item produces in the edit range the LSP layer
  *                   computes: the full name when the caret is on a plain word (`re` →
  *                   `reify.time`), or only the segment after the last typed dot when the caret
  *                   follows one (`reify.` / `reify.ti` → `time`), so accepting never doubles
  *                   the namespace
  * @param kind whether the item is a function, a procedure, or an in-scope variable; the LSP layer
  *             reads [[CompletionKind.itemKind]] for the protocol's `CompletionItemKind`
  * @param detail the signature line(s) shown beside the label, empty for standard built-ins
  * @param documentation the Markdown description shown in the popup, empty for standard
  *                      built-ins
  * @param filterText the text the client filters the typed prefix against — the same string as
  *                   `insertText`, so filtering matches what accepting inserts
  */
final case class NameCompletion(
  label: String,
  insertText: String,
  kind: CompletionKind,
  detail: java.util.Optional[String],
  documentation: java.util.Optional[String],
  filterText: String,
)

class ContextAwareLanguageService(nameSource: NameCompletionSource) {

  /** Construct with the static, in-module name source (the documentation registries). A running
    * Quine app instead constructs the service with a source backed by the live runtime registries,
    * so user-defined functions and procedures are offered as completions too.
    */
  def this() = this(NameCompletionSource.Static)

  import ContextAwareLanguageService._
  import TokenPosition._

  import com.thatdot.quine.language.phases.UpgradeModule._
  import com.thatdot.quine.cypher.phases.SymbolAnalysisModule.{PropertyAccessMappingMonoid, TableMonoid}
  import com.thatdot.quine.cypher.phases.MaterializationOutput.AggregationAccessMappingMonoid

  val cypherParser: Phase[LexerState, SymbolAnalysisState, String, Query] =
    LexerPhase andThen ParserPhase andThen SymbolAnalysisPhase

  /** The pipeline behind [[queryKind]]: parsing and symbol analysis as in [[cypherParser]], plus
    * type checking so node-valued projections are identifiable by their inferred types.
    */
  val queryKindPipeline: Phase[LexerState, TypeCheckingState, String, Query] =
    LexerPhase andThen ParserPhase andThen SymbolAnalysisPhase andThen TypeCheckingPhase()

  /** Computes the Cypher keywords the grammar allows at a caret position.
    *
    * Candidates come from walking the generated parser's ATN with CodeCompletionCore: the token
    * stream is matched up to the caret token and every token type that may appear there is
    * collected. When the caret touches a word, candidates are computed at that word's start, so a
    * partially typed keyword yields the keywords valid in its place (the LSP client filters the
    * set against the typed prefix). Only keyword tokens are reported; the grammar's explicit
    * whitespace token (SP), punctuation, and literal/identifier token types are omitted, as are
    * keyword tokens reachable only as names (the grammar lets keywords double as property keys,
    * labels, and variables — see [[ContextAwareLanguageService.namePreferredRules]]).
    *
    * @param queryText full text of the document being edited
    * @param line 0-based line of the caret (LSP convention)
    * @param character 0-based UTF-16 column of the caret (LSP convention); out-of-range positions
    *                  are clamped to the document
    * @return keyword labels valid at the caret, sorted alphabetically
    */
  def keywordCompletions(queryText: String, line: Int, character: Int): java.util.List[String] = {
    val lexer = new CypherLexer(CharStreams.fromString(queryText))
    lexer.removeErrorListeners()
    val tokenStream = new CommonTokenStream(lexer)
    tokenStream.fill()
    val tokens = tokenStream.getTokens.asScala.toVector

    caretTokenIndex(tokens, caretCodePointOffset(queryText, line, character)) match {
      case None => java.util.Collections.emptyList[String]()
      case Some(caretIndex) =>
        val parser = new CypherParser(tokenStream)
        parser.removeErrorListeners()
        val core = new CodeCompletionCore(parser)
        core.preferredRules = namePreferredRules
        // CodeCompletionCore caches follow sets in a static map shared across instances, so
        // candidate collection is serialized JVM-wide. The cache is computed from the ATN
        // alone; preferredRules is consulted only during traversal, so it does not poison
        // the cache.
        val candidateTokenTypes = candidateCollectionLock.synchronized {
          core.collectCandidates(caretIndex, null).tokens.keySet.asScala.toList
        }
        candidateTokenTypes
          .flatMap(tokenType => keywordLabel(tokenType.intValue, parser.getVocabulary))
          .sorted
          .asJava
    }
  }

  /** Computes the function-, procedure-, and variable-name completions to offer at a caret
    * position.
    *
    * Every name is offered everywhere completion fires (v1 makes no attempt to gate by grammar
    * position). Function and procedure names come from the injected [[NameCompletionSource]] —
    * the live runtime registries in a running Quine app (so user-defined functions and procedures
    * are offered too), or the static documentation registries by default — each carrying any
    * signature as `detail` and description as `documentation`. In-scope variable names come from
    * the buffer's own symbol table ([[CompletionKind.Variable]]); they are dotless, so they are
    * dropped once the caret is past a dot. Candidates are de-duplicated by lowercased label with
    * source names
    * inserted before variables, so a documented name is never shadowed by a same-named variable.
    * The client filters and sorts the returned set against the typed text.
    *
    * Dotted names complete both ways. When the caret is on a plain word (`re`), the full name
    * is offered (`reify.time`) and accepting it replaces the typed word. When the caret follows
    * a dot — resting just past it (`reify.`) or partway through a further segment (`reify.ti`)
    * — only candidates whose name continues the committed prefix (`reify.`) are offered, and
    * each `insertText` is the remainder after that prefix (`time`); paired with the LSP layer's
    * edit range, which covers only the segment after the dot, accepting yields the full name
    * rather than doubling the namespace. The committed prefix is taken verbatim from the buffer,
    * so the user's casing of the namespace is preserved.
    *
    * Unlike [[keywordCompletions]] this consults no parser ATN (names are offered everywhere)
    * and touches no shared mutable state, so it needs no candidate-collection lock.
    *
    * @param queryText full text of the document being edited
    * @param line 0-based line of the caret (LSP convention)
    * @param character 0-based UTF-16 column of the caret (LSP convention); out-of-range
    *                  positions are clamped to the document
    * @return the name completions valid at the caret, registries before bare built-ins
    */
  def nameCompletions(queryText: String, line: Int, character: Int): java.util.List[NameCompletion] = {
    val lexer = new CypherLexer(CharStreams.fromString(queryText))
    lexer.removeErrorListeners()
    val tokenStream = new CommonTokenStream(lexer)
    tokenStream.fill()
    val tokens = tokenStream.getTokens.asScala.toVector
    val prefix = committedDottedPrefix(tokens, caretCodePointOffset(queryText, line, character))
    val lowerPrefix = prefix.toLowerCase

    // Function/procedure names from the injected source first so they win the de-dup over a
    // same-named variable; in-scope variables (dotless) only when the caret is not past a dot.
    val nameCandidates: Vector[Candidate] =
      nameSource.names().toVector.map(n => Candidate(n.label, n.kind, n.detail, n.documentation))
    val variableCandidates: Vector[Candidate] =
      if (prefix.contains(".")) Vector.empty
      else inScopeVariables(queryText).map(name => Candidate(name, CompletionKind.Variable, None, None))
    val candidates: Vector[Candidate] = nameCandidates ++ variableCandidates

    def matchesPrefix(candidate: Candidate): Boolean = {
      val lowerLabel = candidate.label.toLowerCase
      prefix.isEmpty || (lowerLabel.startsWith(lowerPrefix) && lowerLabel.length > lowerPrefix.length)
    }

    // Keep the first candidate per lowercased label, preserving the source-first order of
    // `candidates` so a documented name wins over a same-named bare built-in or variable.
    candidates
      .filter(matchesPrefix)
      .distinctBy(_.label.toLowerCase)
      .map { candidate =>
        val insertText = if (prefix.isEmpty) candidate.label else candidate.label.substring(prefix.length)
        NameCompletion(
          label = candidate.label,
          insertText = insertText,
          kind = candidate.kind,
          detail = candidate.detail.toJava,
          documentation = candidate.documentation.toJava,
          filterText = insertText,
        )
      }
      .asJava
  }

  /** The distinct user-written variable names bound anywhere in the buffer, for completion.
    *
    * Built from the same lex/parse/symbol-analysis pass [[parseErrors]] runs, reading the symbol
    * table's binding declarations (anonymous/synthetic bindings have no original name and are
    * skipped). A parse that throws yields no variables rather than failing the completion request,
    * matching how [[queryKind]] degrades on a buffer state a phase cannot process.
    */
  private def inScopeVariables(queryText: String): Vector[String] =
    Try(analyzedState(queryText).symbolTable.references.flatMap(_.originalName.map(_.name)).distinct.toVector)
      .getOrElse(Vector.empty)

  /** Runs the lex/parse/symbol-analysis pipeline and returns its final state. The completion and
    * diagnostic paths share it, reading different fields (the symbol table vs. the diagnostics).
    */
  private def analyzedState(queryText: String): SymbolAnalysisState =
    cypherParser.process(queryText).value.runS(LexerState(List.empty)).value

  /** Computes hover documentation for the Quine-specific function or procedure invoked at a
    * position.
    *
    * The hovered token is resolved against the same lexing the completion candidates use: the
    * caret's LSP position is converted to a code-point offset and matched to the word-like token
    * (a run of letters, digits, or underscores) it touches, including the word's start and end
    * boundaries. The token is expanded to the full dotted name it is a segment of (`reify` and
    * `time` both resolve to `reify.time`), and the name documents a function or procedure only
    * when it is actually invoked — the next non-whitespace token must be `(`, which both
    * function applications and `CALL name(...)` syntax satisfy — so a variable or property that
    * merely shares a documented name never hovers. The name is looked up in
    * [[FunctionDocRegistry]] first and [[ProcedureDocRegistry]] second; the two registries
    * document disjoint name sets (functions and procedures are resolved by the runtime in
    * separate namespaces), so the order never selects between candidates. Keyword tokens pass
    * the same pipeline and fall out at the lookups, which know only Quine's documented names.
    *
    * @param queryText full text of the document being edited
    * @param line 0-based line of the hover position (LSP convention)
    * @param character 0-based UTF-16 column of the hover position (LSP convention);
    *                  out-of-range positions are clamped to the document
    * @return the invoked name's documentation and location, or empty when the position is not
    *         on a documented function or procedure invocation
    */
  def invocationHoverAt(queryText: String, line: Int, character: Int): java.util.Optional[InvocationHover] = {
    val lexer = new CypherLexer(CharStreams.fromString(queryText))
    lexer.removeErrorListeners()
    val tokenStream = new CommonTokenStream(lexer)
    tokenStream.fill()
    val tokens = tokenStream.getTokens.asScala.toVector
    val caretOffset = caretCodePointOffset(queryText, line, character)

    val maybeHover = for {
      hoveredIndex <- hoveredWordTokenIndex(tokens, caretOffset)
      TokenSpan(startIndex, endIndex) = dottedNameSpan(tokens, hoveredIndex)
      if isFollowedByOpenParen(tokens, endIndex)
      name = tokens.slice(startIndex, endIndex + 1).map(_.getText).mkString
      markdown <- FunctionDocRegistry
        .lookup(name)
        .map(_.markdown)
        .orElse(ProcedureDocRegistry.lookup(name).map(_.markdown))
    } yield {
      val startToken = tokens(startIndex)
      val endToken = tokens(endIndex)
      val endText = endToken.getText
      InvocationHover(
        markdown = markdown,
        startLine = startToken.getLine,
        startColumn = startToken.getCharPositionInLine,
        endLine = endToken.getLine,
        endColumn = endToken.getCharPositionInLine + endText.codePointCount(0, endText.length),
      )
    }
    maybeHover.fold(java.util.Optional.empty[InvocationHover]())(java.util.Optional.of)
  }

  /** Reports the lex/parse/symbol-analysis diagnostics for a buffer.
    *
    * Guarded the way [[queryKind]] is: a phase that throws on a buffer it cannot process degrades to
    * no diagnostics rather than erroring the LSP `textDocument/diagnostic` request. (Unsupported-but-
    * parseable constructs are surfaced as real diagnostics upstream; see
    * [[com.thatdot.quine.cypher.phases.ParserPhase]].) The swallowed Throwable is logged so the
    * degrade is observable.
    */
  def parseErrors(queryText: String): java.util.List[Diagnostic] =
    Try(analyzedState(queryText).diagnostics)
      .recover { case t =>
        logger.log(
          java.util.logging.Level.WARNING,
          "diagnostic analysis failed; reporting no diagnostics for this buffer",
          t,
        )
        Nil
      }
      .get
      .asJava

  /** Classifies a query as node-returning, side-effectful, tabular, or unclassifiable.
    *
    * The verdict is `node` exactly when every result column is provably a node value:
    *
    *   - for a regular query, the type checker must resolve every projection of the final
    *     RETURN to the primitive node type through the inferred type environment. Procedure
    *     yields (`CALL ... YIELD node RETURN node`) participate via the yielded bindings'
    *     declared types in [[com.thatdot.quine.language.procedures.ProcedureRegistry]];
    *   - for a standalone procedure call (`CALL recentNodes(10)`), which parses without
    *     errors but builds no AST, every (yielded) output column declared in the registry
    *     must be node-typed.
    *
    * The verdict is `sideEffects` when the query is well-formed but produces no result rows:
    *
    *   - for a regular query, the final part has no RETURN clause (empty `bindings`), so the
    *     result set is empty — e.g. `CREATE (n)`, `MATCH (n) SET n.x = 1`, or a multipart
    *     query whose last WITH-chain part has no projections;
    *   - for a standalone procedure call, the procedure is known and declares no output columns
    *     — e.g. `CALL util.sleep(10)` or `CALL debug.sleep(10)`.
    *
    * The verdict is `table` when the query is well-formed, produces rows, but is not provably
    * node-returning: at least one result column may hold a non-node value (a property,
    * aggregate, literal, or procedure yield). Tabular rendering accepts values of any shape,
    * so this verdict is always safe to execute. It also covers:
    *
    *   - `RETURN *` (the wildcard's columns are not enumerated in the AST),
    *   - yields of procedures or result fields the registry does not know (they type-check
    *     to fresh type variables),
    *   - standalone calls of procedures the registry does not know.
    *
    * `unknown` is reserved for buffers with no classifiable query: empty or whitespace-only
    * text, or lexer/parser/symbol-analysis errors (the same diagnostics [[parseErrors]]
    * reports). Type-check diagnostics do not affect the verdict because they are not surfaced
    * by [[parseErrors]] and do not stop a query from running.
    *
    * @param queryText full text of the document being edited
    * @return [[QueryKind.NODE]], [[QueryKind.TABLE]], [[QueryKind.SIDE_EFFECTS]],
    *         or [[QueryKind.UNKNOWN]]
    */
  def queryKind(queryText: String): QueryKind =
    if (queryText.isBlank) QueryKind.UNKNOWN
    else
      Try {
        val (state, maybeQuery) = queryKindPipeline.process(queryText).value.run(LexerState(List.empty)).value
        val hasBlockingErrors = state.diagnostics.exists {
          case _: Diagnostic.ParseError | _: Diagnostic.SymbolAnalysisError => true
          case _ => false
        }
        val provablyNodeReturning = maybeQuery match {
          case Some(query) => returnsOnlyNodes(query, state)
          case None => standaloneCallReturnsOnlyNodes(queryText)
        }
        val isNoRows =
          maybeQuery.exists(returnsNoRows) || standaloneCallIsZeroOutput(queryText)
        if (hasBlockingErrors) QueryKind.UNKNOWN
        else if (provablyNodeReturning) QueryKind.NODE
        else if (isNoRows) QueryKind.SIDE_EFFECTS
        else QueryKind.TABLE
        // The classifier must answer deterministically for any buffer state, so a pipeline failure
        // (e.g. an AST shape a phase cannot process) yields no verdict instead of failing the request.
      }.getOrElse(QueryKind.UNKNOWN)

  def semanticAnalysis(queryText: String): java.util.List[SemanticToken] = {
    val input = CharStreams.fromString(queryText)
    val lexer = new CypherLexer(input)
    lexer.removeErrorListeners()

    val tokens = new CommonTokenStream(lexer)
    val parser = new CypherParser(tokens)
    parser.removeErrorListeners()

    val tree = parser.oC_Query()

    QueryVisitor.visitOC_Query(tree).asJava
  }
}

object ContextAwareLanguageService {

  /** java.util.logging is the LSP layer's logging facility; this module declares no other logging
    * dependency. [[parseErrors]] logs a swallowed analysis failure here so its backstop is observable.
    */
  private val logger: java.util.logging.Logger =
    java.util.logging.Logger.getLogger(classOf[ContextAwareLanguageService].getName)

  /** Guards CodeCompletionCore.collectCandidates, whose static follow-set cache is not
    * thread-safe.
    */
  private val candidateCollectionLock = new Object

  /** Parser rules at which CodeCompletionCore stops descending, reporting the rule instead of
    * the keyword tokens inside it.
    *
    * The grammar lets most keywords double as names: every reserved word is a valid property
    * key, label, or relationship type (oC_SchemaName : oC_SymbolicName | oC_ReservedWord), and
    * a handful (COUNT, FILTER, EXTRACT, ANY, ALL, NONE, SINGLE, CREATE) are valid variable,
    * function, and procedure names (oC_SymbolicName). A keyword whose only path to the caret
    * runs through these rules is a name in that position, not a keyword — suggesting it would
    * offer the whole keyword vocabulary after `n.` or inside a map literal. Keywords with a
    * direct use at the caret (for example COUNT in `COUNT(*)` or ALL in a quantifier) are
    * still reported through their direct paths.
    */
  private val namePreferredRules: java.util.Set[Integer] =
    java.util.Set.of(
      Int.box(CypherParser.RULE_oC_SchemaName),
      Int.box(CypherParser.RULE_oC_SymbolicName),
    )

  /** Whether every column of the query's final RETURN is provably a node value.
    *
    * The final projections are the `bindings` of the query's last single-part query (for a
    * multipart query, the part its WITH chain feeds `into`); a UNION returns nodes only when
    * both sides do. A wildcard RETURN never qualifies because its columns are not enumerated in
    * the AST, and a query with no projections returns no node rows to render.
    */
  private def returnsOnlyNodes(query: Query, state: TypeCheckingState): Boolean =
    query match {
      case union: Query.Union =>
        returnsOnlyNodes(union.lhs, state) && returnsOnlyNodes(union.rhs, state)
      case single: Query.SingleQuery =>
        val finalPart = single match {
          case singlepart: Query.SingleQuery.SinglepartQuery => singlepart
          case multipart: Query.SingleQuery.MultipartQuery => multipart.into
        }
        finalPart.bindings.nonEmpty &&
        !finalPart.hasWildcard &&
        finalPart.bindings.forall(projectsNode(_, state))
    }

  /** Parses `queryText` as a whole-buffer standalone `CALL` and resolves the invoked procedure
    * against [[ProcedureRegistry]].
    *
    * Returns the parsed `oC_StandaloneCall` context paired with the resolved signature when the
    * buffer is a standalone call naming a registered procedure (looked up by name,
    * case-insensitively, matching the runtime registries' resolution). Returns `None` for any
    * other buffer shape or an unregistered procedure — both of which fall to the `table` verdict.
    */
  private def resolveStandaloneCall(
    queryText: String,
  ): Option[(CypherParser.OC_StandaloneCallContext, ProcedureSignature)] = {
    val lexer = new CypherLexer(CharStreams.fromString(queryText))
    lexer.removeErrorListeners()
    val parser = new CypherParser(new CommonTokenStream(lexer))
    parser.removeErrorListeners()

    for {
      statement <- Option(parser.oC_Cypher().oC_Statement())
      query <- Option(statement.oC_Query())
      standaloneCall <- Option(query.oC_StandaloneCall())
      procedureName <- Option(standaloneCall.oC_ExplicitProcedureInvocation())
        .map(_.oC_ProcedureName())
        .orElse(Option(standaloneCall.oC_ImplicitProcedureInvocation()).map(_.oC_ProcedureName()))
        .map(_.getText)
      signature <- ProcedureRegistry.lookup(procedureName)
    } yield (standaloneCall, signature)
  }

  /** Whether the buffer is a standalone procedure call whose result columns are all node
    * values.
    *
    * A standalone `CALL proc(args)` is the one error-free query shape that builds no AST, so
    * node-ness comes from [[ProcedureRegistry]] (via [[resolveStandaloneCall]]) instead of the
    * type checker. The result columns are the procedure's declared output columns, narrowed by
    * an explicit YIELD list (`YIELD *` keeps all columns; YIELD result fields are matched
    * case-sensitively, and an undeclared field disqualifies the column set). The call returns
    * only nodes when there is at least one result column and every one is node-typed.
    *
    * A buffer that yields anything not provably a node is not node-returning — as a runnable
    * query it falls to the `table` verdict.
    */
  private def standaloneCallReturnsOnlyNodes(queryText: String): Boolean =
    resolveStandaloneCall(queryText).exists { case (standaloneCall, signature) =>
      Option(standaloneCall.oC_YieldItems()) match {
        // No YIELD clause, or YIELD *: the result columns are all declared outputs.
        case None => signature.returnsOnlyNodes
        case Some(yieldItems) =>
          val yieldedTypes = yieldItems.oC_YieldItem().asScala.toList.map { yieldItem =>
            val resultField = Option(yieldItem.oC_ProcedureResultField())
              .map(_.getText)
              .getOrElse(yieldItem.oC_Variable().getText)
            signature.outputType(resultField)
          }
          yieldedTypes.nonEmpty && yieldedTypes.forall(_.contains(Type.PrimitiveType.NodeType))
      }
    }

  /** Whether the query produces no result rows.
    *
    * A query returns no rows when its final part has no projections (`bindings.isEmpty`) and no
    * wildcard RETURN: there is no RETURN clause at all, so the result set is empty regardless of
    * how many graph nodes or properties are touched. `RETURN *` is excluded because its wildcard
    * expands to all in-scope variables at runtime — those rows exist, even though the AST does
    * not enumerate them. For a UNION, both sides must have no rows.
    *
    * This is the AST-path counterpart to [[standaloneCallIsZeroOutput]], which covers the
    * standalone-call path where no AST is built.
    */
  private def returnsNoRows(query: Query): Boolean =
    query match {
      case union: Query.Union =>
        returnsNoRows(union.lhs) && returnsNoRows(union.rhs)
      case single: Query.SingleQuery =>
        val finalPart = single match {
          case singlepart: Query.SingleQuery.SinglepartQuery => singlepart
          case multipart: Query.SingleQuery.MultipartQuery => multipart.into
        }
        finalPart.bindings.isEmpty && !finalPart.hasWildcard
    }

  /** Whether the buffer is a standalone procedure call with zero declared output columns.
    *
    * A standalone `CALL proc(args)` whose procedure is registered with no output columns (e.g.
    * `util.sleep`, `debug.sleep`) runs purely for its side effects and yields no result rows.
    * Resolution goes through [[resolveStandaloneCall]]; the result is true only when the procedure
    * is known AND declares no output columns.
    *
    * An unknown procedure falls to `table` (same as [[standaloneCallReturnsOnlyNodes]]) because
    * without a signature the output shape is not statically knowable.
    */
  private def standaloneCallIsZeroOutput(queryText: String): Boolean =
    resolveStandaloneCall(queryText).exists { case (_, signature) => signature.outputs.isEmpty }

  /** Whether a single projection's binding has the inferred type of a node.
    *
    * The type checker records a type for every projection alias in the symbol table; that type
    * is resolved through the type environment and compared to the primitive node type. A
    * projection whose alias was not rewritten to a binding id, has no recorded type, or
    * resolves to anything but a node (including an unresolved type variable) is not a node
    * column.
    */
  private def projectsNode(projection: Projection, state: TypeCheckingState): Boolean =
    projection.as match {
      case Right(bindingId) =>
        state.symbolTable.typeVars
          .find(_.identifier == bindingId)
          .exists(entry => resolveType(entry.ty, state.typeEnv) == Type.PrimitiveType.NodeType)
      case Left(_) => false
    }

  /** Resolves a type by following type-variable bindings in the type environment.
    *
    * The traversal mirrors the type checker's `deref`, with a visited set so a malformed
    * environment cannot loop. An unbound type variable resolves to itself.
    */
  @tailrec
  private def resolveType(ty: Type, typeEnv: Map[Symbol, Type], seen: Set[Symbol] = Set.empty): Type =
    ty match {
      case Type.TypeVariable(id, _) if !seen.contains(id) =>
        typeEnv.get(id) match {
          case Some(next) => resolveType(next, typeEnv, seen + id)
          case None => ty
        }
      case _ => ty
    }

  /** Keyword lexer rules are named in upper case (UNION, MATCH, ...); other token types
    * (identifiers, literals, punctuation) have mixed-case symbolic names or none at all.
    */
  private val KeywordNamePattern = "[A-Z][A-Z_]*".r

  /** A de-duplication candidate inside [[ContextAwareLanguageService.nameCompletions]]: a name from
    * a [[NameCompletionSource]] (a function or procedure) or an in-scope variable, unified so both
    * pass through the same prefix-filter, de-dup, and edit-range logic.
    */
  final private case class Candidate(
    label: String,
    kind: CompletionKind,
    detail: Option[String],
    documentation: Option[String],
  )

  /** Maps a candidate token type to a completion label, or None for non-keyword tokens.
    *
    * The keyword's text is its lexer rule name, except L_SKIP, named to avoid ANTLR's reserved
    * word `skip`. SP (the grammar's explicit whitespace token) and token types without symbolic
    * names (EOF, punctuation literals) are not keywords.
    */
  private def keywordLabel(tokenType: Int, vocabulary: Vocabulary): Option[String] =
    if (tokenType <= 0) None
    else
      Option(vocabulary.getSymbolicName(tokenType)).collect {
        case "L_SKIP" => "SKIP"
        case name if name != "SP" && KeywordNamePattern.matches(name) => name
      }
}
