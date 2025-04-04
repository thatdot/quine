package com.thatdot.quine.compiler

import scala.util.control.NonFatal

import cats.implicits._
import com.github.blemale.scaffeine.{Cache, Scaffeine}
import org.opencypher.v9_0.ast.semantics
import org.opencypher.v9_0.ast.semantics.SemanticFeature
import org.opencypher.v9_0.expressions.LabelExpression
import org.opencypher.v9_0.frontend.phases._
import org.opencypher.v9_0.frontend.phases.rewriting.cnf.{CNFNormalizer, rewriteEqualityToInPredicate}
import org.opencypher.v9_0.frontend.{PlannerName, phases}
import org.opencypher.v9_0.rewriting.Deprecations.{semanticallyDeprecatedFeatures, syntacticallyDeprecatedFeatures}
import org.opencypher.v9_0.rewriting.rewriters.Forced
import org.opencypher.v9_0.util.OpenCypherExceptionFactory.SyntaxException
import org.opencypher.v9_0.util.{
  AnonymousVariableNameGenerator,
  CancellationChecker,
  CypherExceptionFactory,
  ErrorMessageProvider,
  InputPosition,
  NotImplementedErrorMessageProvider,
  OpenCypherExceptionFactory,
  RecordingNotificationLogger,
  symbols,
}
import org.opencypher.v9_0.{ast, expressions}

import com.thatdot.common.logging.Log.LogConfig
import com.thatdot.quine.graph.cypher._
import com.thatdot.quine.graph.{CypherOpsGraph, GraphQueryPattern, NamespaceId}
import com.thatdot.quine.model.{Milliseconds, QuineIdProvider}
import com.thatdot.quine.util.MonadHelpers._

package object cypher {

  /** Compile a Cypher statement
    *
    * @param statement statement to compile
    * @param paramsIdx what parameters are in scope?
    * @param initialCols what columns are initially in scope?
    */
  def compileStatement(
    statement: ast.Statement,
    avng: AnonymousVariableNameGenerator,
    paramsIdx: ParametersIndex,
    initialCols: Vector[Symbol],
  )(implicit
    source: SourceText,
  ): Query[Location.Anywhere] =
    statement match {

      /* TODO: implement this. Commands include:
       *
       *   - creating indices
       *   - dropping indices
       *   - creating constraints
       *   - dropping constraints
       *
       * Some design space needs to be explored here.
       */
      case _: ast.SchemaCommand =>
        throw CypherException.Compile(
          "Cypher commands are not supported (only queries)",
          Some(position(statement.position)),
        )

      /* TODO: periodic commit hint, which Alec thinks is only relevant for
       *       transactions when running `LOAD CSV`. See
       *       <https://neo4j.com/docs/cypher-manual/current/query-tuning/using/#query-using-periodic-commit-hint>
       */
      case ast.Query(queryPart) =>
        val queryScopeInfo = initialCols.foldLeft(QueryScopeInfo.empty)(_.addColumn(_)._1)
        QueryPart.compile(queryPart, avng).run(paramsIdx, source, queryScopeInfo).valueOr(throw _)

      case other =>
        throw CypherException.Compile(
          wrapping = s"Unexpected AST element: $other",
          position = Some(position(other.position)),
        )
    }

  // Guava (thread-safe) cache
  private[this] val compiledQueryCache: Cache[UncompiledQueryIdentity, (Query[Location.Anywhere], Parameters)] =
    Scaffeine()
      .maximumSize(1024) // TODO parameterize -- 1024 is 100% arbitrary
      .build()

  /** core utility to actually do the query compilation
    *
    * @see [[compile]]
    * @see [[compileCached]]
    */
  @throws[CypherException]
  private def compileFresh(
    queryIdentity: UncompiledQueryIdentity,
    customParsingContext: Option[(InputPosition, SourceText)],
  ): (Query[Location.Anywhere], Parameters) = {
    // parameters passed to openCypher only on load
    // these are used for producing (helpful) errors, but errors which may not be relevant on reuse of the query
    val sourceForParseErrors = customParsingContext.fold(SourceText(queryIdentity.queryText))(_._2)
    val parserStartPosition = customParsingContext.fold(InputPosition(0, 1, 1))(_._1)

    // Run `front-end` stuff to get back the statement and parameters
    val astState = openCypherParseAndRewrite(
      queryIdentity.queryText,
      queryIdentity.initialColumns,
      parserStartPosition,
      openCypherPipeline,
    )(sourceForParseErrors)
    val (fixedParameters: Parameters, paramsIdx: ParametersIndex) = {
      var idx = 0
      val paramsIdxMap = Map.newBuilder[String, Int]

      for (paramName <- queryIdentity.unfixedParameters) {
        paramsIdxMap += (paramName -> idx)
        idx += 1
      }

      val paramArray = IndexedSeq.newBuilder[Value]
      for ((paramName, paramJavaValue) <- astState.extractedParams()) {
        val paramValue = Value.fromAny(paramJavaValue).getOrThrow
        paramsIdxMap += (paramName -> idx)
        paramArray += paramValue
        idx += 1
      }

      (Parameters(paramArray.result()), ParametersIndex(paramsIdxMap.result()))
    }

    val initialCols: Vector[Symbol] = queryIdentity.initialColumns.view.map(c => Symbol(c._1)).toVector
    val compiled = VariableRewriter.convertAnyQuery(
      compileStatement(astState.statement(), astState.anonymousVariableNameGenerator, paramsIdx, initialCols)(
        SourceText(queryIdentity.queryText),
      ),
      Columns.Specified.empty,
    )
    (compiled, fixedParameters)

  }

  /** Compile, or load from [[compiledQueryCache]], the Query and fixed Parameters corresponding to the provided
    * [[queryIdentity]]. If this query needs to be loaded into the cache, use the provided customParsingContext
    * for any parse errors
    * @param queryIdentity the query to compile
    * @param customParsingContext the context to use for rich parsing errors, if any
    * @throws [[CypherException]] when the provided query is invalid, [[UncheckedExecutionException]] when something
    *         unexpected is thrown
    * @return (the compiled query, the compiled fixedParameters). These results are also guaranteed to be present in
    *         [[compiledQueryCache]], keyed by [[queryIdentity]]
    */
  @throws[CypherException]
  private def compileCached(
    queryIdentity: UncompiledQueryIdentity,
    customParsingContext: Option[(InputPosition, SourceText)],
  ): (Query[Location.Anywhere], Parameters) =
    try compiledQueryCache.get(
      queryIdentity,
      newQueryIdentity => compileFresh(newQueryIdentity, customParsingContext),
    )
    catch {
      case _: IllegalArgumentException =>
        throw CypherException.Compile(
          s"""Attempting to compile query by way of cache caused an infinite loop. Retry compilation with
             |caching disabled. Query was: ${queryIdentity.queryText}
             |""".stripMargin.replace('\n', ' ').trim,
          position = None,
        )
      case runtimeException: RuntimeException =>
        runtimeException.getCause match {
          case ce: CypherException => throw ce
          case _ =>
            throw CypherException.Compile(
              s"Unexpected error while compiling query: ${runtimeException.getClass.getSimpleName} ${runtimeException.getMessage}",
              position = None,
            )
        }
      case ce: CypherException =>
        throw ce
    }

  /** Compile a query
    *
    * @param queryText the Cypher query
    * @param unfixedParameters constants that will be passed to the query at runtime
    * @param initialColumns columns that should be assumed to already be in scope
    * @param customParsingContext override the input position and source text used for errors during parsing
    * @param cache whether the query compilation should be cached
    *
    * @return the compiled query and the results
    */
  @throws[CypherException]
  def compile(
    queryText: String,
    unfixedParameters: Seq[String] = Seq.empty,
    initialColumns: Seq[(String, symbols.CypherType)] = Seq.empty,
    customParsingContext: Option[(InputPosition, SourceText)] = None,
    cache: Boolean = true,
  ): CompiledQuery[Location.Anywhere] = {
    val uncompiled = UncompiledQueryIdentity(queryText, unfixedParameters, initialColumns)

    val (compiled, fixedParameters) =
      if (cache) compileCached(uncompiled, customParsingContext)
      else compileFresh(uncompiled, customParsingContext)

    CompiledQuery(
      Some(uncompiled.queryText),
      query = compiled,
      unfixedParameters,
      fixedParameters,
      uncompiled.initialColumns.map(_._1),
    )
  }

  /** Compile an expression
    *
    * @param expressionText the Cypher expression
    * @param unfixedParameters constants that will be passed to the query at runtime
    * @param initialColumns columns that should be assumed to already be in scope
    * @param customErrorContext override the input position and source text used for errors
    * @param cache whether the query compilation should be cached
    *
    * @return the compiled expression
    */
  @throws[CypherException]
  def compileExpression(
    expressionText: String,
    unfixedParameters: Seq[String] = Seq.empty,
    initialColumns: Seq[(String, symbols.CypherType)] = Seq.empty,
    customErrorContext: Option[(InputPosition, SourceText)] = None,
    cache: Boolean = true,
  ): CompiledExpr = {
    val returnPrefix = "RETURN "
    val startPosition = InputPosition(-returnPrefix.length, 1, 1 - returnPrefix.length)
    val sourceText = SourceText(expressionText)
    val compiled = compile(
      returnPrefix + expressionText,
      unfixedParameters,
      initialColumns,
      customErrorContext.orElse(Some(startPosition -> sourceText)),
      cache,
    )
    compiled.query match {
      case Query.AdjustContext(true, Vector((_, compiledExpr)), Query.Unit(_), _) =>
        CompiledExpr(
          expressionText,
          compiledExpr,
          compiled.unfixedParameters,
          compiled.fixedParameters,
          compiled.initialColumns,
        )
      case _ =>
        throw CypherException.Compile("Cypher expression cannot be evaluated outside a graph", None)
    }
  }

  /** Try to compile queries of the form `MATCH <pattern> WHERE <condition> RETURN [DISTINCT] <columns>`
    * into a pattern that can be used to construct a standing query.
    *
    * @param queryText the Cypher query
    */
  @throws[CypherException]
  def compileStandingQueryGraphPattern(
    queryText: String,
  )(implicit idProvider: QuineIdProvider, logConfig: LogConfig): GraphQueryPattern = {
    val source = SourceText(queryText)
    val startPosition = InputPosition(0, 1, 1)
    // compile and do basic (front-end) semantic analysis on queryText
    val astState = openCypherParseAndRewrite(queryText, Seq.empty, startPosition, openCypherStandingPipeline)(source)
    StandingQueryPatterns.compile(astState.statement(), astState.anonymousVariableNameGenerator, ParametersIndex.empty)(
      source,
      idProvider,
      logConfig,
    )
  }

  /** Compile and run a query on the graph
    *
    * @param queryText the Cypher query
    * @param parameters constants in the query
    * @param initialColumns columns already in scope
    * @param atTime moment in time to query ([[None]] represents the present)
    * @param namespace Which namespace to query. Default namespece unless specified.
    * @param graph the graph on which to run the query
    * @param timeout how long before timing out the query
    * @param cacheCompilation Whether to cache query compilation
    *
    * @return the compiled query and the results
    */
  @throws[CypherException]
  def queryCypherValues(
    queryText: String,
    namespace: NamespaceId,
    parameters: Map[String, Value] = Map.empty,
    initialColumns: Map[String, Value] = Map.empty,
    atTime: Option[Milliseconds] = None,
    cacheCompilation: Boolean = true,
  )(implicit
    graph: CypherOpsGraph,
  ): RunningCypherQuery = {

    val initialCompiledColumns: Seq[(String, symbols.CypherType)] = initialColumns.toSeq.map { case (col, value) =>
      (col, OpenCypherUdf.typeToOpenCypherType(value.typ))
    }

    val compiledQuery = compile(queryText, parameters.keys.toSeq, initialCompiledColumns, cache = cacheCompilation)
    graph.cypherOps.query(compiledQuery, namespace, atTime, parameters)
  }

  /** The openCypher `front-end` pipeline that will parse, validate, and
    * normalize queries before we start trying to turn them into the IR AST that
    * runs in Quine
    *
    * @see openCypherParseAndRewrite
    */
  private val openCypherPipeline: Transformer[BaseContext, BaseState, BaseState] = {
    import org.opencypher.v9_0.frontend.phases._

    // TODO What is the semantic equivalent of SemanticFeature.CorrelatedSubQueries
    val supportedFeatures = Array.empty[SemanticFeature]
    // format: off
    val parsingPhase = {
      OpenCypherJavaCCParsing                                                   andThen
      SyntaxDeprecationWarningsAndReplacements(syntacticallyDeprecatedFeatures) andThen
      PreparatoryRewriting                                                      andThen
      patternExpressionAsComprehension                                          andThen
      SemanticAnalysis(warn = true, supportedFeatures.toIndexedSeq: _*)         andThen
      SyntaxDeprecationWarningsAndReplacements(semanticallyDeprecatedFeatures)  andThen
      AstRewriting()                                                            andThen
      ProjectNamedPathsRewriter                                                 andThen
      LiteralExtraction(Forced)
      //Transformer.printAst("parsed ad hoc")
    }

    val stepList = CNFNormalizer.steps.toList
    val first: Transformer[BaseContext,BaseState,BaseState] = stepList.head
    val rest: List[Transformer[BaseContext,BaseState,BaseState]] = stepList.tail

    // format: off
    val rewritePhase = {
      isolateAggregation andThen
      SemanticAnalysis(warn = false, supportedFeatures.toIndexedSeq: _*) andThen
      Namespacer andThen
      transitiveClosure andThen
      rewriteEqualityToInPredicate andThen
      rest.foldLeft(first)(_ andThen _) andThen
      collapseMultipleInPredicates andThen
      SemanticAnalysis(warn = false, supportedFeatures.toIndexedSeq: _*)
    } // CompilationPhases.lateAstRewriting

    // format: off
    val pipeline = {
      parsingPhase              andThen
      resolveFunctions          andThen
      //Transformer.printAst("resolved") andThen
      resolveCalls              andThen
      rewritePhase
    }

    pipeline
  }

  /** The openCypher `front-end` pipeline that will parse, validate, and
    * normalize standing queries before we start trying to turn them into the IR
    * AST that runs in Quine
    *
    * @note Unlike [[openCypherPipeline]], this opts out of much more of the openCypher analysis
    * pipeline. This is because a lot of the re-writings that pipeline does complicate the
    * compilation process for us (eg. introduce parameters, alias common subexpressions using
    * `WITH`). In particular, this pipeline does NOT check for syntax deprecation, perform AST
    * rewriting (ie, normalization to reduce unused or redundant AST nodes), or perform preparatory
    * rewriting (ie, normalization of with, where, merge in, and call clauses, and of aliased functions)
    *
    * @see [[openCypherParseAndRewrite]]
    * @see [[openCypherPipeline]]
    */
  private val openCypherStandingPipeline: Transformer[BaseContext, BaseState, BaseState] = {
    import org.opencypher.v9_0.frontend.phases.CompilationPhaseTracer.CompilationPhase.AST_REWRITE
    import org.opencypher.v9_0.frontend.phases._
    import org.opencypher.v9_0.rewriting.rewriters.normalizeWithAndReturnClauses
    import org.opencypher.v9_0.util.StepSequencer

    val supportedFeatures = Array[SemanticFeature]()

    case object aliasReturns extends Phase[BaseContext, BaseState, BaseState] {
      override def process(from: BaseState, context: BaseContext): BaseState = {
        val rewriter = normalizeWithAndReturnClauses.getRewriter(context.cypherExceptionFactory, context.notificationLogger)
        val rewrittenStatement = from.statement().endoRewrite(rewriter)
        from.withStatement(rewrittenStatement)
      }

      override val phase = AST_REWRITE
      override def postConditions: Set[StepSequencer.Condition] = Set.empty
    }

    // format: off
    val parsingPhase = {
      OpenCypherJavaCCParsing                                           andThen
      patternExpressionAsComprehension                                  andThen
      aliasReturns                                                      andThen
      SemanticAnalysis(warn = true, supportedFeatures.toIndexedSeq: _*) // andThen
      // COMMENTARY ON QU-1292: There is a compilation error thrown when using exists() with
      // pattern expressions/comprehensions in SQ pattern queries. Ethan spent a few days
      // exploring options for fixing the issues, but ultimately it was not the most valuable use of time.
      // There are 2 main options for fixing the bug, one by further fixing the OC pipeline (option 1),
      // the other by extending Quine's SQ support for queries rewritten by OC pipelines (option 2).
//            new CustomAstRewriting(SameNameNamer)(
        // option 1: using nameAllPatternElementsInPatternComprehensions (a simplification of `nameAllPatternElements`
        // implemented below) fixes the original error, but violates some unknown precondition for
        // [[inlineNamedPathsInPatternComprehensions]] causing an unsafe None.get that throws a useless error message
        // Possible fix: Reimplement the subset of inlineNamedPathsInPatternComprehensions that we need
//        nameAllPatternElementsInPatternComprehensions,
        // option 2: fixes the original error to something more helpful ("invalid use of node variable `n`),
        // but rewrites anonymous edges to named edges, which we don't know how to support. Also sometimes
        // adds node variables we don't know how to support. Possible fix: parse edge variables during SQ
        // post-compilation checks and validate whether their uses are legitimate (as we do with node variables)
//        nameAllPatternElements,
//        normalizeMatchPredicates,
        // In either case, finish up with this rewrite:
//        inlineNamedPathsInPatternComprehensions, // (maybe also projectNamedPaths)
//      ) andThen
//      Transformer.printAst("parsed SQ")
    }

    // format: off
    val pipeline = {
      parsingPhase              andThen
      resolveFunctions          andThen
  //    Transformer.printAst("resolved") andThen
      resolveCalls
    }

    pipeline
  }

  private val openCypherPlanner = new PlannerName {
    override def name: String = "quine_planner"
    override def toTextOutput: String = "Quine Planner"
    override def version: String = "0.1"
  }

  /** Run a query through the openCypher `front-end` pipeline
    *
    * @param queryText the Cypher query
    * @param initialColumns columns already in scope
    * @param startPosition initial position of the query test
    * @param pipeline set of transformation steps through which to run
    */
  @throws[CypherException]
  private[quine] def openCypherParseAndRewrite(
    queryText: String,
    initialColumns: Seq[(String, symbols.CypherType)],
    startPosition: InputPosition,
    pipeline: Transformer[BaseContext, BaseState, BaseState]
  )(
    implicit
    source: SourceText
  ): BaseState = {
    val initial = phases.InitialState(
      queryText = queryText,
      startPosition = Some(startPosition),
      plannerName = openCypherPlanner,
      anonymousVariableNameGenerator = new AnonymousVariableNameGenerator(),
      maybeStatement = None,
      maybeSemantics = None,
      maybeExtractedParams = None,
      maybeSemanticTable = None,
      accumulatedConditions = Set(),
      maybeReturnColumns = None,
      maybeObfuscationMetadata = None
    )

    val errors = collection.mutable.ListBuffer.empty[semantics.SemanticErrorDef]
    val baseContext = new BaseContext {

      override def tracer = phases.CompilationPhaseTracer.NO_TRACING

      override def notificationLogger = new RecordingNotificationLogger()

      override def cypherExceptionFactory: CypherExceptionFactory = OpenCypherExceptionFactory(initial.startPosition)

      override def errorMessageProvider: ErrorMessageProvider = NotImplementedErrorMessageProvider

      override def cancellationChecker: CancellationChecker = CancellationChecker.NeverCancelled

      /* This is gross. The only way I found to understand how to reasonably
       * implement this was to look at the corresponding code in Neo4j. I'm
       * still not fully clear on what purpose this serves...
       */
      override def monitors = new phases.Monitors {

        import java.lang.reflect.{InvocationHandler, Method, Proxy}

        import scala.reflect.{ClassTag, classTag}

        def newMonitor[T <: AnyRef : ClassTag](tags: String*): T = {
          val cls: Class[_] = classTag[T].runtimeClass
          require(cls.isInterface(), "Monitor expects interface")

          val invocationHandler = new InvocationHandler {
            override def invoke(
                                 proxy: AnyRef,
                                 method: Method,
                                 args: Array[AnyRef]
                               ): AnyRef = new Object()
          }

          Proxy
            .newProxyInstance(cls.getClassLoader, Array(cls), invocationHandler)
            .asInstanceOf[T]
        }

        def addMonitorListener[T](monitor: T, tags: String*) = ()
      }

      override def errorHandler: Seq[semantics.SemanticErrorDef] => Unit =
        (errs: Seq[semantics.SemanticErrorDef]) => errors ++= errs
    }

    // Run the pipeline
    val output = try pipeline.transform(initial, baseContext)
    catch {
      case error: SyntaxException =>
        throw CypherException.Syntax(
          wrapping = error.getMessage(),
          position = Some(position(error.pos))
        )

      // TODO: can something better than this be done? What sorts of errors
      // can these be?
      case NonFatal(error) =>
        throw CypherException.Compile(
          wrapping = error.toString,
          position = None
        )
    }

    // TODO: better error reporting (e.g. can we classify these better?)
    // TODO: report more than just one error
    for (error <- errors.headOption) {
      throw CypherException.Compile(
        wrapping = error.msg,
        position = Some(position(error.position))
      )
    }

    output
  }

  /** Register (or overwrite) a UDF
    *
    * @param udf custom (scalar) user-defined function
    */
  def registerUserDefinedFunction(udf: UserDefinedFunction): Unit =
    Func.userDefinedFunctions += udf.name.toLowerCase -> udf

  /** Register (or overwrite) a UDP
    *
    * @param udp custom user-defined procedure
    */
  def registerUserDefinedProcedure(udp: UserDefinedProcedure): Unit =
    Proc.userDefinedProcedures += udp.name.toLowerCase -> udp

  /** Convert an openCypher variable into what our compilation APIs want */
  def logicalVariable2Symbol(lv: expressions.LogicalVariable): Symbol =
    Symbol(lv.name)

  def position(input: InputPosition)(implicit source: SourceText): Position = Position(
    input.line,
    input.column,
    input.offset,
    source
  )

  //TODO Bugs and things to do and everything is awful
  def handleLabelExpression(le: LabelExpression, maybeLoc: Option[Position]): Set[Symbol] =
    le.replaceColonSyntax match {
      case LabelExpression.Leaf(name) => Set(Symbol(name.name))
      case LabelExpression.Conjunctions(children) =>
        children.foldLeft(Set.empty[Symbol])( (labels, le) => labels.union(handleLabelExpression(le, maybeLoc)))
//      case LabelExpression.ColonConjunction(lhs, rhs) =>
//        handleLabelExpression(lhs, maybeLoc).union(handleLabelExpression(rhs, maybeLoc))
      case LabelExpression.Disjunctions(children)  =>
        children.foldLeft(Set.empty[Symbol])( (labels, le) => labels.union(handleLabelExpression(le, maybeLoc)))
//      case LabelExpression.ColonDisjunction(lhs, rhs) =>
//        handleLabelExpression(lhs, maybeLoc).union(handleLabelExpression(rhs, maybeLoc))
      case _ =>
        throw CypherException.Compile(
          s"We don't currently support complex label expressions! (got $le)",
          maybeLoc)
    }

}

/**
  * Like [[nameAllPatternElements]], but does not rewrite naked pattern elements in MATCH clauses.
  */
//case object nameAllPatternElementsInPatternComprehensions extends Rewriter with StepSequencer.Step with ASTRewriterFactory with LazySafeLogging {
//
//  override def getRewriter(
//    innerVariableNamer: InnerVariableNamer,
//    semanticState: SemanticState,
//    parameterTypeMapping: Map[String, CypherType],
//    cypherExceptionFactory: CypherExceptionFactory
//  ): Rewriter = namingRewriter
//
//  override def preConditions: Set[StepSequencer.Condition] = Set.empty
//
//  override def postConditions: Set[StepSequencer.Condition] = Set(
//    noUnnamedPatternElementsInPatternComprehension
//  )
//
//  override def invalidatedConditions: Set[StepSequencer.Condition] = Set(
//    ProjectionClausesHaveSemanticInfo, // It can invalidate this condition by rewriting things inside WITH/RETURN.
//    PatternExpressionsHaveSemanticInfo, // It can invalidate this condition by rewriting things inside PatternExpressions.
//  )
//
//  override def apply(that: AnyRef): AnyRef = namingRewriter.apply(that)
//
//  private val patternRewriter: Rewriter = bottomUp(Rewriter.lift {
//    case pattern: NodePattern if pattern.variable.isEmpty =>
//      val syntheticName = NodeNameGenerator.name(pattern.position.newUniquePos())
//      pattern.copy(variable = Some(Variable(syntheticName)(pattern.position)))(pattern.position)
//
//    case pattern: RelationshipPattern if pattern.variable.isEmpty =>
//      val syntheticName = RelNameGenerator.name(pattern.position.newUniquePos())
//      pattern.copy(variable = Some(Variable(syntheticName)(pattern.position)))(pattern.position)
//  }, stopper = {
//    case _: ShortestPathExpression => true
//    case _ => false
//  })
//
//  private val namingRewriter: Rewriter = bottomUp(Rewriter.lift {
//    case patternComprehension: PatternComprehension => patternRewriter(patternComprehension)
//  }, stopper = {
//    case _: Where => true
//    case _: ShortestPathExpression => true
//    case _ => false
//  })
//}
