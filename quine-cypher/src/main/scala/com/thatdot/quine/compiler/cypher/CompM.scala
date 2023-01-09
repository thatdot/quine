package com.thatdot.quine.compiler.cypher

import cats._
import cats.arrow.FunctionK
import cats.data.{EitherT, IndexedReaderWriterStateT, ReaderWriterState}
import org.opencypher.v9_0.expressions.LogicalVariable
import org.opencypher.v9_0.util

import com.thatdot.quine.graph.cypher.{CypherException, Expr, SourceText}
import com.thatdot.quine.utils.MonadErrorVia

/** Stateful compilation computation
  *
  *   - reads query-global parameter indicies (to compile constructs like `$props`)
  *   - reads query-global source text (used when creating exception positions)
  *   - tracks state: which nodes are in scope at a given point of query compilation
  *   - captures compilation exceptions
  *
  * This monad is intentionally opaque so that the underlying implementation
  * details can be changed out without affecting the API.
  *
  * @see [[QueryScopeInfo]] for details about anchor, column, and variable representation
  */
final case class CompM[A] private (
  private val eitherRws: EitherT[
    ReaderWriterState[(ParametersIndex, SourceText), Unit, QueryScopeInfo, *],
    CypherException.Compile,
    A
  ]
) {

  def map[B](f: A => B): CompM[B] = CompM.monadError.map(this)(f)

  def flatMap[B](f: A => CompM[B]): CompM[B] = CompM.monadError.flatMap(this)(f)

  /** Run the compilation task
    *
    * @param params query-global parameters
    * @param sourceText initial source string of the query (used for pretty errors)
    * @param scopeInfo initial context of stuff in scope
    * @return a compile error or the compilation output
    */
  def run(
    params: ParametersIndex,
    sourceText: SourceText,
    scopeInfo: QueryScopeInfo
  ): Either[CypherException.Compile, A] =
    eitherRws.value.runA(params -> sourceText, scopeInfo).value
}
object CompM {

  // Simple private aliases to shorten types
  private type ReaderPart = (ParametersIndex, SourceText)
  private type WriterPart = Unit
  private type StatePart = QueryScopeInfo
  private type ErrorPart = CypherException.Compile
  private type RwsPart[A] = ReaderWriterState[ReaderPart, WriterPart, StatePart, A]

  @inline
  private def liftRWS[A](action: RwsPart[A]): CompM[A] =
    CompM(EitherT.liftF[RwsPart, ErrorPart, A](action))

  implicit val monadError: MonadError[CompM, CypherException.Compile] =
    new MonadErrorVia[EitherT[RwsPart, ErrorPart, *], CompM, ErrorPart](
      EitherT.catsDataMonadErrorForEitherT[RwsPart, ErrorPart](
        IndexedReaderWriterStateT.catsDataMonadForRWST[Eval, ReaderPart, WriterPart, StatePart]
      ),
      // The `Lambda` bit makes anonymous polymorphic functions for wrapping/unwrapping `CompM`
      Lambda[FunctionK[EitherT[RwsPart[*], ErrorPart, *], CompM]](CompM.apply(_)),
      Lambda[FunctionK[CompM, EitherT[RwsPart[*], ErrorPart, *]]](_.eitherRws)
    )

  /** @return current scope information */
  @inline
  val getQueryScopeInfo: CompM[QueryScopeInfo] =
    liftRWS[QueryScopeInfo](ReaderWriterState.get)

  /** Look up a query parameter (aka. part of the query which stays constant)
    *
    * @param parameterName name of the parameter
    * @param astNode context for an error message if the variable couldn't be found
    * @return expression for reading the parameter
    */
  def getParameter(parameterName: String, astNode: util.ASTNode): CompM[Expr.Parameter] =
    for {
      paramIdxOpt <- liftRWS[Option[Int]](ReaderWriterState.ask.map(_._1.index.get(parameterName)))
      paramExpr <- paramIdxOpt match {
        case Some(idx) => CompM.pure(Expr.Parameter(idx))
        case None => CompM.raiseCompileError(s"Unknown parameter `$parameterName`", astNode)
      }
    } yield paramExpr

  /** Look up a variable
    *
    * @param variableName name of the parameter
    * @param astNode context for an error message if the variable couldn't be found
    * @return expression for reading the variable
    */
  def getVariable(variableName: Symbol, astNode: util.ASTNode): CompM[Expr.Variable] =
    for {
      variableOpt <- liftRWS(ReaderWriterState.inspect(_.getVariable(variableName)))
      variableExpr <- variableOpt match {
        case Some(v) => CompM.pure(v)
        case None => CompM.raiseCompileError(s"Unknown variable `${variableName.name}`", astNode)
      }
    } yield variableExpr

  def getVariable(variable: LogicalVariable, astNode: util.ASTNode): CompM[Expr.Variable] =
    getVariable(logicalVariable2Symbol(variable), astNode)

  /** @return original query source text */
  val getSourceText: CompM[SourceText] = liftRWS(ReaderWriterState.ask.map(_._2))

  // TODO: remove this - this is too general/uninformative (right now, it is just glue to stick
  // the non-monadic code to the monadic code)
  val getContextParametersAndSource: CompM[(QueryScopeInfo, ParametersIndex, SourceText)] =
    liftRWS(
      ReaderWriterState.apply((e: (ParametersIndex, SourceText), n: QueryScopeInfo) => ((), n, (n, e._1, e._2)))
    )

  /** Add or override anchors for node variables
    *
    * @param anchors variables for nodes and expressions that can be used to jump to those nodes
    */
  def addNewAnchors(anchors: Iterable[(Symbol, Expr)]): CompM[Unit] =
    liftRWS(ReaderWriterState.modify(_.withNewAnchors(anchors)))

  /** Clear all anchors for node variables */
  def clearAnchors: CompM[Unit] =
    liftRWS(ReaderWriterState.modify((nc: QueryScopeInfo) => nc.withoutAnchors))

  /** Append a variable to the end of the context
    *
    * @param variable new variable to add to the context
    * @return expression for reading the variable
    */
  def addColumn(variable: Symbol): CompM[Expr.Variable] =
    liftRWS[Expr.Variable](
      ReaderWriterState { (_, initialScope: QueryScopeInfo) =>
        val (newScope, varExpr) = initialScope.addColumn(variable)
        ((), newScope, varExpr)
      }
    )

  def addColumn(variable: LogicalVariable): CompM[Expr.Variable] = addColumn(logicalVariable2Symbol(variable))

  /** Check if a variable is in the current query scope
    *
    * @param variable what to look for
    * @return whether the variable is in the current columns
    */
  def hasColumn(variable: Symbol): CompM[Boolean] =
    liftRWS(ReaderWriterState.inspect((nc: QueryScopeInfo) => nc.getVariable(variable).isDefined))

  /** Remove all variables from the query scope */
  def clearColumns: CompM[Unit] =
    liftRWS(ReaderWriterState.modify((nc: QueryScopeInfo) => nc.clearColumns))

  /** @return columns in scope */
  def getColumns: CompM[Vector[Symbol]] =
    liftRWS(ReaderWriterState.inspect(_.getColumns))

  /** Run an action in a forked context (updates to nodes in context won't be propagated out)
    *
    * @param action what to do in the isolated context
    * @return a separate action which won't modify the context
    */
  def withIsolatedContext[A](action: CompM[A]): CompM[A] =
    for {
      ctx <- liftRWS(ReaderWriterState.get)
      a <- action
      _ <- liftRWS(ReaderWriterState.set(ctx))
    } yield a

  @inline
  def pure[A](a: A): CompM[A] = monadError.pure[A](a)

  @inline
  def raiseError[A](err: CypherException.Compile): CompM[A] = monadError.raiseError[A](err)

  /** Raise a Cypher compilation error
    *
    * TODO: refine this into a hierarchy of errors
    *
    * @param message description of the error
    * @param astNode on which node did the error occur?
    */
  def raiseCompileError[A](message: String, astNode: util.ASTNode): CompM[A] =
    CompM.getSourceText.flatMap[A] { implicit sourceText =>
      CompM.raiseError(CypherException.Compile(message, Some(position(astNode.position))))
    }
}
