package com.thatdot.quine.language

import com.thatdot.quine.cypher.ast.Query
import com.thatdot.quine.cypher.phases.SymbolAnalysisModule.{SymbolTable, TableMonoid}
import com.thatdot.quine.cypher.phases.{LexerPhase, LexerState, ParserPhase, SymbolAnalysisPhase}
import com.thatdot.quine.language.diagnostic.Diagnostic
import com.thatdot.quine.language.phases.{TypeCheckingPhase, UpgradeModule}
import com.thatdot.quine.language.types.Type

import UpgradeModule._

case class ParseResult(
  ast: Option[Query],
  diagnostics: List[Diagnostic],
) {
  def hasErrors: Boolean = diagnostics.exists(!_.isInstanceOf[Diagnostic.SymbolAnalysisWarning])
  def isSuccess: Boolean = ast.isDefined && !hasErrors

  private def instances = com.thatdot.quine.language.prettyprint.ResultInstances

  def pretty: String = instances.parseResultPrettyPrint.pretty(this)
  def prettyAst: String = ast.map(instances.queryPrettyPrint.pretty(_)).getOrElse("None")
  def prettyDiagnostics: String =
    diagnostics.map(instances.diagnosticPrettyPrint.pretty(_)).mkString("\n")
}

case class AnalyzeResult(
  ast: Option[Query],
  symbolTable: SymbolTable,
  diagnostics: List[Diagnostic],
) {
  def hasErrors: Boolean = diagnostics.exists(!_.isInstanceOf[Diagnostic.SymbolAnalysisWarning])
  def isSuccess: Boolean = ast.isDefined && !hasErrors

  private def instances = com.thatdot.quine.language.prettyprint.ResultInstances

  def pretty: String = instances.analyzeResultPrettyPrint.pretty(this)
  def prettyAst: String = ast.map(instances.queryPrettyPrint.pretty(_)).getOrElse("None")
  def prettySymbolTable: String = instances.symbolTablePrettyPrint.pretty(symbolTable)
  def prettyDiagnostics: String =
    diagnostics.map(instances.diagnosticPrettyPrint.pretty(_)).mkString("\n")
}

case class TypeCheckResult(
  ast: Option[Query],
  symbolTable: SymbolTable,
  typeEnv: Map[Symbol, Type],
  diagnostics: List[Diagnostic],
) {
  def hasErrors: Boolean = diagnostics.exists(!_.isInstanceOf[Diagnostic.SymbolAnalysisWarning])
  def isSuccess: Boolean = ast.isDefined && !hasErrors

  private def instances = com.thatdot.quine.language.prettyprint.ResultInstances

  def pretty: String = instances.typeCheckResultPrettyPrint.pretty(this)
  def prettyAst: String = ast.map(instances.queryPrettyPrint.pretty(_)).getOrElse("None")
  def prettySymbolTable: String = instances.symbolTablePrettyPrint.pretty(symbolTable)
  def prettyTypeEnv: String =
    instances.mapPrettyPrint(instances.symbolPrettyPrint, instances.typePrettyPrint).pretty(typeEnv)
  def prettyDiagnostics: String =
    diagnostics.map(instances.diagnosticPrettyPrint.pretty(_)).mkString("\n")
}

object Cypher {

  type CompileResult = TypeCheckResult

  private val parsePipeline = LexerPhase andThen ParserPhase
  private val analyzePipeline = parsePipeline andThen SymbolAnalysisPhase
  private val typeCheckPipeline = analyzePipeline andThen TypeCheckingPhase()

  def parse(query: String): ParseResult = {
    val (state, result) = parsePipeline.process(query).value.run(LexerState(Nil)).value
    ParseResult(
      ast = result,
      diagnostics = state.diagnostics,
    )
  }

  def analyze(query: String): AnalyzeResult = {
    val (state, result) = analyzePipeline.process(query).value.run(LexerState(Nil)).value
    AnalyzeResult(
      ast = result,
      symbolTable = state.symbolTable,
      diagnostics = state.diagnostics,
    )
  }

  def typeCheck(query: String): TypeCheckResult = {
    val (state, result) = typeCheckPipeline.process(query).value.run(LexerState(Nil)).value
    TypeCheckResult(
      ast = result,
      symbolTable = state.symbolTable,
      typeEnv = state.typeEnv,
      diagnostics = state.diagnostics,
    )
  }

  def compile(query: String): CompileResult = typeCheck(query)
}
