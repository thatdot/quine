package com.thatdot.quine.language.semantic

import scala.jdk.CollectionConverters._

import org.antlr.v4.runtime.{ParserRuleContext, Token}

sealed trait SemanticType

object SemanticType {
  case object MatchKeyword extends SemanticType
  case object ReturnKeyword extends SemanticType
  case object AsKeyword extends SemanticType
  case object WhereKeyword extends SemanticType
  case object CreateKeyword extends SemanticType
  case object AndKeyword extends SemanticType
  case object PatternVariable extends SemanticType
  case object AssignmentOperator extends SemanticType
  case object AdditionOperator extends SemanticType
  case object NodeLabel extends SemanticType
  case object EdgeLabel extends SemanticType
  case object NodeVariable extends SemanticType
  case object Variable extends SemanticType
  case object Edge extends SemanticType
  case object FunctionApplication extends SemanticType
  case object Parameter extends SemanticType
  case object StringLiteral extends SemanticType
  case object NullLiteral extends SemanticType
  case object BooleanLiteral extends SemanticType
  case object IntLiteral extends SemanticType
  case object DoubleLiteral extends SemanticType
  case object Property extends SemanticType

  def toInt(semanticType: SemanticType): Int = semanticType match {
    case MatchKeyword => 0
    case ReturnKeyword => 1
    case AsKeyword => 2
    case WhereKeyword => 3
    case CreateKeyword => 4
    case AndKeyword => 5
    case PatternVariable => 6
    case AssignmentOperator => 7
    case AdditionOperator => 8
    case NodeLabel => 9
    case NodeVariable => 10
    case Variable => 11
    case Edge => 12
    case FunctionApplication => 13
    case Parameter => 14
    case StringLiteral => 15
    case NullLiteral => 16
    case BooleanLiteral => 17
    case IntLiteral => 18
    case DoubleLiteral => 19
    case Property => 20
    case EdgeLabel => 21
  }

  /** Decode the LSP wire encoding back into a [[SemanticType]]. Total: an out-of-range index yields [[None]]. */
  def fromInt(n: Int): Option[SemanticType] = n match {
    case 0 => Some(MatchKeyword)
    case 1 => Some(ReturnKeyword)
    case 2 => Some(AsKeyword)
    case 3 => Some(WhereKeyword)
    case 4 => Some(CreateKeyword)
    case 5 => Some(AndKeyword)
    case 6 => Some(PatternVariable)
    case 7 => Some(AssignmentOperator)
    case 8 => Some(AdditionOperator)
    case 9 => Some(NodeLabel)
    case 10 => Some(NodeVariable)
    case 11 => Some(Variable)
    case 12 => Some(Edge)
    case 13 => Some(FunctionApplication)
    case 14 => Some(Parameter)
    case 15 => Some(StringLiteral)
    case 16 => Some(NullLiteral)
    case 17 => Some(BooleanLiteral)
    case 18 => Some(IntLiteral)
    case 19 => Some(DoubleLiteral)
    case 20 => Some(Property)
    case 21 => Some(EdgeLabel)
    case _ => None
  }

  val semanticTypes: List[SemanticType] =
    (0 to 21).toList.flatMap(fromInt)

  val semanticTypesJava: java.util.List[String] =
    semanticTypes.map(_.toString).asJava
}

case class SemanticToken(line: Int, charOnLine: Int, length: Int, semanticType: SemanticType, modifiers: Int)

object SemanticToken {
  def fromToken(token: Token, semanticType: SemanticType): SemanticToken =
    SemanticToken(
      line = token.getLine,
      charOnLine = token.getCharPositionInLine,
      length = (token.getStopIndex + 1) - token.getStartIndex,
      semanticType = semanticType,
      modifiers = 0,
    )

  /** Builds a token spanning a whole parse-tree rule context, which may cover several lexer
    * tokens (a namespaced procedure name, a backtick-escaped property key, ...). The span is
    * measured in the character stream from the context's first token to its last.
    */
  def fromContext(ctx: ParserRuleContext, semanticType: SemanticType): SemanticToken =
    SemanticToken(
      line = ctx.getStart.getLine,
      charOnLine = ctx.getStart.getCharPositionInLine,
      length = (ctx.getStop.getStopIndex + 1) - ctx.getStart.getStartIndex,
      semanticType = semanticType,
      modifiers = 0,
    )
}
