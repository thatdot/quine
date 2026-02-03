package com.thatdot.quine.language.semantic

import scala.jdk.CollectionConverters._

import org.antlr.v4.runtime.Token

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

  def fromInt(n: Int): SemanticType = n match {
    case 0 => MatchKeyword
    case 1 => ReturnKeyword
    case 2 => AsKeyword
    case 3 => WhereKeyword
    case 4 => CreateKeyword
    case 5 => AndKeyword
    case 6 => PatternVariable
    case 7 => AssignmentOperator
    case 8 => AdditionOperator
    case 9 => NodeLabel
    case 10 => NodeVariable
    case 11 => Variable
    case 12 => Edge
    case 13 => FunctionApplication
    case 14 => Parameter
    case 15 => StringLiteral
    case 16 => NullLiteral
    case 17 => BooleanLiteral
    case 18 => IntLiteral
    case 19 => DoubleLiteral
    case 20 => Property
    case 21 => EdgeLabel
  }

  val semanticTypes: List[SemanticType] =
    (0 to 21).toList.map(fromInt)

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
}
