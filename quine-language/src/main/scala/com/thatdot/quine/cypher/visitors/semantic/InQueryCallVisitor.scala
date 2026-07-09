package com.thatdot.quine.cypher.visitors.semantic

import scala.jdk.CollectionConverters._

import com.thatdot.quine.cypher.parsing.{CypherBaseVisitor, CypherParser}
import com.thatdot.quine.cypher.utils.Helpers.maybeMatch
import com.thatdot.quine.language.semantic.{SemanticToken, SemanticType}

object InQueryCallVisitor extends CypherBaseVisitor[List[SemanticToken]] {

  /** Emits the tokens of an in-query `CALL procedure(args) YIELD items` or `CALL { subquery }`.
    * The procedure name (including any namespace) is a FunctionApplication; argument
    * expressions and yielded bindings contribute their own tokens, with procedure result
    * fields and yield variables emitted as Variables. The CALL and YIELD keywords have no
    * semantic token legend entry, so they contribute no tokens.
    */
  override def visitOC_InQueryCall(ctx: CypherParser.OC_InQueryCallContext): List[SemanticToken] = {
    val invocationTokens =
      Option(ctx.oC_ExplicitProcedureInvocation()).fold(List.empty[SemanticToken]) { invocation =>
        val nameToken = SemanticToken.fromContext(invocation.oC_ProcedureName(), SemanticType.FunctionApplication)
        val argumentTokens =
          invocation.oC_Expression().asScala.toList.flatMap(argument => argument.accept(ExpressionVisitor))

        nameToken :: argumentTokens
      }

    val subqueryTokens = Option(ctx.oC_Subquery()).fold(List.empty[SemanticToken]) { subquery =>
      val importedVariableTokens =
        subquery.oC_Variable().asScala.toList.map(variable => variable.accept(VariableVisitor))

      importedVariableTokens ::: subquery.oC_RegularQuery().accept(RegularQueryVisitor)
    }

    val yieldTokens = Option(ctx.oC_YieldItems()).fold(List.empty[SemanticToken]) { yieldItems =>
      val itemTokens = yieldItems.oC_YieldItem().asScala.toList.flatMap { item =>
        val fieldTokens = Option(item.oC_ProcedureResultField())
          .map(field => SemanticToken.fromContext(field, SemanticType.Variable))
          .toList
        val asTokens =
          Option(item.AS()).map(as => SemanticToken.fromToken(as.getSymbol, SemanticType.AsKeyword)).toList
        val variableToken = item.oC_Variable().accept(VariableVisitor)

        fieldTokens ::: asTokens ::: variableToken :: Nil
      }
      val whereTokens = maybeMatch(yieldItems.oC_Where(), WhereVisitor).getOrElse(List.empty[SemanticToken])

      itemTokens ::: whereTokens
    }

    invocationTokens ::: subqueryTokens ::: yieldTokens
  }
}
