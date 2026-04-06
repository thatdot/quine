package com.thatdot.quine.graph.cypher.quinepattern

import scala.collection.immutable.SortedMap

import cats.data.ReaderT
import cats.implicits._

import com.thatdot.common.quineid.QuineId
import com.thatdot.quine.graph.cypher.CypherException
import com.thatdot.quine.graph.cypher.CypherException.Runtime
import com.thatdot.quine.language.ast.{BindingId, CypherIdentifier, Expression, Operator, Value}
import com.thatdot.quine.model.QuineIdProvider

object QuinePatternExpressionInterpreter {

  /** Convert an identifier to the BindingId key used in QueryContext.
    * After symbol analysis, identifiers should be Right(BindingId).
    */
  private def identKey(ident: Either[CypherIdentifier, BindingId]): BindingId =
    ident match {
      case Right(bindingId) => bindingId
      case Left(cypherIdent) =>
        throw new RuntimeException(
          s"Encountered unresolved CypherIdentifier '${cypherIdent.name}' - " +
          "this indicates a bug in the symbol analysis phase",
        )
    }

  /** Evaluation environment using QuinePattern's native QueryContext with Pattern.Value bindings.
    * This avoids unnecessary conversion between Pattern.Value and Expr.Value.
    */
  case class EvalEnvironment(queryContext: QueryContext, parameters: Map[Symbol, Value])

  type ContextualEvaluationResult[A] = ReaderT[Either[CypherException, *], EvalEnvironment, A]

  def fromEnvironment[A](view: EvalEnvironment => A): ContextualEvaluationResult[A] =
    ReaderT.apply(env => Right(view(env)))
  def liftF[A](either: Either[Runtime, A]): ContextualEvaluationResult[A] = ReaderT.liftF(either)
  def error[A](message: String): ContextualEvaluationResult[A] = liftF(Left(Runtime(message)))
  def pure[A](a: A): ContextualEvaluationResult[A] = ReaderT.pure(a)

  def evalCase(
    caseBlock: Expression.CaseBlock,
  )(implicit idProvider: QuineIdProvider): ContextualEvaluationResult[Value] =
    caseBlock.cases.findM(aCase => eval(aCase.condition).map(_ == Value.True)) >>= {
      case Some(sc) => eval(sc.value)
      case None => eval(caseBlock.alternative)
    }

  def evalIsNull(isNull: Expression.IsNull)(implicit idProvider: QuineIdProvider): ContextualEvaluationResult[Value] =
    eval(isNull.of) map {
      case Value.Null => Value.True
      case _ => Value.False
    }

  def evalIdLookup(
    idLookup: Expression.IdLookup,
  )(implicit @scala.annotation.unused idProvider: QuineIdProvider): ContextualEvaluationResult[Value] =
    fromEnvironment(env => env.queryContext).map(_.get(identKey(idLookup.nodeIdentifier))) >>= {
      case Some(value) =>
        // Value is already Pattern.Value - extract node ID directly
        value match {
          case Value.NodeId(qid) => pure(Value.NodeId(qid))
          case Value.Bytes(bytes) => pure(Value.NodeId(QuineId(bytes)))
          case Value.Node(id, _, _) => pure(Value.NodeId(id))
          case Value.Null => pure(Value.Null)
          case other =>
            liftF(CypherAndQuineHelpers.getNode(other).map(n => Value.NodeId(n.id)))
        }
      case None => pure(Value.Null)
    }

  def evalSynthesizeId(
    synthesizeId: Expression.SynthesizeId,
  )(implicit idProvider: QuineIdProvider): ContextualEvaluationResult[Value] =
    synthesizeId.from.traverse(eval) map { evaledArgs =>
      val cypherIdValues = evaledArgs.map(QuinePatternHelpers.patternValueToCypherValue)
      val id = com.thatdot.quine.graph.idFrom(cypherIdValues: _*)(idProvider)
      Value.NodeId(id)
    }

  def evalIdentifier(
    identExp: Expression.Ident,
  )(implicit @scala.annotation.unused idProvider: QuineIdProvider): ContextualEvaluationResult[Value] =
    fromEnvironment(_.queryContext) map (_.get(identKey(identExp.identifier))) >>= {
      case Some(value) => pure(value)
      case None => pure(Value.Null)
    }

  /** Evaluates a given parameter expression in the current evaluation context.
    *
    * NOTE The parser is not currently correctly handling parameters, so this
    *      will trim off the leading `$` to enable the variable to be found
    *
    * @param parameter  the parameter to be evaluated
    * @param idProvider an implicit provider for handling Quine-specific IDs
    * @return the evaluation result of the parameter as a contextual value
    */
  def evalParameter(parameter: Expression.Parameter): ContextualEvaluationResult[Value] = {
    val trimName = Symbol(parameter.name.name.substring(1))
    fromEnvironment(_.parameters) >>= { parameters =>
      val containsName = parameters.contains(trimName)
      if (containsName) {
        pure(parameters(trimName))
      } else {
        error[Value](s"Parameter $trimName not found in $parameters")
      }
    }
  }

  def evalFunctionApplication(
    applyExp: Expression.Apply,
  )(implicit idProvider: QuineIdProvider): ContextualEvaluationResult[Value] =
    applyExp.args.traverse(arg => eval(arg)) >>= { evaledArgs =>
      applyExp.name.name match {
        // Currently unsure how I want to handle functions with external dependencies
        // so I'm handling `idFrom` as a special case here
        case "idFrom" =>
          val cypherIdValues = evaledArgs.map(QuinePatternHelpers.patternValueToCypherValue)
          val id = com.thatdot.quine.graph.idFrom(cypherIdValues: _*)(idProvider)
          pure(Value.NodeId(id))
        // Handling `strId` as a special case due to its reliance on the idProvider
        case "strId" =>
          evaledArgs match {
            case List(Value.NodeId(id)) => pure(Value.Text(idProvider.qidToPrettyString(id)))
            case List(Value.Node(id, _, _)) => pure(Value.Text(idProvider.qidToPrettyString(id)))
            case _ => error[Value]("Unable to interpret the arguments to `strId`")
          }
        case otherFunctionName =>
          QuinePatternFunction.findBuiltIn(otherFunctionName) match {
            case Some(func) => liftF(func(evaledArgs))
            case None => error[Value](s"No function named $otherFunctionName found in the QuinePattern library")
          }
      }
    }

  def evalUnaryExp(unaryExp: Expression.UnaryOp)(implicit
    idProvider: QuineIdProvider,
  ): ContextualEvaluationResult[Value] =
    //TODO Probably would be nice to convert these to functions
    unaryExp.op match {
      case Operator.Minus =>
        eval(unaryExp.exp) >>= {
          case Value.Integer(n) => pure(Value.Integer(-n))
          case Value.Real(d) => pure(Value.Real(-d))
          case other => error(s"Unexpected expression: $other")
        }
      case Operator.Not =>
        eval(unaryExp.exp) >>= {
          case Value.True => pure(Value.False)
          case Value.False => pure(Value.True)
          case Value.Null => pure(Value.Null)
          case _ => error(s"Unexpected expression: ${unaryExp.exp}")
        }
      case otherOperator => error(s"Unexpected operator: $otherOperator")
    }

  def eval(exp: Expression)(implicit idProvider: QuineIdProvider): ContextualEvaluationResult[Value] =
    exp match {
      case caseBlock: Expression.CaseBlock => evalCase(caseBlock)
      case isNull: Expression.IsNull => evalIsNull(isNull)
      case idLookup: Expression.IdLookup => evalIdLookup(idLookup)
      case synthesizeId: Expression.SynthesizeId => evalSynthesizeId(synthesizeId)
      case Expression.AtomicLiteral(_, value, _) => pure(value)
      case listLiteral: Expression.ListLiteral => listLiteral.value.traverse(eval) map Value.List
      case mapLiteral: Expression.MapLiteral =>
        mapLiteral.value.toList
          .traverse(p => eval(p._2).map(v => p._1 -> v))
          .map(xs => Value.Map(SortedMap.from(xs)))
      case identifier: Expression.Ident => evalIdentifier(identifier)
      case parameterExp: Expression.Parameter => evalParameter(parameterExp)
      case applyExp: Expression.Apply => evalFunctionApplication(applyExp)
      case unaryExp: Expression.UnaryOp => evalUnaryExp(unaryExp)
      case binaryExp: Expression.BinOp =>
        for {
          leftArg <- eval(binaryExp.lhs)
          rightArg <- eval(binaryExp.rhs)
          args = List(leftArg, rightArg)
          result <- liftF(binaryExp.op match {
            case Operator.Plus => AddFunction(args)
            case Operator.Minus => SubtractFunction(args)
            case Operator.Asterisk => MultiplyFunction(args)
            case Operator.Slash => DivideFunction(args)
            case Operator.Percent => ModuloFunction(args)
            case Operator.Equals => CompareEqualityFunction(args)
            case Operator.LessThan => CompareLessThanFunction(args)
            case Operator.LessThanEqual => CompareLessThanEqualToFunction(args)
            case Operator.GreaterThanEqual => CompareGreaterThanEqualToFunction(args)
            case Operator.GreaterThan => CompareGreaterThanFunction(args)
            case Operator.And => LogicalAndFunction(args)
            case Operator.Or => LogicalOrFunction(args)
            case Operator.NotEquals => NotEquals(args)
            case otherOperator => Left(Runtime(s"Unexpected operator: $otherOperator"))
          })
        } yield result
      case Expression.FieldAccess(_, of, fieldName, _) =>
        eval(of) >>= {
          case Value.Map(values) =>
            pure(values.get(fieldName) match {
              case Some(value) => value
              case None => Value.Null
            })
          case Value.Node(_, _, _) | Value.NodeId(_) =>
            // After materialization, all FieldAccess on graph-element-typed bindings
            // should have been rewritten to references to synthetic temporaries.
            // Reaching this path means the type system or materialization missed a case.
            error(
              s"FieldAccess '${fieldName.name}' on graph element value — " +
              "this indicates the materialization phase failed to rewrite a graph-element field access",
            )
          case Value.Null =>
            // Null propagation: field access on null produces null
            pure(Value.Null)
          case thing => error(s"Don't know how to do field access on $thing")
        }
      case Expression.IndexIntoArray(_, of, indexExp, _) =>
        eval(of) >>= {
          case Value.List(values) =>
            eval(indexExp) >>= {
              case Value.Integer(indexValue) =>
                pure(CypherAndQuineHelpers.maybeGetByIndex(values, indexValue.toInt).getOrElse(Value.Null))
              case other => error(s"$other is not a valid index expression")
            }
          case Value.Null => pure(Value.Null)
          case other => error(s"Don't know how to index into $other")
        }
    }
}
