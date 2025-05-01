package com.thatdot.quine.graph.cypher.quinepattern

import scala.collection.immutable.SortedMap

import com.thatdot.language.ast.{Expression, Operator, Value}
import com.thatdot.quine.graph.cypher
import com.thatdot.quine.graph.cypher.QueryContext
import com.thatdot.quine.model.QuineIdProvider

object QuinePatternExpressionInterpreter {

  def eval(
    exp: Expression,
    idProvider: QuineIdProvider,
    qc: QueryContext,
    parameters: Map[Symbol, cypher.Value],
  ): Value =
    exp match {
      case Expression.CaseBlock(_, cases, alternative, _) =>
        cases.find(c => eval(c.condition, idProvider, qc, parameters) == Value.True) match {
          case Some(sc) => eval(sc.value, idProvider, qc, parameters)
          case None => eval(alternative, idProvider, qc, parameters)
        }
      case Expression.IsNull(_, exp, _) =>
        eval(exp, idProvider, qc, parameters) match {
          case Value.Null => Value.True
          case _ => Value.False
        }
      case Expression.IdLookup(_, nodeIdentifier, _) =>
        qc.get(nodeIdentifier.name) match {
          case Some(value) =>
            Value.NodeId(
              CypherAndQuineHelpers.getNode(CypherAndQuineHelpers.cypherValueToPatternValue(idProvider)(value)).id,
            )
          case None => Value.Null
        }
      case Expression.SynthesizeId(_, args, _) =>
        val evaledArgs = args.map(arg => eval(arg, idProvider, qc, parameters))
        val cypherIdValues = evaledArgs.map(QuinePatternHelpers.patternValueToCypherValue)
        val id = com.thatdot.quine.graph.idFrom(cypherIdValues: _*)(idProvider)
        Value.NodeId(id)
      case Expression.AtomicLiteral(_, value, _) => value
      case Expression.ListLiteral(_, value, _) =>
        Value.List(value.map(eval(_, idProvider, qc, parameters)))
      case Expression.MapLiteral(_, value, _) =>
        Value.Map(SortedMap(value.map(p => p._1.name -> eval(p._2, idProvider, qc, parameters)).toList: _*))
      case Expression.Ident(_, identifier, _) =>
        qc.get(identifier.name) match {
          case Some(value) => CypherAndQuineHelpers.cypherValueToPatternValue(idProvider)(value)
          case None => Value.Null
        }
      case Expression.Parameter(_, name, _) =>
        val trimName = Symbol(name.name.substring(1))
        if (!parameters.contains(trimName))
          throw new QuinePatternUnimplementedException(s"Parameter $trimName not found in $parameters")
        CypherAndQuineHelpers.cypherValueToPatternValue(idProvider)(parameters(trimName))
      case Expression.Apply(_, name, args, _) =>
        val evaledArgs = args.map(arg => eval(arg, idProvider, qc, parameters))
        name.name match {
          // Currently unsure how I want to handle functions with external dependencies
          // so I'm handling `idFrom` as a special case here
          case "idFrom" =>
            val cypherIdValues = evaledArgs.map(QuinePatternHelpers.patternValueToCypherValue)
            val id = com.thatdot.quine.graph.idFrom(cypherIdValues: _*)(idProvider)
            Value.NodeId(id)
          // Handling `strId` as a special case due to it's reliance on the idProvider
          case "strId" =>
            evaledArgs match {
              case List(Value.NodeId(id)) => Value.Text(idProvider.qidToPrettyString(id))
              case List(Value.Node(id, _, _)) => Value.Text(idProvider.qidToPrettyString(id))
              case _ => throw new QuinePatternUnimplementedException(s"Unexpected arguments to strId: $evaledArgs")
            }
          case otherFunctionName =>
            QuinePatternFunction.findBuiltIn(otherFunctionName) match {
              case Some(func) => func(evaledArgs)
              case None => throw new QuinePatternUnimplementedException(s"Unexpected function call $name")
            }
        }
      case Expression.UnaryOp(_, op, exp, _) =>
        //TODO Probably would be nice to convert these to functions
        op match {
          case Operator.Minus =>
            eval(exp, idProvider, qc, parameters) match {
              case Value.Integer(n) => Value.Integer(-n)
              case Value.Real(d) => Value.Real(-d)
              case other => throw new QuinePatternUnimplementedException(s"Unexpected expression: $other")
            }
          case Operator.Not =>
            eval(exp, idProvider, qc, parameters) match {
              case Value.True => Value.False
              case Value.False => Value.True
              case Value.Null => Value.Null
              case _ => throw new QuinePatternUnimplementedException(s"Unexpected expression: $exp")
            }
          case otherOperator => throw new QuinePatternUnimplementedException(s"Unexpected operator: $otherOperator")
        }
      case Expression.BinOp(_, op, lhs, rhs, _) =>
        val evaledArgs = List(lhs, rhs).map(arg => eval(arg, idProvider, qc, parameters))
        op match {
          case Operator.Plus => AddFunction(evaledArgs)
          case Operator.Asterisk => MultiplyFunction(evaledArgs)
          case Operator.Slash => DivideFunction(evaledArgs)
          case Operator.Equals => CompareEqualityFunction(evaledArgs)
          case Operator.LessThan => CompareLessThanFunction(evaledArgs)
          case Operator.GreaterThanEqual => CompareGreaterThanEqualToFunction(evaledArgs)
          case Operator.And => LogicalAndFunction(evaledArgs)
          case Operator.Or => LogicalOrFunction(evaledArgs)
          case Operator.NotEquals => NotEquals(evaledArgs)
          case Operator.GreaterThan => CompareGreaterThanFunction(evaledArgs)
          case otherOperator => throw new QuinePatternUnimplementedException(s"Unexpected operator: $otherOperator")
        }
      case Expression.FieldAccess(_, of, fieldName, _) =>
        val thing = eval(of, idProvider, qc, parameters)
        thing match {
          case Value.Map(values) =>
            values.get(fieldName.name) match {
              case Some(value) => value
              case None => Value.Null
            }
          case Value.Node(_, _, props) =>
            props.values.get(fieldName.name) match {
              case Some(value) => value
              case None => Value.Null
            }
          case Value.Null => Value.Null
          case _ => throw new QuinePatternUnimplementedException(s"Don't know how to do field access on $thing")
        }
      case Expression.IndexIntoArray(_, of, indexExp, _) =>
        eval(of, idProvider, qc, parameters) match {
          case Value.List(values) =>
            val index = eval(indexExp, idProvider, qc, parameters).asInstanceOf[Value.Integer]
            val intIndex = index.n.toInt
            CypherAndQuineHelpers.maybeGetByIndex(values, intIndex).getOrElse(Value.Null)
          case Value.Null => Value.Null
          case other => throw new QuinePatternUnimplementedException(s"Don't know how to index into $other")
        }
    }
}
