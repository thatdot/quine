package com.thatdot.quine.language.prettyprint

import com.thatdot.quine.language.ast._
import com.thatdot.quine.language.types.Type

trait ASTInstances extends BaseInstances with TypeInstances {
  import Doc._

  implicit val cypherIdentifierPrettyPrint: PrettyPrint[CypherIdentifier] =
    PrettyPrint.instance(id => text(id.name.name))

  implicit val quineIdentifierPrettyPrint: PrettyPrint[QuineIdentifier] =
    PrettyPrint.instance(id => text(s"#${id.name}"))

  implicit def identifierEitherPrettyPrint: PrettyPrint[Either[CypherIdentifier, QuineIdentifier]] =
    PrettyPrint.instance {
      case Left(cid) => cypherIdentifierPrettyPrint.doc(cid)
      case Right(qid) => quineIdentifierPrettyPrint.doc(qid)
    }

  implicit val sourcePrettyPrint: PrettyPrint[Source] =
    PrettyPrint.instance {
      case Source.TextSource(start, end) => text(s"@[$start-$end]")
      case Source.NoSource => text("@[?]")
    }

  implicit val operatorPrettyPrint: PrettyPrint[Operator] =
    PrettyPrint.instance {
      case Operator.Plus => text("+")
      case Operator.Minus => text("-")
      case Operator.Asterisk => text("*")
      case Operator.Slash => text("/")
      case Operator.Percent => text("%")
      case Operator.Carat => text("^")
      case Operator.Equals => text("=")
      case Operator.NotEquals => text("<>")
      case Operator.LessThan => text("<")
      case Operator.LessThanEqual => text("<=")
      case Operator.GreaterThan => text(">")
      case Operator.GreaterThanEqual => text(">=")
      case Operator.And => text("AND")
      case Operator.Or => text("OR")
      case Operator.Xor => text("XOR")
      case Operator.Not => text("NOT")
    }

  implicit val directionPrettyPrint: PrettyPrint[Direction] =
    PrettyPrint.instance {
      case Direction.Left => text("Left")
      case Direction.Right => text("Right")
    }

  implicit lazy val valuePrettyPrint: PrettyPrint[Value] =
    PrettyPrint.instance {
      case Value.Null => text("null")
      case Value.True => text("true")
      case Value.False => text("false")
      case Value.Integer(n) => text(n.toString)
      case Value.Real(d) => text(d.toString)
      case Value.Text(s) => text(s""""$s"""")
      case Value.Bytes(arr) => text(s"Bytes(${arr.length} bytes)")
      case Value.Duration(d) => text(s"Duration($d)")
      case Value.Date(d) => text(s"Date($d)")
      case Value.DateTime(dt) => text(s"DateTime($dt)")
      case Value.DateTimeLocal(dt) => text(s"DateTimeLocal($dt)")
      case Value.List(values) =>
        if (values.isEmpty) text("[]")
        else {
          val items = values.map(valuePrettyPrint.doc)
          concat(text("["), intercalate(text(", "), items), text("]"))
        }
      case Value.Map(values) =>
        if (values.isEmpty) text("{}")
        else {
          val items = values.toList.map { case (k, v) =>
            concat(text(k.name), text(": "), valuePrettyPrint.doc(v))
          }
          concat(text("{"), intercalate(text(", "), items), text("}"))
        }
      case Value.NodeId(id) => text(s"NodeId($id)")
      case Value.Node(id, labels, props) =>
        concat(
          text("Node("),
          text(id.toString),
          text(", labels="),
          text(labels.map(_.name).mkString("{", ", ", "}")),
          text(", props="),
          valuePrettyPrint.doc(props),
          text(")"),
        )
      case Value.Relationship(start, edgeType, properties, end) =>
        val propsDoc =
          if (properties.isEmpty) text("{}")
          else {
            val items = properties.toList.map { case (k, v) =>
              concat(text(k.name), text(": "), valuePrettyPrint.doc(v))
            }
            concat(text("{"), intercalate(text(", "), items), text("}"))
          }
        concat(
          text("Relationship("),
          text(start.toString),
          text("-[:"),
          text(edgeType.name),
          text("]->"),
          text(end.toString),
          text(", props="),
          propsDoc,
          text(")"),
        )
    }

  private def typeAnnotation(ty: Option[Type]): Doc = ty match {
    case Some(t) => concat(text(" : "), typePrettyPrint.doc(t))
    case None => empty
  }

  implicit lazy val specificCasePrettyPrint: PrettyPrint[SpecificCase] =
    PrettyPrint.instance { sc =>
      concat(
        text("WHEN "),
        expressionPrettyPrint.doc(sc.condition),
        text(" THEN "),
        expressionPrettyPrint.doc(sc.value),
      )
    }

  implicit lazy val expressionPrettyPrint: PrettyPrint[Expression] =
    PrettyPrint.instance { expr =>
      val sourceDoc = sourcePrettyPrint.doc(expr.source)
      val tyDoc = typeAnnotation(expr.ty)

      val bodyDoc: Doc = expr match {
        case Expression.IdLookup(_, nodeId, _) =>
          concat(text("idFrom("), identifierEitherPrettyPrint.doc(nodeId), text(")"))

        case Expression.SynthesizeId(_, from, _) =>
          val args = from.map(expressionPrettyPrint.doc)
          concat(text("locIdFrom("), intercalate(text(", "), args), text(")"))

        case Expression.AtomicLiteral(_, value, _) =>
          valuePrettyPrint.doc(value)

        case Expression.ListLiteral(_, values, _) =>
          if (values.isEmpty) text("[]")
          else {
            val items = values.map(expressionPrettyPrint.doc)
            concat(
              text("["),
              nest(1, concat(line, intercalate(concat(text(","), line), items))),
              line,
              text("]"),
            )
          }

        case Expression.MapLiteral(_, values, _) =>
          if (values.isEmpty) text("{}")
          else {
            val items = values.toList.map { case (k, v) =>
              concat(symbolPrettyPrint.doc(k), text(": "), expressionPrettyPrint.doc(v))
            }
            concat(
              text("{"),
              nest(1, concat(line, intercalate(concat(text(","), line), items))),
              line,
              text("}"),
            )
          }

        case Expression.Ident(_, identifier, _) =>
          concat(text("Ident("), identifierEitherPrettyPrint.doc(identifier), text(")"))

        case Expression.Parameter(_, name, _) =>
          concat(text("$"), text(name.name))

        case Expression.Apply(_, name, args, _) =>
          val argDocs = args.map(expressionPrettyPrint.doc)
          concat(text(name.name), text("("), intercalate(text(", "), argDocs), text(")"))

        case Expression.UnaryOp(_, op, exp, _) =>
          concat(
            text("UnaryOp("),
            nest(
              1,
              concat(
                line,
                text("op = "),
                operatorPrettyPrint.doc(op),
                text(","),
                line,
                text("exp = "),
                expressionPrettyPrint.doc(exp),
              ),
            ),
            line,
            text(")"),
          )

        case Expression.BinOp(_, op, lhs, rhs, _) =>
          concat(
            text("BinOp("),
            nest(
              1,
              concat(
                line,
                text("op = "),
                operatorPrettyPrint.doc(op),
                text(","),
                line,
                text("lhs = "),
                expressionPrettyPrint.doc(lhs),
                text(","),
                line,
                text("rhs = "),
                expressionPrettyPrint.doc(rhs),
              ),
            ),
            line,
            text(")"),
          )

        case Expression.FieldAccess(_, of, fieldName, _) =>
          concat(
            text("FieldAccess("),
            nest(
              1,
              concat(
                line,
                text("of = "),
                expressionPrettyPrint.doc(of),
                text(","),
                line,
                text("field = "),
                symbolPrettyPrint.doc(fieldName),
              ),
            ),
            line,
            text(")"),
          )

        case Expression.IndexIntoArray(_, of, index, _) =>
          concat(
            text("IndexIntoArray("),
            nest(
              1,
              concat(
                line,
                text("of = "),
                expressionPrettyPrint.doc(of),
                text(","),
                line,
                text("index = "),
                expressionPrettyPrint.doc(index),
              ),
            ),
            line,
            text(")"),
          )

        case Expression.IsNull(_, of, _) =>
          concat(text("IsNull("), expressionPrettyPrint.doc(of), text(")"))

        case Expression.CaseBlock(_, cases, alternative, _) =>
          val caseDocs = cases.map(specificCasePrettyPrint.doc)
          concat(
            text("CASE"),
            nest(1, concat(line, intercalate(line, caseDocs))),
            line,
            text("ELSE "),
            expressionPrettyPrint.doc(alternative),
            line,
            text("END"),
          )
      }

      concat(bodyDoc, text(" "), sourceDoc, tyDoc)
    }

  implicit lazy val localEffectPrettyPrint: PrettyPrint[LocalEffect] =
    PrettyPrint.instance {
      case LocalEffect.SetProperty(field, to) =>
        concat(
          text("SetProperty("),
          nest(
            1,
            concat(
              line,
              text("field = "),
              expressionPrettyPrint.doc(field),
              text(","),
              line,
              text("to = "),
              expressionPrettyPrint.doc(to),
            ),
          ),
          line,
          text(")"),
        )

      case LocalEffect.SetLabels(on, labels) =>
        concat(
          text("SetLabels("),
          nest(
            1,
            concat(
              line,
              text("on = "),
              identifierEitherPrettyPrint.doc(on),
              text(","),
              line,
              text("labels = "),
              setPrettyPrint[Symbol].doc(labels),
            ),
          ),
          line,
          text(")"),
        )

      case LocalEffect.CreateNode(identifier, labels, maybeProperties) =>
        concat(
          text("CreateNode("),
          nest(
            1,
            concat(
              line,
              text("identifier = "),
              identifierEitherPrettyPrint.doc(identifier),
              text(","),
              line,
              text("labels = "),
              setPrettyPrint[Symbol].doc(labels),
              text(","),
              line,
              text("maybeProperties = "),
              maybeProperties match {
                case Some(props) => expressionPrettyPrint.doc(props)
                case None => text("None")
              },
            ),
          ),
          line,
          text(")"),
        )

      case LocalEffect.CreateEdge(labels, direction, left, right, binding) =>
        concat(
          text("CreateEdge("),
          nest(
            1,
            concat(
              line,
              text("labels = "),
              setPrettyPrint[Symbol].doc(labels),
              text(","),
              line,
              text("direction = "),
              directionPrettyPrint.doc(direction),
              text(","),
              line,
              text("left = "),
              identifierEitherPrettyPrint.doc(left),
              text(","),
              line,
              text("right = "),
              identifierEitherPrettyPrint.doc(right),
              text(","),
              line,
              text("binding = "),
              identifierEitherPrettyPrint.doc(binding),
            ),
          ),
          line,
          text(")"),
        )
    }

  implicit lazy val projectionLangPrettyPrint: PrettyPrint[Projection] =
    PrettyPrint.instance { proj =>
      concat(
        expressionPrettyPrint.doc(proj.expression),
        text(" AS "),
        identifierEitherPrettyPrint.doc(proj.as),
      )
    }

  implicit lazy val operationPrettyPrint: PrettyPrint[Operation] =
    PrettyPrint.instance {
      case Operation.Call =>
        text("CALL")

      case Operation.Unwind(expression, as) =>
        concat(
          text("UNWIND "),
          expressionPrettyPrint.doc(expression),
          text(" AS "),
          identifierEitherPrettyPrint.doc(as),
        )

      case Operation.Effect(cypherEffect) =>
        concat(text("Effect("), text(cypherEffect.toString), text(")"))
    }

}

object ASTInstances extends ASTInstances
