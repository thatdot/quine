package com.thatdot.quine.language.prettyprint

import com.thatdot.quine.language.types.{Constraint, Type}

trait TypeInstances extends BaseInstances {
  import Doc._

  implicit val constraintPrettyPrint: PrettyPrint[Constraint] =
    PrettyPrint.instance {
      case Constraint.None => text("_")
      case Constraint.Numeric => text("Numeric")
      case Constraint.Semigroup => text("Semigroup")
    }

  implicit lazy val typePrettyPrint: PrettyPrint[Type] =
    PrettyPrint.instance {
      case Type.Any => text("Any")
      case Type.Null => text("Null")
      case Type.Error => text("Error")

      case Type.Effectful(valueType) =>
        concat(text("Effectful["), typePrettyPrint.doc(valueType), text("]"))

      case Type.TypeConstructor(id, args) =>
        val argDocs = args.toList.map(typePrettyPrint.doc)
        concat(
          text(id.name),
          text("["),
          intercalate(text(", "), argDocs),
          text("]"),
        )

      case Type.TypeVariable(id, constraint) =>
        constraint match {
          case Constraint.None => text(s"?${id.name}")
          case _ => concat(text(s"?${id.name}"), text(": "), constraintPrettyPrint.doc(constraint))
        }

      case Type.PrimitiveType.Integer => text("Integer")
      case Type.PrimitiveType.Real => text("Real")
      case Type.PrimitiveType.Boolean => text("Boolean")
      case Type.PrimitiveType.String => text("String")
      case Type.PrimitiveType.NodeType => text("Node")
    }
}

object TypeInstances extends TypeInstances
