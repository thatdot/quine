package com.thatdot.quine.bolt

import scala.util.hashing.MurmurHash3

import com.typesafe.scalalogging.LazyLogging

import com.thatdot.quine.graph.cypher.{Expr, Value}
import com.thatdot.quine.model.QuineIdProvider

/** The Bolt protocol talks about how to serialize arbitrary structures,
  * and uses this to describe the format of nodes, relationships, paths, etc.
  * Custom structures are not excluded.
  *
  * @see <https://boltprotocol.org/v1/#structures>
  * @param signature a unique byte identifying the type of the structure
  * @param fields the fields of the structure
  */
final case class Structure(
  signature: Byte,
  fields: List[Value]
)
object Structure {

  /** Convert a structured value into the canonical [[Structure]] format
    *
    * @param value the value to convert
    * @param impl how to convert the value
    * @param idp ID provider
    */
  def apply[A](value: A)(implicit
    impl: Structured[A],
    idp: QuineIdProvider
  ): Structure = impl.intoStructure(value)

}

/** Types which can be represented as a [[Structure]] in the Bolt protocol */
trait Structured[A] {

  /** Extract out of a structure the given type */
  def fromStructure(structure: Structure)(implicit idp: QuineIdProvider): A

  /** Turn the given value into its structure form */
  final def intoStructure(value: A)(implicit idp: QuineIdProvider): Structure =
    Structure(signature, fields(value))

  /** Signature byte of this type */
  val signature: Byte

  /** Serialized fields of the type */
  def fields(value: A)(implicit idp: QuineIdProvider): List[Value]

}

object Structured extends LazyLogging {

  /** Cypher nodes are represented as structures.
    *
    * @see <https://boltprotocol.org/v1/#node-structure>
    */
  implicit object NodeStructure extends Structured[Expr.Node] {

    def fromStructure(structure: Structure)(implicit idp: QuineIdProvider): Expr.Node = {
      assert(structure.signature == signature, "Wrong signature for node")
      structure.fields match {
        case List(
              nodeId,
              Expr.List(lbls),
              Expr.Map(props)
            ) =>
          val nodeQid = idp.valueToQid(Expr.toQuineValue(nodeId)).getOrElse {
            throw new IllegalArgumentException(
              s"Cannot deserialize node $structure whose ID cannot be read"
            )
          }

          val lblSet = Set.newBuilder[Symbol]
          for (lbl <- lbls)
            lbl match {
              case Expr.Str(l) => lblSet += Symbol(l)
              case _ =>
                throw new IllegalArgumentException(
                  s"Structure with signature of a node has the wrong schema"
                )
            }

          Expr.Node(
            id = nodeQid,
            labels = lblSet.result(),
            properties = props.view.map(kv => Symbol(kv._1) -> kv._2).toMap
          )

        case _ =>
          throw new IllegalArgumentException(
            s"Structure with signature of a node has the wrong schema"
          )
      }
    }

    val signature: Byte = 0x4E.toByte

    def fields(node: Expr.Node)(implicit idp: QuineIdProvider): List[Value] = List(
      Expr.fromQuineValue(idp.qidToValue(node.id)) match {
        case i: Expr.Integer => i
        case other =>
          logger.warn(
            s"Serializing node: ${node.id.debug} with a non-integer ID may cause Bolt clients to crash"
          )
          other
      },
      Expr.List(node.labels.map(lbl => Expr.Str(lbl.name)).toVector),
      Expr.Map(node.properties.view.map(kv => kv._1.name -> kv._2).toMap)
    )

  }

  /** Cypher relationships are represented as structures.
    *
    * @see <https://boltprotocol.org/v1/#rel-structure>
    */
  implicit object RelationshipStructure extends Structured[Expr.Relationship] {

    val signature: Byte = 0x52.toByte

    def fromStructure(structure: Structure)(implicit idp: QuineIdProvider): Expr.Relationship = {
      assert(structure.signature == signature, "Wrong signature for relationship")
      structure.fields match {
        case List(
              Expr.Integer(_), // TODO: relationship ID goes here
              startId,
              endId,
              Expr.Str(typ),
              Expr.Map(props)
            ) =>
          Expr.Relationship(
            idp.valueToQid(Expr.toQuineValue(startId)).getOrElse {
              throw new IllegalArgumentException(
                s"Cannot deserialize edge $structure whose start cannot be read as an ID"
              )
            },
            Symbol(typ),
            props.view.map(kv => Symbol(kv._1) -> kv._2).toMap,
            idp.valueToQid(Expr.toQuineValue(endId)).getOrElse {
              throw new IllegalArgumentException(
                s"Cannot deserialize edge $structure whose end cannot be read as an ID"
              )
            }
          )
        case unknown => sys.error(s"Expected a specific list structure, but got $unknown instead")
      }
    }

    def fields(relationship: Expr.Relationship)(implicit idp: QuineIdProvider): List[Value] = List(
      // TODO: relationship ID goes here. This is a (deterministic) hack to make some UIs work!
      Expr.Integer(
        MurmurHash3
          .orderedHash(
            Vector(
              relationship.start,
              relationship.end,
              relationship.name
            )
          )
          .toLong
      ),
      Expr.fromQuineValue(idp.qidToValue(relationship.start)) match {
        case i: Expr.Integer => i
        case other =>
          logger.warn(
            s"Serializing edge with a non-integer start ID: $other may cause Bolt clients to crash"
          )
          other
      },
      Expr.fromQuineValue(idp.qidToValue(relationship.end)) match {
        case i: Expr.Integer => i
        case other =>
          logger.warn(
            s"Serializing edge with a non-integer end ID: $other may cause Bolt clients to crash"
          )
          other
      },
      Expr.Str(relationship.name.name),
      Expr.Map(Map.empty) // TODO: relationship properties go here
    )
  }

}
