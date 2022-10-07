package com.thatdot.quine.app.yaml

import scala.jdk.CollectionConverters._

import org.snakeyaml.engine.v2.api.LoadSettings
import org.snakeyaml.engine.v2.common.{FlowStyle, ScalarStyle}
import org.snakeyaml.engine.v2.constructor.StandardConstructor
import org.snakeyaml.engine.v2.nodes.{AnchorNode, MappingNode, Node, NodeTuple, ScalarNode, SequenceNode, Tag}
import upickle.core.{ArrVisitor, ObjVisitor, Visitor}

private class FlatteningConstructor(loadSettings: LoadSettings) extends StandardConstructor(loadSettings) {
  def processKeys(node: MappingNode): MappingNode = {
    processDuplicateKeys(node)
    node
  }
}

class YamlJson(loadSettings: LoadSettings) extends ujson.AstTransformer[Node] {
  private val constructor = new FlatteningConstructor(loadSettings)

  def transform[T](yaml: Node, f: Visitor[_, T]): T = yaml match {
    case node: AnchorNode => sys.error("AnchorNode support is unimplemented. Got: " + node)
    case node: MappingNode =>
      transformObject(
        f,
        constructor
          .processKeys(node)
          .getValue
          .asScala
          .map(pair => pair.getKeyNode.asInstanceOf[ScalarNode].getValue -> pair.getValueNode)
      )
    case node: SequenceNode => transformArray(f, node.getValue.asScala)
    case node: ScalarNode =>
      // snakeyaml-engine only supports YAML 1.2's JSON Schema, NOT the "Core Schema" of YAML
      // extra formats on top of that. For booleans, that means only true/false are accepted,
      // not True/False or TRUE/FALSE. For numbers, only numbers that are valid in JSON are
      // treated as numbers, no 0x01 or anything. Everything else gets treated as a string.
      // See http://blogs.perl.org/users/tinita/2018/01/introduction-to-yaml-schemas-and-tags.html
      node.getTag match {
        case Tag.FLOAT => f.visitFloat64String(node.getValue, -1)
        case Tag.INT => f.visitFloat64StringParts(node.getValue, -1, -1, -1)
        case Tag.BOOL =>
          node.getValue match {
            case "true" => f.visitTrue(-1)
            case "false" => f.visitFalse(-1)
            case other => f.visitString(other, -1)
          }
        case Tag.NULL => f.visitNull(-1)
        case _ => f.visitString(node.getValue, -1)
      }
    case null => f.visitNull(-1)
  }

  // The following is for going the other direction: converting uJSON to YAML.
  private def literal(tag: Tag, value: String): ScalarNode =
    new ScalarNode(
      tag,
      value,
      // Upstream uses PLAIN unless there's a non-breaking zero-width space or a newline in the value:
      // https://github.com/circe/circe-yaml/blob/master/src/main/scala/io/circe/yaml/Printer.scala#L67
      if (value.contains('\u0085') || value.contains('\ufeff')) ScalarStyle.DOUBLE_QUOTED
      else if (value.contains('\n')) ScalarStyle.LITERAL
      else ScalarStyle.PLAIN
    )

  def visitArray(length: Int, index: Int): ArrVisitor[Node, Node] = new AstArrVisitor[Seq](x =>
    new SequenceNode(Tag.SEQ, x.asJava, FlowStyle.AUTO)
  )

  def visitObject(length: Int, index: Int): ObjVisitor[Node, Node] = new AstObjVisitor[Seq[(String, Node)]](x =>
    new MappingNode(
      Tag.MAP,
      x.map { case (k, v) => new NodeTuple(literal(Tag.STR, k), v) }.asJava,
      FlowStyle.AUTO
    )
  )

  def visitNull(index: Int): Node = literal(Tag.NULL, "null")

  def visitFalse(index: Int): Node = literal(Tag.BOOL, "false")

  def visitTrue(index: Int): Node = literal(Tag.BOOL, "true")

  def visitFloat64StringParts(s: CharSequence, decIndex: Int, expIndex: Int, index: Int): Node = {
    val numString = s.toString
    literal(if (numString contains ".") Tag.FLOAT else Tag.INT, numString)
  }

  def visitString(s: CharSequence, index: Int): Node = literal(Tag.STR, s.toString)
}
