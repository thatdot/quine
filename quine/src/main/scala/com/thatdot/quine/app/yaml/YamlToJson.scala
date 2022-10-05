package com.thatdot.quine.app.yaml

import scala.jdk.CollectionConverters._

import org.yaml.snakeyaml.DumperOptions.{FlowStyle, ScalarStyle}
import org.yaml.snakeyaml.LoaderOptions
import org.yaml.snakeyaml.constructor.SafeConstructor
import org.yaml.snakeyaml.nodes._
import upickle.core.{ArrVisitor, ObjVisitor, Visitor}

private class FlatteningConstructor extends SafeConstructor(new LoaderOptions) {
  def flatten(node: MappingNode): MappingNode = {
    flattenMapping(node)
    node
  }

  def construct(node: ScalarNode): AnyRef =
    getConstructor(node).construct(node)
}

class YamlJson extends ujson.AstTransformer[Node] {
  // Isn't thread-safe internally, may hence not be shared
  private val constructor = new FlatteningConstructor

  /*
   * We go through Yaml's SafeConstructor.construct sometimes to normalize extra YAML features like
   * other number formats ("0x"-prefixed or "_"-containing numbers) into plain (boxed) Java values
   * - otherwise we pass on the literal string representation of these things to the JSON AST.
   */
  def transform[T](yaml: Node, f: Visitor[_, T]): T = yaml match {
    case node: AnchorNode => sys.error("AnchorNode support is unimplemented. Got: " + node)
    case node: MappingNode =>
      transformObject(
        f,
        constructor
          .flatten(node)
          .getValue
          .asScala
          .map(pair => pair.getKeyNode.asInstanceOf[ScalarNode].getValue -> pair.getValueNode)
      )
    case node: SequenceNode => transformArray(f, node.getValue.asScala)
    case node: ScalarNode =>
      node.getTag match {
        case Tag.FLOAT => f.visitFloat64String(node.getValue, -1)
        case Tag.INT =>
          if (node.getValue.startsWith("0x") || node.getValue.contains("_"))
            f.visitFloat64(constructor.construct(node).asInstanceOf[java.lang.Number].doubleValue, -1)
          else
            f.visitFloat64StringParts(node.getValue, -1, -1, -1)
        case Tag.BOOL =>
          node.getValue match {
            // *only* the exact strings "true" and "false" will be considered booleans, all
            // other strings, like:
            // y|Y|yes|Yes|YES|n|N|no|No|NO|True|TRUE|False|FALSE|on|On|ON|off|Off|OFF
            // will be considered as strings, despite the YAML spec.
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
      null,
      null,
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
