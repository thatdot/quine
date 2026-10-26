package com.thatdot.api.v2

import scala.collection.mutable

import sttp.tapir.Schema.SName
import sttp.tapir.SchemaType._
import sttp.tapir.docs.apispec.DocsExtensionAttribute.RichSchema
import sttp.tapir.json.circe._
import sttp.tapir.{AttributeKey, Schema}

/** Fields and variants of the shared V2 API model that belong to some products only.
  *
  * Such a field or variant is defined once and marked with the products it belongs to (see [[only]]). Every product's
  * published API documentation omits what is not its own, and its server and recipes refuse it (see [[validate]]).
  */
object ProductSpecific {

  /** OpenAPI extension listing, by slug, the products a component schema or property belongs to. Each product's
    * published spec drops what is not its own and then removes this extension.
    */
  val docsExtensionKey: String = "x-products"

  /** The same information as [[docsExtensionKey]], for checks made against values at runtime. */
  private val attributeKey: AttributeKey[Set[ProductVersion]] = AttributeKey[Set[ProductVersion]]

  /** Marks a schema as belonging to the given products only, for use as `@customise(...)` on a case class, on a
    * variant of a sealed type, or on a field.
    *
    * A field whose type is an object renders in the spec as a reference, which drops a mark placed on the field, so
    * mark the object's type instead. A marked field must also be optional, since the other products leave it out.
    * [[misplacedMarks]] finds marks that break either rule.
    */
  def only(products: ProductVersion*): Schema[_] => Schema[_] = { schema =>
    schema
      .attribute(attributeKey, products.toSet)
      .docsExtension(docsExtensionKey, products.map(_.slug).toList)
  }

  /** Marks a schema as belonging to Quine Enterprise only; see [[only]]. */
  val enterpriseOnly: Schema[_] => Schema[_] = only(ProductVersion.Enterprise)

  /** A refusal of `value` naming every field and variant in it that `product` does not support, if there are any. */
  def validate[T](value: T, product: ProductVersion)(implicit schema: Schema[T]): Option[FieldViolationsException] =
    unsupported(value, product) match {
      case Nil => None
      case violations =>
        Some(
          new FieldViolationsException(
            s"The request uses fields or options that ${product.displayName} does not support.",
            violations,
          ),
        )
    }

  /** Every field and variant in `value` marked as belonging to products other than `product`, found by walking the
    * value alongside its schema. A marked part is reported once, without looking inside it.
    *
    * Working from the decoded value rather than from the request text keeps the check the same for every way a
    * definition arrives (JSON or YAML request bodies, and recipes), with the same field names and variant choices as
    * the codecs.
    */
  private def unsupported[T](value: T, product: ProductVersion)(implicit
    schema: Schema[T],
  ): List[ErrorDetail.FieldViolation] = {
    val found = List.newBuilder[ErrorDetail.FieldViolation]
    val description = s"Not supported in ${product.displayName}."

    def visit(v: Any, s: Schema[_], path: FieldPath, named: Map[SName, Schema[_]]): Unit = {
      lazy val withThis = s.name.fold(named)(named.updated(_, s))
      s.schemaType match {
        // An absent optional value uses nothing, even where the option itself is marked.
        case o: SOption[_, _] if o.asInstanceOf[SOption[Any, Any]].toOption(v).isEmpty => ()
        case _ if s.attribute(attributeKey).exists(!_.contains(product)) =>
          found += ErrorDetail.FieldViolation(path.render, description)
        case o: SOption[_, _] =>
          o.asInstanceOf[SOption[Any, Any]].toOption(v).foreach(visit(_, o.element, path, withThis))
        case a: SArray[_, _] =>
          a.asInstanceOf[SArray[Any, Any]].toIterable(v).iterator.zipWithIndex.foreach { case (e, i) =>
            visit(e, a.element, path.index(i), withThis)
          }
        case p: SProduct[_] =>
          visitFields(v, p.asInstanceOf[SProduct[Any]].fields, path, withThis)
        case op: SOpenProduct[_, _] =>
          val open = op.asInstanceOf[SOpenProduct[Any, Any]]
          visitFields(v, open.fields, path, withThis)
          open.mapFieldValues(v).foreach { case (k, fv) => visit(fv, open.valueSchema, path.key(k), withThis) }
        case c: SCoproduct[_] =>
          c.asInstanceOf[SCoproduct[Any]].subtypeSchema(v).foreach(sv => visit(sv.value, sv.schema, path, withThis))
        case SRef(name) =>
          named.get(name).foreach(visit(v, _, path, named))
        case _ => ()
      }
    }

    def visitFields(v: Any, fields: List[SProductField[Any]], path: FieldPath, named: Map[SName, Schema[_]]): Unit =
      fields.foreach(f => f.get(v).foreach(visit(_, f.schema, path.field(f.name.encodedName), named)))

    visit(value, schema, FieldPath.root, Map.empty)
    found.result()
  }

  /** Marks within `schema` that cannot work, each described with where it is (see [[only]]):
    *   - a marked field that is not optional, which the other products could not leave out;
    *   - a mark on an optional field whose type is an unmarked object, which the published spec would lose.
    */
  def misplacedMarks(schema: Schema[_]): List[String] = {
    val problems = List.newBuilder[String]
    val seen = mutable.Set.empty[SName]

    def isMarked(s: Schema[_]): Boolean = s.attribute(attributeKey).isDefined

    def isObject(s: Schema[_]): Boolean = s.schemaType match {
      case _: SProduct[_] | _: SCoproduct[_] | _: SOpenProduct[_, _] | _: SRef[_] => s.name.isDefined
      case _ => false
    }

    def visit(s: Schema[_]): Unit =
      if (s.name.forall(seen.add)) s.schemaType match {
        case o: SOption[_, _] => visit(o.element)
        case a: SArray[_, _] => visit(a.element)
        case c: SCoproduct[_] => c.subtypes.foreach(visit)
        case p: SProduct[_] => p.fields.foreach(f => visitField(s, f.name.encodedName, f.schema))
        case op: SOpenProduct[_, _] =>
          op.fields.foreach(f => visitField(s, f.name.encodedName, f.schema))
          visit(op.valueSchema)
        case _ => ()
      }

    def visitField(owner: Schema[_], name: String, field: Schema[_]): Unit = {
      val where = s"${owner.name.fold("(unnamed)")(_.show)}.$name"
      if (isMarked(field)) field.schemaType match {
        case o: SOption[_, _] =>
          if (isObject(o.element) && !isMarked(o.element))
            problems += s"$where: mark the field's type, since a mark on a field of object type is lost from the spec"
        case _ =>
          problems += s"$where: a marked field must be optional, since the other products leave it out"
      }
      visit(field)
    }

    visit(schema)
    problems.result()
  }

  /** A path to a field within a request body, rendered in AIP-161 form: `a.b[2].c`, with map keys that are not plain
    * identifiers in backticks.
    */
  final private case class FieldPath(segments: Vector[String]) {
    def field(name: String): FieldPath = FieldPath(segments :+ s".$name")
    def index(i: Int): FieldPath = FieldPath(segments :+ s"[$i]")
    def key(k: String): FieldPath = field(if (FieldPath.isIdentifier(k)) k else s"`$k`")
    def render: String = segments.mkString.stripPrefix(".")
  }

  private object FieldPath {
    val root: FieldPath = FieldPath(Vector.empty)
    def isIdentifier(s: String): Boolean = s.nonEmpty && s.forall(c => c.isLetterOrDigit || c == '_')
  }
}
