package com.thatdot.quine.openapi

/** Schema-level analysis helpers shared by the form renderer and any form
  * that wants to peek at discriminated-union shapes before rendering.
  *
  * These helpers cope with two flavours of discriminated `oneOf` produced by
  * Tapir's OpenAPI generator:
  *   1. Standard OpenAPI: an explicit `discriminator` block with a `mapping`.
  *   2. Tapir's default: a bare `oneOf` of all-`$ref` variants. Wire format
  *      is `{ "type": "<VariantName>" }`; we recover the wire value from the
  *      variant's title (when it's a Scala identifier) or from the ref name,
  *      stripping Tapir's disambiguating digits when the stripped prefix also
  *      exists as a sibling schema.
  */
object SchemaAnalysis {

  private val ScalaIdentifier = """^[A-Za-z_][A-Za-z0-9_]*$""".r
  private val TrailingDigits = """^(.+?)(\d+)$""".r

  /** A `oneOf` with each variant's wire discriminator value paired with the
    * fully-resolved variant schema, ready for either rendering or selector UI.
    */
  final case class DiscriminatedUnion(
    propertyName: String,
    variants: List[(String, SchemaNode)],
  ) {
    def variantMap: Map[String, SchemaNode] = variants.toMap
  }

  /** Interpret a node as a discriminated union, if it looks like one. */
  def discriminatedUnion(node: SchemaNode, spec: ParsedSpec): Option[DiscriminatedUnion] =
    node.oneOf.filter(_.nonEmpty).flatMap { variants =>
      val propertyName = node.discriminator.map(_.propertyName).getOrElse("type")

      val fromExplicitMapping: Option[List[(String, SchemaNode)]] =
        node.discriminator.flatMap(_.mapping).map { mapping =>
          mapping.toList.map { case (discValue, refStr) =>
            discValue -> OpenApiParser.resolveRef(refStr, spec.schemas)
          }
        }

      def fromRefs: Option[List[(String, SchemaNode)]] = {
        val byRef = variants.flatMap(v => v.ref.map(r => r.split("/").last -> r))
        if (byRef.size != variants.size) None
        else
          Some(byRef.map { case (refName, refStr) =>
            val resolved = OpenApiParser.resolveRef(refStr, spec.schemas)
            wireDiscriminatorValue(refName, resolved, spec) -> resolved
          })
      }

      fromExplicitMapping.orElse(fromRefs).map(DiscriminatedUnion(propertyName, _))
    }

  /** Wire discriminator value for a ref-style variant. Tapir disambiguates ref
    * names across the whole spec when multiple sealed traits share a case-class
    * simple name (e.g. `FileFormat.Json` stays `Json`, `StreamingFormat.Json`
    * becomes `Json1`); the wire format keeps the original class name on both.
    *
    *   1. If the variant's `title` is a Scala identifier, use it (Tapir's
    *      default title is the unadorned class name). Falls back when an
    *      explicit `@title(...)` annotation has whitespace or punctuation.
    *   2. Otherwise, if the ref name ends in digits AND the stripped prefix
    *      exists as a sibling schema, use the stripped prefix — that's
    *      exactly Tapir's disambiguation pattern.
    *
    * Falls back to the ref name when neither heuristic applies.
    */
  def wireDiscriminatorValue(
    refName: String,
    variantSchema: SchemaNode,
    spec: ParsedSpec,
  ): String =
    variantSchema.title match {
      case Some(t) if ScalaIdentifier.pattern.matcher(t).matches() => t
      case _ =>
        refName match {
          case TrailingDigits(prefix, _) if spec.schemas.contains(prefix) => prefix
          case _ => refName
        }
    }
}
