package com.thatdot.quine.app.llm

import scala.util.Try

import cats.syntax.traverse._
import io.circe.{Json, JsonObject}

/** A JSON Schema subset validator for `llm.prompt` output: `type`, `enum`, `const`, `properties`, `required`,
  * `additionalProperties` (boolean form), `items` (single-schema form), `minimum`, `maximum`, `minLength`,
  * `maxLength`, `pattern`, `minItems`, `maxItems`, `anyOf`, `allOf`, `oneOf`, `not`; the annotations `title`,
  * `description`, `default`, and `examples` pass unenforced. [[checkSchema]] rejects everything else before
  * any request leaves Quine, so a schema never validates more permissively than it reads.
  */
object JsonSchemaValidator {

  /** The keywords [[validate]] enforces, and the annotations it permits; [[checkSchema]] rejects every other key. */
  private val knownKeywords: Set[String] = Set(
    "type",
    "enum",
    "const",
    "properties",
    "required",
    "additionalProperties",
    "items",
    "minimum",
    "maximum",
    "minLength",
    "maxLength",
    "pattern",
    "minItems",
    "maxItems",
    "anyOf",
    "allOf",
    "oneOf",
    "not",
    // Annotations: not enforced, and the model reads `description`, so they pass
    "title",
    "description",
    "default",
    "examples",
  )

  private val typeNames: Set[String] = Set("object", "array", "string", "number", "integer", "boolean", "null")

  /** Reject a schema [[validate]] would not fully enforce, with the path to each fault. An unknown keyword,
    * a misspelled type name, or a keyword value of the wrong shape would otherwise be skipped silently, so
    * the schema would pass model output that does not conform to what it reads as.
    */
  def checkSchema(schema: Json, path: String = "$"): List[String] =
    schema.asObject match {
      case None if schema.isBoolean => Nil
      case None => List(s"$path: a schema must be an object or a boolean")
      case Some(obj) =>
        def check(key: String)(rule: Json => List[String]): List[String] = obj(key).toList.flatMap(rule)
        def number(key: String): List[String] =
          check(key)(j => if (j.isNumber) Nil else List(s"$path.$key: must be a number"))
        // `validate` compares these through `toInt`, so a value it cannot hold must not pass unenforced
        def integer(key: String): List[String] =
          check(key) { j =>
            if (j.asNumber.flatMap(_.toInt).isDefined) Nil
            else List(s"$path.$key: must be an integer between -2147483648 and 2147483647")
          }
        def schemaList(key: String): List[String] = check(key) { j =>
          j.asArray match {
            case None => List(s"$path.$key: must be a list of schemas")
            case Some(subs) => subs.toList.zipWithIndex.flatMap { case (s, i) => checkSchema(s, s"$path.$key[$i]") }
          }
        }

        val unknown = obj.keys.filterNot(knownKeywords).map(k => s"""$path: unknown schema keyword "$k"""").toList
        val typeErrs = check("type") { t =>
          t.asString.map(List(_)).orElse(t.asArray.flatMap(_.toList.traverse(_.asString))) match {
            case None => List(s"$path.type: must be a type name or a list of type names")
            case Some(Nil) => List(s"$path.type: must name at least one type")
            case Some(names) => names.filterNot(typeNames).map(n => s"""$path.type: "$n" is not a JSON Schema type""")
          }
        }
        val enumErrs = check("enum")(j => if (j.isArray) Nil else List(s"$path.enum: must be an array"))
        val requiredErrs = check("required") { j =>
          if (j.asArray.exists(_.forall(_.isString))) Nil else List(s"$path.required: must be an array of strings")
        }
        val propErrs = check("properties") { j =>
          j.asObject match {
            case None => List(s"$path.properties: must be an object")
            case Some(props) => props.toList.flatMap { case (k, s) => checkSchema(s, s"$path.properties.$k") }
          }
        }
        // `validate` enforces only the boolean form, so the schema form must not pass unenforced
        val additionalErrs = check("additionalProperties") { j =>
          if (j.isBoolean) Nil
          else List(s"$path.additionalProperties: must be true or false, a schema here is not enforced")
        }
        // `validate` applies `items` as one schema per element, so the tuple form must not pass unenforced
        val itemErrs = check("items") { j =>
          if (j.isArray) List(s"$path.items: must be a single schema, not a list of schemas by position")
          else checkSchema(j, s"$path.items")
        }
        // `validate` treats a non-compiling pattern as no constraint, so it must not pass unenforced
        val patternErrs = check("pattern") { j =>
          j.asString match {
            case None => List(s"$path.pattern: must be a string")
            case Some(p) if Try(p.r).isFailure => List(s"$path.pattern: is not a valid regular expression")
            case Some(_) => Nil
          }
        }
        val notErrs = check("not")(checkSchema(_, s"$path.not"))

        unknown ++ typeErrs ++ enumErrs ++ requiredErrs ++ propErrs ++ additionalErrs ++ itemErrs ++
        number("minimum") ++ number("maximum") ++
        integer("minLength") ++ integer("maxLength") ++ integer("minItems") ++ integer("maxItems") ++
        patternErrs ++ schemaList("anyOf") ++ schemaList("allOf") ++ schemaList("oneOf") ++ notErrs
    }

  def isValid(schema: Json, instance: Json): Boolean = validate(schema, instance).isEmpty

  def validate(schema: Json, instance: Json, path: String = "$"): List[String] =
    schema.asBoolean match {
      // Boolean schemas: `true` accepts anything, `false` rejects everything.
      case Some(true) => Nil
      case Some(false) => List(s"$path: schema is `false` (rejects all values)")
      case None =>
        schema.asObject match {
          case None => Nil // non-object, non-boolean schema: be permissive
          case Some(obj) => validateObject(obj, instance, path)
        }
    }

  private def validateObject(obj: JsonObject, instance: Json, path: String): List[String] = {
    def sub(key: String): Option[Json] = obj(key)
    def subArray(key: String): List[Json] = sub(key).flatMap(_.asArray).map(_.toList).getOrElse(Nil)
    def intKeyword(key: String): Option[Int] = sub(key).flatMap(_.asNumber).flatMap(_.toInt)
    def doubleKeyword(key: String): Option[Double] = sub(key).flatMap(_.asNumber).map(_.toDouble)

    val typeErrs: List[String] =
      sub("type").map(t => validateType(t, instance, path)).getOrElse(Nil)

    val constErrs: List[String] =
      sub("const").map(c => if (c == instance) Nil else List(s"$path: value does not equal const")).getOrElse(Nil)

    val enumErrs: List[String] =
      sub("enum").flatMap(_.asArray) match {
        case Some(allowed) if !allowed.contains(instance) => List(s"$path: value not in enum")
        case _ => Nil
      }

    val anyOfErrs: List[String] =
      sub("anyOf").flatMap(_.asArray) match {
        case Some(alts) if !alts.exists(s => validate(s, instance, path).isEmpty) =>
          List(s"$path: does not match any subschema in anyOf")
        case _ => Nil
      }

    val oneOfErrs: List[String] =
      sub("oneOf").flatMap(_.asArray) match {
        case Some(alts) if alts.count(s => validate(s, instance, path).isEmpty) != 1 =>
          List(s"$path: does not match exactly one subschema in oneOf")
        case _ => Nil
      }

    val allOfErrs: List[String] =
      subArray("allOf").flatMap(s => validate(s, instance, path))

    val notErrs: List[String] =
      sub("not")
        .map(s => if (validate(s, instance, path).isEmpty) List(s"$path: matches `not` subschema") else Nil)
        .getOrElse(Nil)

    val objErrs: List[String] = instance.asObject match {
      case None => Nil
      case Some(io) =>
        val props = sub("properties").flatMap(_.asObject)
        val requiredErrs = subArray("required").flatMap(_.asString).collect {
          case k if io(k).isEmpty => s"$path.$k: required property missing"
        }
        val propErrs = props.toList.flatMap { p =>
          io.toList.flatMap { case (k, v) => p(k).toList.flatMap(s => validate(s, v, s"$path.$k")) }
        }
        val additionalErrs = sub("additionalProperties") match {
          case Some(j) if j.asBoolean.contains(false) =>
            val declared = props.map(_.keys.toSet).getOrElse(Set.empty)
            io.keys.filterNot(declared.contains).map(k => s"$path.$k: additional property not allowed").toList
          case _ => Nil
        }
        requiredErrs ++ propErrs ++ additionalErrs
    }

    val arrErrs: List[String] = instance.asArray match {
      case None => Nil
      case Some(arr) =>
        val itemErrs = sub("items").toList.flatMap { itemSchema =>
          arr.toList.zipWithIndex.flatMap { case (v, i) => validate(itemSchema, v, s"$path[$i]") }
        }
        val minItemErrs = intKeyword("minItems").collect {
          case m if arr.size < m => s"$path: fewer than $m items"
        }.toList
        val maxItemErrs = intKeyword("maxItems").collect {
          case m if arr.size > m => s"$path: more than $m items"
        }.toList
        itemErrs ++ minItemErrs ++ maxItemErrs
    }

    val strErrs: List[String] = instance.asString match {
      case None => Nil
      case Some(s) =>
        val minLenErrs = intKeyword("minLength").collect {
          case m if s.length < m => s"$path: shorter than $m chars"
        }.toList
        val maxLenErrs = intKeyword("maxLength").collect {
          case m if s.length > m => s"$path: longer than $m chars"
        }.toList
        val patternErrs = sub("pattern").flatMap(_.asString).toList.flatMap { p =>
          // An invalid regex in the schema is treated as no constraint rather than failing the value.
          if (Try(p.r.findFirstIn(s).isDefined).getOrElse(true)) Nil else List(s"$path: does not match pattern")
        }
        minLenErrs ++ maxLenErrs ++ patternErrs
    }

    val numErrs: List[String] = instance.asNumber.map(_.toDouble) match {
      case None => Nil
      case Some(n) =>
        val minErrs = doubleKeyword("minimum").collect { case m if n < m => s"$path: less than minimum $m" }.toList
        val maxErrs = doubleKeyword("maximum").collect { case m if n > m => s"$path: greater than maximum $m" }.toList
        minErrs ++ maxErrs
    }

    typeErrs ++ constErrs ++ enumErrs ++ anyOfErrs ++ oneOfErrs ++ allOfErrs ++ notErrs ++
    objErrs ++ arrErrs ++ strErrs ++ numErrs
  }

  private def validateType(typeJson: Json, instance: Json, path: String): List[String] = {
    val types: List[String] =
      typeJson.asString.map(List(_)).orElse(typeJson.asArray.map(_.toList.flatMap(_.asString))).getOrElse(Nil)
    if (types.isEmpty || types.exists(matchesType(_, instance))) Nil
    else List(s"$path: expected type ${types.mkString(" | ")}")
  }

  private def matchesType(ty: String, j: Json): Boolean = ty match {
    case "string" => j.isString
    case "number" => j.isNumber
    case "integer" => j.asNumber.flatMap(_.toBigInt).isDefined
    case "boolean" => j.isBoolean
    case "object" => j.isObject
    case "array" => j.isArray
    case "null" => j.isNull
    case _ => true // unknown type token: do not fail
  }
}
