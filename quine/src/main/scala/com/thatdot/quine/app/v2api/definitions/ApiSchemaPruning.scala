package com.thatdot.quine.app.v2api.definitions

import scala.collection.immutable.ListMap
import scala.collection.mutable

import io.circe.Json
import io.circe.syntax._
import sttp.apispec.openapi.OpenAPI
import sttp.apispec.openapi.circe._
import sttp.apispec.{Schema, SchemaLike}

/** Removes component schemas and properties from a published OpenAPI document, along with everything that only they
  * reach.
  *
  * Hiding a discriminated-union member is more than dropping its `$ref` from the `oneOf`: its component schema (and any
  * schemas reachable only through it) must also be removed, or they linger as orphaned definitions. Likewise a hidden
  * property can be the only thing reaching a schema. So we name only what to hide and then garbage collect: a component
  * schema is removed iff it is in the hidden schemas' reachable closure and is not reachable from anything that
  * remains. Schemas shared with a visible type are therefore kept.
  *
  * A property is removed if it is named for removal, or if it refers to a removed schema directly, as array items, or
  * as map values. Removed schemas must not be referenced directly by an operation.
  */
object ApiSchemaPruning {

  /** @param schemas    names of component schemas to remove
    * @param properties for each component schema name, the names of properties to remove from it
    */
  def prune(api: OpenAPI, schemas: Set[String], properties: Map[String, Set[String]]): OpenAPI =
    api.components match {
      case Some(components) if schemas.exists(components.schemas.contains) || properties.nonEmpty =>
        // Properties go first: one may be the only thing keeping a schema alive.
        val withoutProperties: ListMap[String, SchemaLike] = components.schemas.map { case (name, s) =>
          name -> removeProperties(s, properties.getOrElse(name, Set.empty), schemas)
        }
        val pruned = api.copy(components = Some(components.copy(schemas = withoutProperties)))

        // Reference graph among component schemas (robust: scan each schema's JSON for "$ref"). Derived from the
        // whole-document JSON so we only need the OpenAPI encoder, not a per-`SchemaLike` one.
        val schemasJson = pruned.asJson.hcursor.downField("components").downField("schemas")
        val edges: Map[String, Set[String]] =
          withoutProperties.keys.iterator.map { name =>
            name -> schemasJson.downField(name).focus.fold(Set.empty[String])(refNamesIn)
          }.toMap

        // Roots: schema names referenced from anywhere *except* the schema section itself (paths, parameters,
        // request/response bodies). These must survive.
        val nonSchemaJson = pruned.copy(components = Some(components.copy(schemas = ListMap.empty))).asJson
        val rootRefs = refNamesIn(nonSchemaJson)

        // What stays alive if the hidden schemas are removed, and what they reach. Anything in the hidden closure
        // that nothing else keeps alive is safe to drop.
        val live = reachable(rootRefs -- schemas, edges, blocked = schemas)
        val hiddenClosure = reachable(schemas, edges, blocked = Set.empty)
        val drop = (schemas ++ hiddenClosure) -- live

        val keptSchemas: ListMap[String, SchemaLike] =
          withoutProperties.collect { case (name, s) if !drop(name) => name -> stripDroppedRefs(s, drop) }

        pruned.copy(components = Some(components.copy(schemas = keptSchemas)))
      case _ => api
    }

  /** Remove the named properties, and those referring to a removed schema, from a schema's own properties. */
  private def removeProperties(s: SchemaLike, named: Set[String], removedSchemas: Set[String]): SchemaLike = s match {
    case schema: Schema if schema.properties.nonEmpty =>
      def refersToRemoved(p: SchemaLike): Boolean = p match {
        case ps: Schema =>
          ps.`$ref`.exists(r => removedSchemas(localName(r))) ||
            ps.items.exists(refersToRemoved) ||
            ps.additionalProperties.exists(refersToRemoved)
        case _ => false
      }
      val removed = schema.properties.collect { case (k, p) if named(k) || refersToRemoved(p) => k }.toSet
      if (removed.isEmpty) schema
      else
        schema.copy(
          properties = schema.properties.filterNot { case (k, _) => removed(k) },
          required = schema.required.filterNot(removed),
        )
    case other => other
  }

  /** Local name of every `$ref` target anywhere in the given JSON. */
  private def refNamesIn(json: Json): Set[String] = {
    val out = Set.newBuilder[String]
    def go(j: Json): Unit =
      j.fold(
        (),
        _ => (),
        _ => (),
        _ => (),
        arr => arr.foreach(go),
        obj =>
          obj.toIterable.foreach {
            case ("$ref", v) => v.asString.foreach(r => out += localName(r))
            case (_, v) => go(v)
          },
      )
    go(json)
    out.result()
  }

  /** Schema names reachable from `start` through `edges`, never entering `blocked`. */
  private def reachable(start: Set[String], edges: Map[String, Set[String]], blocked: Set[String]): Set[String] = {
    val visited = mutable.Set.empty[String]
    val queue = mutable.Queue.from(start.diff(blocked))
    while (queue.nonEmpty) {
      val n = queue.dequeue()
      if (!visited(n)) {
        visited += n
        edges.getOrElse(n, Set.empty).diff(blocked).foreach(queue.enqueue)
      }
    }
    visited.toSet
  }

  /** Remove references to dropped schemas from a kept schema's union fields. */
  private def stripDroppedRefs(s: SchemaLike, drop: Set[String]): SchemaLike = s match {
    case schema: Schema =>
      def isDropped(sl: SchemaLike): Boolean = sl match {
        case ref: Schema => ref.`$ref`.exists(r => drop(localName(r)))
        case _ => false
      }
      schema.copy(
        oneOf = schema.oneOf.filterNot(isDropped),
        anyOf = schema.anyOf.filterNot(isDropped),
        allOf = schema.allOf.filterNot(isDropped),
        discriminator = schema.discriminator.map { d =>
          d.copy(mapping = d.mapping.map(_.filterNot { case (k, v) => drop(k) || drop(localName(v)) }))
        },
      )
    case other => other
  }

  private def localName(ref: String): String = ref.substring(ref.lastIndexOf('/') + 1)
}
