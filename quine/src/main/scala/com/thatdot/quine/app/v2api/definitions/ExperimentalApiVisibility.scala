package com.thatdot.quine.app.v2api.definitions

import scala.collection.immutable.ListMap
import scala.collection.mutable

import io.circe.Json
import io.circe.syntax._
import sttp.apispec.openapi.OpenAPI
import sttp.apispec.openapi.circe._
import sttp.apispec.{Schema, SchemaLike}

/** Hides experimental ingest sources from the published OpenAPI document.
  *
  * Some ingest sources are wired into the API (so they can be created and run)
  * but are not yet ready to advertise to users. The runtime still accepts them;
  * we only strip them from the generated docs. Each builder of a published V2
  * document applies [[hide]] as its final step — the generated `openapi-v2.json`
  * for OSS, Enterprise, and Novelty, plus the live `/openapi.json` endpoint — so
  * the experimental sources are absent everywhere the spec is exposed.
  *
  * Hiding a discriminated-union member is more than dropping its `$ref` from the
  * `oneOf`: its component schema (and any schemas reachable only through it, e.g.
  * the Databricks auth types) must also be removed, or they linger as orphaned
  * definitions. So we name only the union member(s) to hide and then garbage
  * collect: a component schema is removed iff it is in the hidden member's
  * reachable closure and is not reachable from anything that remains. Schemas
  * shared with a visible type are therefore kept.
  */
object ExperimentalApiVisibility {

  /** Component-schema names of ingest sources to hide from the published spec. */
  val hiddenSchemas: Set[String] = Set("DeltaSharingCdf")

  def hide(api: OpenAPI): OpenAPI = api.components match {
    case Some(components) if hiddenSchemas.exists(components.schemas.contains) =>
      val schemas = components.schemas

      // Reference graph among component schemas (robust: scan each schema's JSON
      // for "$ref"). Derived from the whole-document JSON so we only need the
      // OpenAPI encoder, not a per-`SchemaLike` one.
      val schemasJson = api.asJson.hcursor.downField("components").downField("schemas")
      val edges: Map[String, Set[String]] =
        schemas.keys.iterator.map { name =>
          name -> schemasJson.downField(name).focus.fold(Set.empty[String])(refNamesIn)
        }.toMap

      // Roots: schema names referenced from anywhere *except* the schema section
      // itself (paths, parameters, request/response bodies). These must survive.
      val nonSchemaJson = api.copy(components = Some(components.copy(schemas = ListMap.empty))).asJson
      val rootRefs = refNamesIn(nonSchemaJson)

      // What stays alive if the hidden members are removed, and what the hidden
      // members reach. Anything in the hidden closure that nothing else keeps alive
      // is safe to drop.
      val live = reachable(rootRefs -- hiddenSchemas, edges, blocked = hiddenSchemas)
      val hiddenClosure = reachable(hiddenSchemas, edges, blocked = Set.empty)
      val drop = (hiddenSchemas ++ hiddenClosure) -- live

      val keptSchemas: ListMap[String, SchemaLike] =
        schemas.iterator.collect { case (name, s) if !drop(name) => name -> stripDroppedRefs(s, drop) }.to(ListMap)

      api.copy(components = Some(components.copy(schemas = keptSchemas)))
    case _ => api
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
