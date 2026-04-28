package com.thatdot.quine.openapi

import io.circe.Json

/** UI rendering hints for a schema. Overlay is additive — if a field is absent
  * from a given `UiHints`, the renderer falls back to its default behavior
  * (spec-order, required-vs-optional partitioning, etc.).
  *
  * `order` is an allow-list that wins first; unmentioned fields fall to the end
  * in spec order, so new schema properties render automatically without an
  * overlay update.
  *
  * `promote` forces a field into the "primary" bucket even when it would
  * otherwise be treated as optional (not required, or required-with-default).
  *
  * `hide` omits a field from the rendered form entirely.
  */
final case class UiHints(
  order: List[String] = Nil,
  promote: Set[String] = Set.empty,
  hide: Set[String] = Set.empty,
  // Custom human-facing labels per property of this schema. Lets the UI say
  // something more contextual than the referenced schema's title — e.g., the
  // `resultEnrichment: CypherQuery` property on a workflow can be labeled
  // "Enrichment Query" even though the shared `CypherQuery` schema (also used
  // as a destination type) keeps its generic title "Run Cypher Query".
  labels: Map[String, String] = Map.empty,
)

object UiHints {
  val empty: UiHints = UiHints()
}

/** Lookup of UI hints by schema name, where the name matches the key of the
  * schema under `components.schemas` in the OpenAPI document.
  */
trait UiHintsSource {
  def forSchema(schemaName: String): UiHints

  /** All known hints — used for drift checks and debugging. */
  def entries: Iterable[(String, UiHints)]
}

object UiHintsSource {

  val empty: UiHintsSource = new UiHintsSource {
    def forSchema(schemaName: String): UiHints = UiHints.empty
    def entries: Iterable[(String, UiHints)] = Nil
  }

  /** Build a source from an already-parsed JSON document shaped like
    * `{ "<SchemaName>": { "order": [...], "promote": [...], "hide": [...] }, ... }`.
    */
  def fromJson(json: Json): UiHintsSource = {
    val byName: Map[String, UiHints] = json.asObject
      .map(_.toMap.flatMap { case (name, v) => parseHints(v).map(name -> _) })
      .getOrElse(Map.empty)
    new UiHintsSource {
      def forSchema(schemaName: String): UiHints = byName.getOrElse(schemaName, UiHints.empty)
      def entries: Iterable[(String, UiHints)] = byName
    }
  }

  /** Build a source from a JSON string. Parse errors are returned on the Left
    * so the caller can decide whether to log and fall back to [[empty]].
    */
  def parse(rawJson: String): Either[String, UiHintsSource] =
    io.circe.parser.parse(rawJson).left.map(_.getMessage).map(fromJson)

  private def parseHints(v: Json): Option[UiHints] = {
    val c = v.hcursor
    Some(
      UiHints(
        order = c.downField("order").as[List[String]].getOrElse(Nil),
        promote = c.downField("promote").as[List[String]].getOrElse(Nil).toSet,
        hide = c.downField("hide").as[List[String]].getOrElse(Nil).toSet,
        labels = c.downField("labels").as[Map[String, String]].getOrElse(Map.empty),
      ),
    )
  }

  /** Check for any hint that references a schema or property that doesn't exist
    * in the current spec. Run once at startup so renames/removals in the spec
    * don't silently break the overlay.
    *
    * @param warn logging function — the library doesn't depend on any browser API,
    *             so the caller provides the logging mechanism (e.g., `dom.console.warn(_)`).
    */
  def checkDrift(source: UiHintsSource, schemas: Map[String, SchemaNode], warn: String => Unit): Unit =
    source.entries.foreach { case (schemaName, hints) =>
      schemas.get(schemaName) match {
        case None =>
          warn(s"UI hints: schema '$schemaName' does not exist in the spec.")
        case Some(schema) =>
          val specFields = schema.properties.map(_.keySet).getOrElse(Set.empty)
          val referenced = hints.order.toSet ++ hints.promote ++ hints.hide ++ hints.labels.keySet
          val unknown = referenced -- specFields
          if (unknown.nonEmpty)
            warn(
              s"UI hints for '$schemaName' reference unknown fields: ${unknown.mkString(", ")}",
            )
      }
    }
}
