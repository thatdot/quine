package com.thatdot.api.v2

import io.circe.syntax.EncoderOps
import io.circe.{Decoder, Encoder, Json}
import sttp.tapir.Schema

/** Pagination envelope for list responses, per AIP-158 / `google.api.PaginatedResponse`.
  *
  * `items` carries the resources returned by this page. `nextPageToken` is an opaque
  * continuation token clients pass back as a future request's `pageToken` to fetch the
  * subsequent page; `None` signals there are no further pages and the field is omitted
  * from the JSON wire format (matching canonical proto3 → JSON encoding, where fields at
  * their default value are not emitted).
  *
  * The envelope is applied to every list response over a user-resource collection
  * (e.g. ingest streams, standing queries, namespaces) regardless of whether server-side
  * paging is currently implemented for that endpoint. The wrapper itself is the
  * forward-compatibility contract — changing a list endpoint from a bare array to an
  * envelope is a breaking change, so we ship the envelope before V2 stabilizes even
  * when `nextPageToken` is always `None` for the foreseeable future.
  *
  * Endpoints that intentionally return bare arrays — small, admin-curated configuration
  * blobs (e.g. `/queryUi/sampleQueries`) — are not wrapped; they are documented at
  * their endpoint definitions.
  */
final case class Page[+T](items: List[T], nextPageToken: Option[String] = None)

object Page {

  /** Build a single-page response containing every element of `items`. */
  def of[T](items: Iterable[T]): Page[T] = Page(items.toList)

  /** Encoder that omits an absent `nextPageToken` from the JSON output rather than
    * emitting `null`. Matches canonical proto3 JSON encoding for a default-valued field.
    */
  implicit def encoder[A: Encoder]: Encoder[Page[A]] = Encoder.instance { page =>
    val withItems = Json.obj("items" -> page.items.asJson)
    page.nextPageToken match {
      case Some(token) if token.nonEmpty =>
        withItems.deepMerge(Json.obj("nextPageToken" -> token.asJson))
      case _ => withItems
    }
  }

  /** Decoder that accepts the field either omitted, `null`, or as an empty string,
    * canonicalizing all three to `None` so callers have a single end-of-pages signal.
    */
  implicit def decoder[A: Decoder]: Decoder[Page[A]] = Decoder.instance { cursor =>
    for {
      items <- cursor.get[List[A]]("items")
      rawToken <- cursor.get[Option[String]]("nextPageToken").orElse(Right(None))
    } yield Page(items, rawToken.filter(_.nonEmpty))
  }

  /** Derive a schema parameterized by the inner element's name so each `Page[T]` ends
    * up as a distinct OpenAPI component (`Page_Ingest`, `Page_StandingQuery`, …) rather
    * than collapsing into a single `Page_A` shared across every list endpoint.
    */
  implicit def schema[A](implicit inner: Schema[A]): Schema[Page[A]] = {
    val innerName: String = inner.name match {
      case Some(sname) => sname.fullName.split('.').last
      case None => "Item"
    }
    Schema.derived[Page[A]].name(Schema.SName("com.thatdot.api.v2.Page", List(innerName)))
  }
}
