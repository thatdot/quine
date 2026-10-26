package com.thatdot.quine.app.model.outputs2.destination

import scala.util.Try

import org.apache.pekko.http.scaladsl.model.Uri

/** URLs of a Novelty instance's API, built from a configured Novelty base URL (an `endpoints` entry
  * of a Novelty output destination).
  */
object NoveltyEndpoints {

  /** Why `baseUrl` is not a usable Novelty base URL, or `None` if it is: it must be an absolute
    * `http` or `https` URL with a host.
    */
  def invalidBaseUrl(baseUrl: String): Option[String] =
    Try(Uri(baseUrl.trim)).toOption match {
      case Some(uri) if (uri.scheme == "http" || uri.scheme == "https") && !uri.authority.host.isEmpty => None
      case _ => Some(s"`$baseUrl` is not an http(s) URL with a host, e.g. `http://localhost:8382`.")
    }

  /** The models listing (`GET /api/v2/system/models`) of the Novelty instance at `baseUrl`. */
  def modelsUri(baseUrl: String): Uri = {
    val base = Uri(baseUrl.trim.stripSuffix("/"))
    base.withPath(base.path ++ (Uri.Path.Empty / "api" / "v2" / "system" / "models"))
  }

  /** Why `model` can never name a Novelty model, or `None` if it can. Novelty lowercases model names,
    * then requires 1 to 16 letters and digits, starting with a letter.
    */
  def invalidModel(model: String): Option[String] =
    Option.when(!ModelPattern.matches(model.toLowerCase))(
      s"`model` must be 1 to 16 letters and digits, starting with a letter, but was `$model`.",
    )

  private val ModelPattern = "[a-z][a-z0-9]{0,15}".r

  /** The model's ingests collection on the Novelty instance at `baseUrl`, with `segments` appended.
    * Each segment is percent-encoded, so a model or ingest name may hold any characters.
    */
  def ingestsUri(baseUrl: String, model: String, segments: String*): Uri = {
    val base = Uri(baseUrl.trim.stripSuffix("/"))
    val path = (Uri.Path.Empty / "api" / "v2" / "model" / model / "ingests") ++
      segments.foldLeft(Uri.Path.Empty: Uri.Path)(_ / _)
    base.withPath(base.path ++ path)
  }

  /** The WebSocket URL (`http` → `ws`, `https` → `wss`) of the "Quine" ingest named `ingestName`. */
  def quineIngestWsUri(baseUrl: String, model: String, ingestName: String): Uri = {
    val uri = ingestsUri(baseUrl, model, ingestName, "quine")
    uri.withScheme(if (uri.scheme == "https") "wss" else "ws")
  }
}
