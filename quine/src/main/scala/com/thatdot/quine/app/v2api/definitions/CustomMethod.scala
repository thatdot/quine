package com.thatdot.quine.app.v2api.definitions

import sttp.apispec.openapi.{OpenAPI, Operation, Parameter, PathItem, Reference}
import sttp.apispec.{ExampleMultipleValue, ExampleSingleValue, ExampleValue}
import sttp.tapir.{Codec, CodecFormat, DecodeResult, EndpointInput, path}

/** Helpers for AIP-136 "custom method" URLs of the form `{paramName}:verb` within a single
  * path segment (e.g. `POST /ingests/{name}:pause`).
  *
  * Tapir's path-template model treats every URL segment as either a fixed literal or a
  * single captured parameter — there is no built-in way to express literal text inside
  * the same segment as a parameter. Two cooperating pieces bridge the gap:
  *
  *   - [[CustomMethod.colonVerbPath]] builds a path capture whose codec only accepts
  *     segments ending in `:verb`, decodes the prefix as the resource ID, and produces a
  *     `DecodeResult.Mismatch` otherwise so the server interpreter falls through to the
  *     next endpoint registered at the same base path / HTTP method. The captured
  *     parameter is given a synthetic name (`paramName + Marker + verb`) so that two
  *     `:verb` endpoints on the same base path produce distinct OpenAPI path templates
  *     instead of colliding when Tapir's docs interpreter merges operations by path.
  *   - [[CustomMethod.rewriteOpenAPI]] post-processes the OpenAPI document, finds path
  *     templates containing the synthetic marker, and rewrites them to the AIP-136 form
  *     (`/path/{name}:verb`) while restoring the parameter's original name.
  *
  * For collection-level custom methods (no resource ID — e.g. `cypher:query`),
  * the colon-verb suffix is just part of a fixed path segment and does not need this
  * helper; pass it directly to `rawEndpoint`.
  */
object CustomMethod {

  /** Marker embedded into the path-parameter name to signal a colon-verb suffix.
    * Chosen to be a substring that is highly unlikely to appear in any legitimate
    * path-parameter name.
    */
  private val Marker: String = "__colonVerb__"

  /** Build a path capture that matches `{name}:verb` as a single URL segment. */
  def colonVerbPath[T](paramName: String, verb: String)(implicit
    baseCodec: Codec[String, T, CodecFormat.TextPlain],
  ): EndpointInput.PathCapture[T] = {
    require(!paramName.contains(Marker), s"paramName must not contain '$Marker'")
    require(!verb.contains(Marker), s"verb must not contain '$Marker'")
    val suffix = ":" + verb
    val codec: Codec[String, T, CodecFormat.TextPlain] =
      Codec.string.mapDecode { raw =>
        if (raw.endsWith(suffix)) baseCodec.decode(raw.stripSuffix(suffix))
        else DecodeResult.Mismatch(suffix, raw)
      }(t => baseCodec.encode(t) + suffix)
    path[T](paramName + Marker + verb)(codec)
  }

  /** Rewrite OpenAPI path templates containing the colon-verb marker.
    *
    * For each path containing `{paramName__colonVerb__verb}`:
    *   - Replace the placeholder with `{paramName}:verb`
    *   - Rename the corresponding path parameter on each operation back to `paramName`
    *
    * Multiple `:verb` endpoints sharing a base path arrive here with distinct templates
    * because the synthetic parameter name embeds the verb; after rewrite, each ends up
    * at its own `{paramName}:verb` path.
    */
  def rewriteOpenAPI(api: OpenAPI): OpenAPI = {
    val rewrites = api.paths.pathItems.toList.flatMap { case (oldPath, _) =>
      pathRewrite(oldPath).map { case (newPath, paramRenames) => (oldPath, newPath, paramRenames) }
    }

    if (rewrites.isEmpty) return api

    val updatedPathItems = rewrites.foldLeft(api.paths.pathItems) { case (acc, (oldPath, newPath, paramRenames)) =>
      acc.get(oldPath) match {
        case None => acc
        case Some(oldItem) =>
          val renamedItem = renameParametersInPathItem(oldItem, paramRenames)
          (acc - oldPath).updated(newPath, renamedItem)
      }
    }

    api.copy(paths = api.paths.copy(pathItems = updatedPathItems))
  }

  /** Compute the rewritten path and parameter renames for a single path template, if it
    * contains any colon-verb markers. Returns `None` if no rewriting is needed.
    */
  private def pathRewrite(path: String): Option[(String, Map[String, String])] = {
    if (!path.contains(Marker)) return None
    // Find every `{...Marker...}` placeholder and rewrite both the path and parameter name.
    val placeholderRegex = """\{([^{}]+)\}""".r
    var renames = Map.empty[String, String]
    val rewritten = placeholderRegex.replaceAllIn(
      path,
      m => {
        val synthetic = m.group(1)
        val markerIdx = synthetic.indexOf(Marker)
        if (markerIdx < 0) m.matched
        else {
          val paramName = synthetic.substring(0, markerIdx)
          val verb = synthetic.substring(markerIdx + Marker.length)
          renames = renames.updated(synthetic, paramName)
          // Use Regex.quoteReplacement to avoid `$` and `\` being interpreted in the replacement.
          java.util.regex.Matcher.quoteReplacement(s"{$paramName}:$verb")
        }
      },
    )
    if (renames.isEmpty) None else Some((rewritten, renames))
  }

  /** Rename path parameters in the given PathItem and any operations it carries. */
  private def renameParametersInPathItem(item: PathItem, renames: Map[String, String]): PathItem = {
    val updateOp: Operation => Operation = op => op.copy(parameters = op.parameters.map(renameParam(_, renames)))
    item.copy(
      parameters = item.parameters.map(renameParam(_, renames)),
      get = item.get.map(updateOp),
      put = item.put.map(updateOp),
      post = item.post.map(updateOp),
      delete = item.delete.map(updateOp),
      options = item.options.map(updateOp),
      head = item.head.map(updateOp),
      patch = item.patch.map(updateOp),
      trace = item.trace.map(updateOp),
    )
  }

  private def renameParam(
    p: Either[Reference, Parameter],
    renames: Map[String, String],
  ): Either[Reference, Parameter] =
    p.map { param =>
      renames.get(param.name) match {
        case None => param
        case Some(newName) =>
          // Synthetic name format: `${newName}${Marker}${verb}`. The colon-verb codec's encode
          // appends `:verb` to every value it produces, including example values that Tapir's
          // OpenAPI interpreter ran through encode. Now that the path template carries `:verb`
          // literally, strip the suffix back out of any example values.
          val verb = param.name.substring(newName.length + Marker.length)
          val suffix = ":" + verb
          param.copy(
            name = newName,
            example = param.example.map(stripSuffixFromExample(_, suffix)),
            examples = param.examples.map { case (k, v) =>
              k -> v.map(ex => ex.copy(value = ex.value.map(stripSuffixFromExample(_, suffix))))
            },
          )
      }
    }

  private def stripSuffixFromExample(ev: ExampleValue, suffix: String): ExampleValue = ev match {
    case ExampleSingleValue(s: String) if s.endsWith(suffix) =>
      ExampleSingleValue(s.stripSuffix(suffix))
    case ExampleMultipleValue(vs) =>
      ExampleMultipleValue(vs.map {
        case s: String if s.endsWith(suffix) => s.stripSuffix(suffix)
        case other => other
      })
    case other => other
  }
}
