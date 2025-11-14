package com.thatdot.quine.app.config

import pureconfig.ConfigReader
import pureconfig.error.CannotConvert

/** File resolution mode for file ingest security
  *
  * - Static: Only files present at startup are allowed
  * - Dynamic: Any file in allowed directories is allowed (even files added after startup)
  */
sealed trait ResolutionMode extends Product with Serializable

object ResolutionMode {
  case object Static extends ResolutionMode
  case object Dynamic extends ResolutionMode

  implicit val configReader: ConfigReader[ResolutionMode] = ConfigReader.fromString { str =>
    str.toLowerCase match {
      case "static" => Right(Static)
      case "dynamic" => Right(Dynamic)
      case other =>
        Left(
          CannotConvert(
            other,
            "ResolutionMode",
            s"Must be either 'static' or 'dynamic', got: $other",
          ),
        )
    }
  }
}

/** Configuration for file ingest security
  *
  * @param allowedDirectories Whitelist of allowed directories for file ingestion.
  *                            - None: Use product defaults
  *                           - Some(dirs): Only specified directories allowed. Note: Empty means no paths are allowed
  *                           - Relative paths are resolved against working directory at startup
  *                           - Paths are immediately converted to absolute, canonicalized paths
  *                           - Redundant relative components (., ..) are removed during canonicalization
  * @param resolutionMode File resolution mode:
  *                       -  None: User product defaults
  *                       - Static: Only files that exist in allowed directories at startup can be ingested
  *                       - Dynamic: Any file in allowed directories can be ingested (including files created after startup)
  */
final case class FileIngestConfig(
  allowedDirectories: Option[List[String]] = None,
  resolutionMode: Option[ResolutionMode] = None,
)
