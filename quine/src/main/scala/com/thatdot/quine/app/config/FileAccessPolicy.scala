package com.thatdot.quine.app.config

import java.nio.file.{Files, Path, Paths}

import cats.data.ValidatedNel
import cats.implicits._

import com.thatdot.common.logging.Log.{LazySafeLogging, LogConfig, Safe, SafeLoggableInterpolator}
import com.thatdot.quine.exceptions.FileIngestSecurityException
import com.thatdot.quine.util.BaseError

/** File access policy for ingest security
  *
  * @param allowedDirectories Allowlist of canonicalized absolute directory paths
  *                           - Empty list: Deny all file ingests (except recipe files which are automatically added)
  *                           - Non-empty list: Only specified directories allowed
  * @param resolutionMode File resolution mode (static or dynamic)
  * @param allowedFiles For static mode: Set of canonicalized file paths that existed at startup
  */
final case class FileAccessPolicy(
  allowedDirectories: List[Path],
  resolutionMode: ResolutionMode,
  allowedFiles: Set[Path] = Set.empty,
)

object FileAccessPolicy extends LazySafeLogging {

  /** Create a FileAccessPolicy from FileIngestConfig, including recipe file paths
    *
    * Recipe file paths are automatically allowed by extracting their parent directories
    * and adding them to the allowed directories list.
    *
    * @param config The file ingest configuration
    * @param recipeFilePaths File paths from recipe ingest streams
    * @return Validated FileAccessPolicy with canonicalized paths and (for static mode) allowed files
    */
  def fromConfigWithRecipePaths(
    allowedDirectories: List[String],
    resolutionMode: ResolutionMode,
    recipeFilePaths: List[String],
  )(implicit logConfig: LogConfig): ValidatedNel[String, FileAccessPolicy] = {
    // Extract parent directories from recipe file paths
    val recipeDirectories = recipeFilePaths.flatMap { filePath =>
      try {
        val path = Paths.get(filePath)
        val realPath = path.toRealPath()
        val parentDir = Option(realPath.getParent).map(_.toString)
        parentDir
      } catch {
        case e: Exception =>
          logger.error(log"Could not load folder of recipe data because of error" withException e)
          None
      }
    }.distinct

    // Merge recipe directories with configured directories
    val mergedDirectories = (allowedDirectories ++ recipeDirectories).distinct

    // Validate and canonicalize all directory paths
    val validatedPaths: ValidatedNel[String, List[Path]] = mergedDirectories
      .map { dirString =>
        try {
          val path = Paths.get(dirString)
          val absolutePath = if (path.isAbsolute) path else path.toAbsolutePath
          val canonicalPath = absolutePath.normalize()

          if (!Files.exists(canonicalPath)) {
            // This is usually because the user is using the default file_ingests/, but didn't create that folder
            // This is fine if the user doesn't want file ingests
            logger.debug(
              log"Allowed directory does not exist: ${Safe(dirString)} (resolved to: ${Safe(canonicalPath.toString)})",
            )
            List.empty[Path].validNel[String]
          } else {
            val realPath = canonicalPath.toRealPath()
            if (!Files.isDirectory(realPath)) {
              s"Allowed directory path is not a directory: $dirString (resolved to: $realPath)"
                .invalidNel[List[Path]]
            } else {
              List(realPath).validNel[String]
            }
          }
        } catch {
          case e: Exception =>
            s"Invalid allowed directory path: $dirString - ${e.getMessage}"
              .invalidNel[List[Path]]
        }
      }
      .sequence
      .map(_.flatten)

    validatedPaths.map { paths =>
      // For static mode, enumerate all files in allowed directories at startup
      // Only files directly in the directory (not subdirectories) are allowed
      val allowedFiles = resolutionMode match {
        case ResolutionMode.Static =>
          paths.flatMap { dir =>
            try {
              import scala.jdk.CollectionConverters._
              Files
                .list(dir)
                .iterator()
                .asScala
                .filter(Files.isRegularFile(_))
                .map(_.toRealPath())
                .toSet
            } catch {
              case e: Exception =>
                logger.info(log"File from allowlist was not found at startup. Will not be loaded" withException e)
                Set.empty[Path]
            }
          }.toSet
        case ResolutionMode.Dynamic =>
          Set.empty[Path]
      }

      FileAccessPolicy(paths, resolutionMode, allowedFiles)
    }
  }

  /** Validate a file path against the access policy
    *
    * @param pathString The file path to validate
    * @param policy The file access policy
    * @return Validated real Path
    */
  def validatePath(pathString: String, policy: FileAccessPolicy): ValidatedNel[BaseError, Path] =
    try {
      val path = Paths.get(pathString)
      val absolutePath = if (path.isAbsolute) path else path.toAbsolutePath
      val realPath = absolutePath.toRealPath()

      // Handle allowlist scenarios
      if (policy.allowedDirectories.isEmpty) {
        // Empty allowlist = deny all file ingests
        FileIngestSecurityException(
          s"File path not allowed: $pathString (resolved to: $realPath). " +
          s"No allowed directories configured (empty allowlist denies all file ingests).",
        ).invalidNel[Path]
      } else {
        // Check if the file's parent directory exactly matches one of the allowed directories
        // Subdirectories are NOT allowed - only files directly in the allowed directory
        val parentDir = Option(realPath.getParent)
        val isAllowed = parentDir.exists { parent =>
          policy.allowedDirectories.exists { allowedDir =>
            parent.equals(allowedDir)
          }
        }

        if (!isAllowed) {
          val parentDirStr = parentDir.map(_.toString).getOrElse("(no parent)")
          FileIngestSecurityException(
            s"File path not allowed: $pathString (resolved to: $realPath, parent: $parentDirStr).",
          )
            .invalidNel[Path]
        } else {

          // For static mode, check if file was present at startup
          policy.resolutionMode match {
            case ResolutionMode.Static =>
              if (policy.allowedFiles.contains(realPath)) {
                realPath.validNel
              } else {
                FileIngestSecurityException(
                  s"File not allowed in static resolution mode: $pathString (resolved to: $realPath). " +
                  s"Only files present at startup are allowed.",
                )
                  .invalidNel[Path]
              }
            case ResolutionMode.Dynamic =>
              // Dynamic mode allows any files in allowed directories (even files added after startup)
              realPath.validNel
          }
        }
      }
    } catch {
      case e: Exception =>
        FileIngestSecurityException(s"Invalid file path: $pathString - ${e.getMessage}")
          .invalidNel[Path]
    }
}
