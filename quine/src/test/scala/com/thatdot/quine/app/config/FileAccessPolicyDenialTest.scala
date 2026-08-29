package com.thatdot.quine.app.config

import java.nio.file.attribute.PosixFilePermissions
import java.nio.file.{Files, Path}

import cats.data.Validated
import org.scalatest.funsuite.AnyFunSuite

import com.thatdot.quine.exceptions.{FileIngestPathUnresolvable, FileIngestSecurityException}

/** Which of `validatePath`'s two failures a filesystem refusal is.
  *
  * The distinction is the whole of it: a [[FileIngestSecurityException]] is a refusal, which stops
  * an ingest, and a [[FileIngestPathUnresolvable]] is retried for as long as the ingest exists. A
  * denial is an answer -- the process was told it may not look -- so retrying it asks a question
  * that has been answered. An absent file is not: it can appear.
  */
class FileAccessPolicyDenialTest extends AnyFunSuite {

  private def withUnreadableDirectory(check: (Path, Path) => Unit): Unit = {
    val base = Files.createTempDirectory("file-access-policy-denial")
    val locked = Files.createDirectory(base.resolve("locked"))
    val file = Files.writeString(locked.resolve("data.json"), "{}\n")
    val _ = Files.setPosixFilePermissions(locked, PosixFilePermissions.fromString("---------"))
    try
    // Root ignores the bits, and a filesystem without POSIX permissions never applied them, so
    // on those there is no denial here to classify.
    if (Files.isReadable(locked)) cancel("this user can read a directory with no permissions")
    else check(locked, file)
    finally {
      val _ = Files.setPosixFilePermissions(locked, PosixFilePermissions.fromString("rwx------"))
      Files.deleteIfExists(file): Unit
      Files.deleteIfExists(locked): Unit
      Files.deleteIfExists(base): Unit
    }
  }

  test("a path the filesystem refuses to resolve is a refusal, not an unresolvable path") {
    withUnreadableDirectory { (locked, file) =>
      val policy = FileAccessPolicy(List(locked), ResolutionMode.Dynamic)
      val _ = FileAccessPolicy.validatePath(file.toString, policy) match {
        case Validated.Invalid(errors) =>
          assert(
            errors.head.isInstanceOf[FileIngestSecurityException],
            s"a permission denial was classified ${errors.head.getClass.getSimpleName}, so it is retried forever",
          )
        case Validated.Valid(path) => fail(s"a file under an unreadable directory validated: $path")
      }
    }
  }

  test("a path that is merely absent stays retryable") {
    val base = Files.createTempDirectory("file-access-policy-absent")
    try {
      val policy = FileAccessPolicy(List(base), ResolutionMode.Dynamic)
      val _ = FileAccessPolicy.validatePath(base.resolve("not-here-yet.json").toString, policy) match {
        case Validated.Invalid(errors) => assert(errors.head.isInstanceOf[FileIngestPathUnresolvable])
        case Validated.Valid(path) => fail(s"a file that does not exist validated: $path")
      }
    } finally Files.deleteIfExists(base): Unit
  }
}
