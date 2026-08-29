package com.thatdot.quine.exceptions

import com.thatdot.quine.util.QuineError

/** A file path could not be resolved against the filesystem: it is not there, the volume holding
  * it is not mounted, or the attempt to resolve it failed.
  *
  * Held apart from [[FileIngestSecurityException]] because the two are different KINDS of outcome
  * and a caller has to be able to tell them apart. A policy denial is an answer: the path is
  * outside the allowlist and will be outside it on the next attempt too. This is the absence of
  * one, and a filesystem that could not answer now may answer in a minute — a network volume
  * remounts, a producer writes the file it was going to write.
  *
  * Callers that must choose between stopping and retrying read this distinction. Merging the two
  * into one type, as this once did, leaves them unable to: everything arrives wearing the type
  * that means "stop", including the conditions that clear on their own.
  */
case class FileIngestPathUnresolvable(msg: String) extends QuineError {
  override def getMessage: String = msg
}
