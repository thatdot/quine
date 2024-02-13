package com.thatdot.quine.graph

import java.util.regex

import com.softwaremill.diffx.Diff

trait DiffxInstances {
  // java.util.regex.Pattern or Array don't have a good equality method
  // So we define the Diff here in terms of things which do have good equality methods / existing Diffs:
  // the string pattern from which the Pattern was compiled, and the Seq version of the Array
  // Without these, we'd get false diffs / test failures from things with the same value not
  // getting reported as equal due to being different instances (not reference equals)
  implicit val regexPatternDiff: Diff[regex.Pattern] = Diff[String].contramap(_.pattern)
  implicit val byteArrayDiff: Diff[Array[Byte]] = Diff[Seq[Byte]].contramap(_.toSeq)
}
object DiffxInstances extends DiffxInstances
