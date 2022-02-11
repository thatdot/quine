package com.thatdot.quine.persistor

/** Represents a semantic version, used to identify the persistence format used
  *
  * semantics (reflected by canReadFrom):
  *  - no compatibility guarantees between major versions (in general)
  *  - minor version changes are forwards-compatible (eg data in format 3.4.1 will be readable by a persistor using format 3.6.0)
  *  - patch version changes are backwards-compatible (eg data in format 7.3.2 will be readable by a persistor using format 7.3.1)
  */
final case class Version(major: Int, minor: Int, patch: Int) extends Ordered[Version] {
  override def toString: String = s"Version($major.$minor.$patch)"
  def shortString: String = s"$major.$minor.$patch"
  def toBytes: Array[Byte] = Array(major.toByte, minor.toByte, patch.toByte)

  def canReadFrom(onDiskV: Version): Boolean =
    onDiskV.major == major && onDiskV.minor <= minor

  override def compare(other: Version): Int =
    Ordering.by(Version.unapply).compare(this, other)
}

object Version {
  def fromBytes(bs: Array[Byte]): Option[Version] =
    if (bs.length != 3) None
    else Some(Version(bs(0).toInt, bs(1).toInt, bs(2).toInt))
}
