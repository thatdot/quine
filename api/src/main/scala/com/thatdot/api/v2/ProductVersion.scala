package com.thatdot.api.v2

/** A thatDot product with a V2 API. The products share one model of that API, so a field or variant that belongs to
  * some products only is marked with them (see [[ProductSpecific]]).
  *
  * @param slug        stable identifier for the product, used in its published spec's marks
  * @param displayName the product's name as users know it
  */
sealed abstract class ProductVersion(val slug: String, val displayName: String)

object ProductVersion {
  case object Novelty extends ProductVersion("novelty", "Novelty")

  case object Oss extends ProductVersion("oss", "Quine")

  case object Enterprise extends ProductVersion("enterprise", "Quine Enterprise")
}
