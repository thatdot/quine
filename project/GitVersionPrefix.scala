// The exact structure of this object is important. Namely, this file should
// contain an object called `GitVersionPrefix` with a single member `string`.
// This keeps open source builds happily available for integration with the
// greater thatDot ecosystem.
object GitVersionPrefix {
  val string = "v"
}
