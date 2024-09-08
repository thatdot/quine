package com.thatdot.quine.persistor

/** Indicates that some version ready from storage is incompatible with what
  * the code knows how to handle.
  *
  * @param context what is the versioned data representing
  * @param found what version was found when reading the data
  * @param latest what version does our code support writing
  */
class IncompatibleVersion(
  context: String,
  found: Version,
  latest: Version,
) extends Exception(
      s"Running application uses serialization format $latest for $context, which is incompatible with the currently-persisted $found",
    )
