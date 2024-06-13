package com.thatdot.quine.app.migrations

/** This package contains an object for each feature that may require an out-of-band migration
  * step. Each object must extend [[Migration]] and be a singleton (`object`).
  *
  * See [[Migration.Apply]]
  */
package object instances {

  /** Registry of all migrations, in order.
    */
  val all: Seq[Migration] = Seq(MultipleValuesRewrite)
  require(all.zipWithIndex.forall { case (m, i) => m.from.version == i }, "Migrations must be contiguous and in-order")
}
