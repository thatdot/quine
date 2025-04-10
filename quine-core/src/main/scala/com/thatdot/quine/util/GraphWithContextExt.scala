package com.thatdot.quine.util

import org.apache.pekko.stream.scaladsl.SourceWithContext

//Some implicit methods to add to GraphWithContext that are frustratingly missing
object GraphWithContextExt {
  implicit class SourceWithContextExt[Out, Ctx, Mat](source: SourceWithContext[Out, Ctx, Mat]) {
    def zipWithIndex: SourceWithContext[(Out, Long), Ctx, Mat] = SourceWithContext.fromTuples(
      source.asSource.zipWithIndex.map(t => ((t._1._1, t._2), t._1._2)),
    )
    def namedWithContext(name: String): SourceWithContext[Out, Ctx, Mat] = SourceWithContext.fromTuples(
      source.asSource.named(name),
    )
  }

}
