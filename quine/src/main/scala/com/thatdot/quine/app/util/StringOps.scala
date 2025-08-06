package com.thatdot.quine.app.util

trait StringOps {
  implicit class MultilineTransforms(multilineString: String) {
    final private val PIPE: Char = '|'
    final private val SPACE: String = " "

    def asOneLine: String = asOneLine(PIPE)
    def asOneLine(marginChar: Char): String = multilineString.stripMargin(marginChar).linesIterator.mkString(SPACE).trim
  }
}

object StringOps {
  object syntax extends StringOps
}
