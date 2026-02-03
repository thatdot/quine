package com.thatdot.quine.cypher

import scala.collection.mutable

import org.antlr.v4.runtime.{BaseErrorListener, RecognitionException, Recognizer}

import com.thatdot.quine.language.diagnostic.Diagnostic.ParseError

class CollectingErrorListener extends BaseErrorListener {
  val errors: mutable.ArrayBuffer[ParseError] = scala.collection.mutable.ArrayBuffer.empty[ParseError]

  override def syntaxError(
    recognizer: Recognizer[_, _],
    offendingSymbol: Any,
    line: Int,
    charPositionInLine: Int,
    msg: String,
    e: RecognitionException,
  ): Unit = {
    val error = ParseError(line = line, char = charPositionInLine, message = msg)
    errors += error
  }

  def getErrors: Seq[ParseError] = errors.toSeq
}
