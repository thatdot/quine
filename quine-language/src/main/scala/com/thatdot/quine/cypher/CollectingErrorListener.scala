package com.thatdot.quine.cypher

import scala.collection.mutable

import org.antlr.v4.runtime.{BaseErrorListener, RecognitionException, Recognizer, Token}

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
    // `line` is 1-based and `charPositionInLine` is a 0-based column counted in
    // Unicode code points (ANTLR's CodePointCharStream unit). The exclusive end
    // position is derived from the offending token, which the parser supplies as
    // `offendingSymbol`; the lexer passes null instead, and the EOF token has no
    // extent, so both fall back to width-1 and width-0 ranges respectively.
    val (endLine, endChar) = offendingSymbol match {
      case token: Token if token.getType == Token.EOF =>
        (line, charPositionInLine)
      case token: Token if token.getText != null =>
        val text = token.getText
        val newlineCount = text.count(_ == '\n')
        if (newlineCount == 0)
          (line, charPositionInLine + text.codePointCount(0, text.length))
        else {
          // The token spans multiple lines: the end column restarts after the
          // token's last newline.
          val afterLastNewline = text.substring(text.lastIndexOf('\n') + 1)
          (line + newlineCount, afterLastNewline.codePointCount(0, afterLastNewline.length))
        }
      case _ =>
        (line, charPositionInLine + 1)
    }
    val error = ParseError(
      line = line,
      char = charPositionInLine,
      message = msg,
      endLine = endLine,
      endChar = endChar,
    )
    errors += error
  }

  def getErrors: Seq[ParseError] = errors.toSeq
}
