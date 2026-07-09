package com.thatdot.quine.language.server

import scala.annotation.tailrec

import org.antlr.v4.runtime.Token

import com.thatdot.quine.cypher.parsing.CypherLexer

/** Pure token-geometry helpers shared by the language service's completion and hover paths:
  * mapping an LSP caret position to a token index, recognising word-like tokens, and expanding a
  * word to the dotted name it is a segment of.
  *
  * Extracted from [[ContextAwareLanguageService]] because these functions depend only on a
  * document's token vector (and the lexer's token types) — never on the service's parsing or
  * registry state — which both isolates them for unit testing and keeps the service focused on
  * orchestration.
  *
  * Every function takes `tokens` as the document's tokens in order, ending with the EOF token
  * ANTLR appends, and treats caret offsets as code-point offsets into the text (see
  * [[caretCodePointOffset]]). All are total: an out-of-range index or a token vector missing its
  * EOF yields `None`/an empty result rather than throwing.
  */
object TokenPosition {

  /** A span of tokens by index, inclusive of both ends — the first and last token of a dotted
    * name (see [[dottedNameSpan]]). Naming the pair keeps the two indices from being transposed
    * at call sites.
    */
  final case class TokenSpan(start: Int, end: Int)

  /** Converts an LSP position to an offset into the text, measured in Unicode code points.
    *
    * LSP lines are 0-based and characters are 0-based UTF-16 code units (the protocol's default
    * position encoding); ANTLR token start/stop indices count code points (the symbols of its
    * CodePointCharStream). Lines are split on '\n', matching ANTLR's line counting, and
    * out-of-range positions are clamped to the document.
    */
  def caretCodePointOffset(text: String, line: Int, utf16Character: Int): Int = {
    val lines = text.split("\n", -1)
    val lineIndex = math.min(math.max(line, 0), lines.length - 1)
    val precedingCodePoints =
      lines.iterator.take(lineIndex).map(lineText => lineText.codePointCount(0, lineText.length) + 1).sum
    val lineText = lines(lineIndex)
    val clampedCharacter = math.min(math.max(utf16Character, 0), lineText.length)
    precedingCodePoints + lineText.codePointCount(0, clampedCharacter)
  }

  /** Locates the token index at which to collect completion candidates for a caret.
    *
    * The grammar lexes every character into a default-channel token (whitespace is the explicit
    * SP token), so the caret always touches at least one token:
    *   - on or at either edge of a word (keyword, identifier, or other letter/digit run): that
    *     token's index, so candidates are the token types valid where the word starts and a
    *     partially typed keyword completes to the keywords allowed in its place;
    *   - in or at the end of whitespace: the index after the SP token;
    *   - just past a symbol such as ')': the index after it (skipping any whitespace that
    *     follows);
    *   - at end of input: the EOF token's index;
    *   - strictly inside a multi-character symbol or literal such as a string: no index — no
    *     keyword can be completed there.
    *
    * Returns `None` when no token matches the caret (only reachable for a token vector that omits
    * the EOF token ANTLR normally appends), keeping the lookup total.
    *
    * @param tokens every token of the document in order, ending with EOF
    * @param caretOffset caret position as a code-point offset into the text
    */
  def caretTokenIndex(tokens: Vector[Token], caretOffset: Int): Option[Int] = {
    val selectedIndex =
      tokens.indexWhere(token => token.getType == Token.EOF || caretOffset <= token.getStopIndex + 1)

    tokens.lift(selectedIndex).flatMap { selected =>
      val resolvedIndex =
        if (selected.getType == Token.EOF) Some(selectedIndex)
        else if (selected.getType == CypherLexer.SP) Some(selectedIndex + 1)
        else if (isWordLike(selected)) Some(selectedIndex)
        else if (caretOffset <= selected.getStartIndex) Some(selectedIndex)
        else if (caretOffset == selected.getStopIndex + 1) Some(selectedIndex + 1)
        else None

      // Stepping past a symbol can land on the whitespace that follows it; candidates belong after
      // that whitespace. EOF is never SP, so the search always terminates in bounds.
      resolvedIndex.map { index =>
        tokens.indexWhere(token => token.getType != CypherLexer.SP, index)
      }
    }
  }

  /** A token the user may be in the middle of typing: a run of letters, digits, or underscores. */
  def isWordLike(token: Token): Boolean = {
    val text = token.getText
    text.nonEmpty && text.forall(char => Character.isLetterOrDigit(char) || char == '_')
  }

  /** Locates the word-like token a hover position touches: the position is on the token's
    * characters or at either of its boundaries (`start <= offset <= stop + 1`). Two word-like
    * tokens are never adjacent (the grammar lexes the whitespace between them), so at most one
    * token matches; positions on punctuation, inside literals, or on whitespace match none.
    *
    * @param tokens every token of the document in order, ending with EOF
    * @param caretOffset hover position as a code-point offset into the text
    */
  def hoveredWordTokenIndex(tokens: Vector[Token], caretOffset: Int): Option[Int] = {
    val index = tokens.indexWhere(token =>
      token.getType != Token.EOF &&
      isWordLike(token) &&
      token.getStartIndex <= caretOffset &&
      caretOffset <= token.getStopIndex + 1,
    )
    Option.when(index >= 0)(index)
  }

  /** Expands a word-like token to the dotted name it is a segment of: the maximal run of
    * word-like tokens joined by immediately adjacent `.` tokens (no whitespace, which the
    * grammar lexes as explicit SP tokens, may intervene). A name with no dots spans only
    * itself.
    *
    * @return the [[TokenSpan]] of the dotted name's first and last tokens, inclusive
    */
  def dottedNameSpan(tokens: Vector[Token], index: Int): TokenSpan = {
    def adjacent(left: Token, right: Token): Boolean = left.getStopIndex + 1 == right.getStartIndex
    def isDot(token: Token): Boolean = token.getText == "."
    // Total over any index trio: a missing (out-of-range) token makes the run not-joined, so the
    // bounds checks that would otherwise guard the expand* recursions are implicit here.
    def joined(wordIndex: Int, dotIndex: Int, nextWordIndex: Int): Boolean = {
      val (first, second) = if (wordIndex < nextWordIndex) (wordIndex, nextWordIndex) else (nextWordIndex, wordIndex)
      (tokens.lift(first), tokens.lift(dotIndex), tokens.lift(second)) match {
        case (Some(firstToken), Some(dotToken), Some(secondToken)) =>
          isDot(dotToken) && isWordLike(secondToken) &&
            adjacent(firstToken, dotToken) && adjacent(dotToken, secondToken)
        case _ => false
      }
    }

    @tailrec def expandStart(start: Int): Int =
      if (joined(start, start - 1, start - 2)) expandStart(start - 2) else start
    @tailrec def expandEnd(end: Int): Int =
      if (joined(end, end + 1, end + 2)) expandEnd(end + 2) else end

    TokenSpan(expandStart(index), expandEnd(index))
  }

  /** The dotted-name prefix the caret sits immediately after, including the trailing dot, or
    * the empty string when the caret is not in a dotted-name position.
    *
    * Two shapes put the caret after a dot: typing a further segment (`reify.ti`, caret on the
    * `ti` word) and resting just past the dot (`reify.`, caret on no word). In both the prefix
    * is the maximal `word(.word)*.` run ending at the caret — `reify.`, `gen.integer.` — taken
    * verbatim so the buffer's casing of the namespace is preserved. A caret on a word with no
    * dot before it (`re`), or one touching neither a word nor a trailing dot, has no committed
    * prefix, so its completions offer full names. Tokens must be immediately adjacent (no
    * intervening whitespace, which the grammar lexes as explicit SP tokens) to count as one
    * dotted name, matching [[dottedNameSpan]].
    *
    * @param tokens every token of the document in order, ending with EOF
    * @param caretOffset caret position as a code-point offset into the text
    */
  def committedDottedPrefix(tokens: Vector[Token], caretOffset: Int): String = {
    def adjacent(left: Token, right: Token): Boolean = left.getStopIndex + 1 == right.getStartIndex
    def isDot(token: Token): Boolean = token.getText == "."

    // The leftmost word index of the dotted run, paired with the exclusive end of the prefix
    // (the index just past its trailing dot): for a caret on a word, the run left of it; for a
    // caret just past a dot, the run up to and including that dot.
    val anchor: Option[(Int, Int)] =
      hoveredWordTokenIndex(tokens, caretOffset) match {
        case Some(wordIndex) =>
          // word at wordIndex preceded by `dot.word`: prev-word, dot, then the hovered word.
          (tokens.lift(wordIndex - 2), tokens.lift(wordIndex - 1), tokens.lift(wordIndex)) match {
            case (Some(prevWord), Some(dot), Some(word))
                if isDot(dot) && adjacent(prevWord, dot) && adjacent(dot, word) =>
              Some((wordIndex - 2, wordIndex))
            case _ => None
          }
        case None =>
          val dotIndex = tokens.indexWhere(token => isDot(token) && token.getStopIndex + 1 == caretOffset)
          // caret rests just past a dot: the word immediately left of that dot anchors the run.
          (tokens.lift(dotIndex - 1), tokens.lift(dotIndex)) match {
            case (Some(prevWord), Some(dot)) if isWordLike(prevWord) && adjacent(prevWord, dot) =>
              Some((dotIndex - 1, dotIndex + 1))
            case _ => None
          }
      }

    anchor.fold("") { case (lastWordIndex, prefixEnd) =>
      val TokenSpan(start, _) = dottedNameSpan(tokens, lastWordIndex)
      tokens.slice(start, prefixEnd).map(_.getText).mkString
    }
  }

  /** Whether the token at `index` is followed by `(`, skipping the grammar's explicit
    * whitespace (SP) tokens — the shape of a function or procedure invocation, where the
    * grammar allows whitespace between the invoked name and its argument list.
    */
  @tailrec
  def isFollowedByOpenParen(tokens: Vector[Token], index: Int): Boolean =
    tokens.lift(index + 1) match {
      case Some(token) if token.getType == CypherLexer.SP => isFollowedByOpenParen(tokens, index + 1)
      case Some(token) => token.getText == "("
      case None => false
    }
}
