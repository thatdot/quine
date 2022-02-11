package com.thatdot.quine.gremlin
import scala.util.matching.Regex
import scala.util.parsing.combinator._
import scala.util.parsing.combinator.lexical._

import com.thatdot.quine.model.{QuineIdProvider, QuineValue}

sealed abstract class GremlinToken
object GremlinToken {

  /* NB: we take advantage of quoted identifiers to have punctuation tokens
   *     whose names match their source symbol. This makes for a pretty
   *     production rules in the parser.
   */
  case object `(` extends GremlinToken
  case object `)` extends GremlinToken
  case object `[` extends GremlinToken
  case object `]` extends GremlinToken
  case object `,` extends GremlinToken
  case object `;` extends GremlinToken
  case object `=` extends GremlinToken
  case object `.` extends GremlinToken
  case object Underscore extends GremlinToken

  final case class Identifier(str: String) extends GremlinToken
  final case class LitToken(value: QuineValue) extends GremlinToken

  case object Err extends GremlinToken
}

/** A lexer for the supported subset of Gremlin */
final class GremlinLexer(
  idProvider: QuineIdProvider,
  customIdRegex: Regex,
  customLiteralsParser: Option[(Regex, String => Option[QuineValue])]
) extends JavaTokenParsers
    with Scanners {

  override type Token = GremlinToken
  override type Elem = Char

  override def errorToken(msg: String): Token = GremlinToken.Err
  override def token: Parser[Token] =
    punctuation | literal.map(GremlinToken.LitToken(_)) | identifier
  override def whitespace: Parser[Any] = "[ \r\n\t\f]*".r

  /** Lex a custom ID */
  private def customId: Parser[QuineValue.Id] =
    customIdRegex.map(idProvider.qidFromPrettyString).flatMap {
      case scala.util.Success(customId) => success(QuineValue.Id(customId))
      case scala.util.Failure(t) => failure(t.getMessage)
    }

  /** Lex a custom user literal */
  private def customLiteral: Parser[QuineValue] =
    customLiteralsParser.fold[Parser[QuineValue]](failure("no user specified custom literals")) {
      case (customLiteralRegex, func) =>
        customLiteralRegex.map(func).flatMap {
          case Some(literal) => success(literal)
          case None => failure("user specified custom literal failed to parse")
        }
    }

  /** Lex a string literal */
  // format: off
  private def stringLit: Parser[String] =
    ( stringLiteral        ^^  { s => StringContext.processEscapes(s.substring(1, s.length - 1)) }
    | "\'[^\']*\'".r       ^^  { s => s.substring(1, s.length - 1) }
    )

  /** Lex a token corresponding to a literal */
  // format: off
  private def literal: Parser[QuineValue] =
    ( customId
    | customLiteral
    | wholeNumber          ^^  { l => QuineValue.Integer(l.toLong) }
    | "true"               ^^^ { QuineValue.True }
    | "false"              ^^^ { QuineValue.False }
    | stringLit            ^^  { s => QuineValue.Str(s) }
    )

  /** Lex punctuation tokens */
  // format: off
  private def punctuation: Parser[Token] =
    ( "("                  ^^^ { GremlinToken.`(` }
    | ")"                  ^^^ { GremlinToken.`)` }
    | "["                  ^^^ { GremlinToken.`[` }
    | "]"                  ^^^ { GremlinToken.`]` }
    | ","                  ^^^ { GremlinToken.`,` }
    | ";"                  ^^^ { GremlinToken.`;` }
    | "="                  ^^^ { GremlinToken.`=` }
    | ("_" | "__")         ^^^ { GremlinToken.Underscore }
    | "."                  ^^^ { GremlinToken.`.` }
    )

  /** Lex an identifier */
  // format: off
  private def identifier: Parser[Token] =
    ident                  ^^  { GremlinToken.Identifier.apply }
}

