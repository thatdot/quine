package com.thatdot.quine.language.server

import scala.jdk.CollectionConverters._

import org.antlr.v4.runtime.{CharStreams, CommonTokenStream}

import com.thatdot.quine.cypher.ast.Query
import com.thatdot.quine.cypher.parsing.{CypherLexer, CypherParser}
import com.thatdot.quine.cypher.phases.{LexerPhase, LexerState, ParserPhase, SymbolAnalysisPhase, SymbolAnalysisState}
import com.thatdot.quine.cypher.visitors.semantic.QueryVisitor
import com.thatdot.quine.language.diagnostic.Diagnostic
import com.thatdot.quine.language.phases.Phase
import com.thatdot.quine.language.semantic.SemanticToken

class ContextAwareLanguageService {
  val edgeDictionary: SimpleTrie = Helpers.addItem("foo", Helpers.addItem("bar", SimpleTrie.Leaf))

  import com.thatdot.quine.language.phases.UpgradeModule._
  import com.thatdot.quine.cypher.phases.SymbolAnalysisModule.TableMonoid

  val cypherParser: Phase[LexerState, SymbolAnalysisState, String, Query] =
    LexerPhase andThen ParserPhase andThen SymbolAnalysisPhase

  def edgeCompletions(startsWith: String): java.util.List[String] = {
    def go(xs: List[Char], level: SimpleTrie, prefix: String): List[String] = xs match {
      case h :: t =>
        level match {
          case SimpleTrie.Node(children) =>
            children.get(h) match {
              case Some(child) => go(t, child, prefix + h)
              case None => List() // No further path matches the prefix
            }
          case SimpleTrie.Leaf => List(prefix) // Found a leaf, return the current prefix
        }
      case Nil =>
        level match {
          case SimpleTrie.Node(children) if children.nonEmpty =>
            children.flatMap { case (char, child) => go(Nil, child, prefix + char) }.toList
          case SimpleTrie.Node(_) => List(prefix) // If no children, return the prefix as a valid completion
          case SimpleTrie.Leaf => List(prefix) // Leaf reached, return the prefix
        }
    }

    go(startsWith.toList, edgeDictionary, "").asJava
  }

  def parseErrors(queryText: String): java.util.List[Diagnostic] = {
    val resultState = cypherParser.process(queryText).value.runS(LexerState(List.empty)).value
    resultState.diagnostics.asJava
  }

  def semanticAnalysis(queryText: String): java.util.List[SemanticToken] = {
    val input = CharStreams.fromString(queryText)
    val lexer = new CypherLexer(input)

    val tokens = new CommonTokenStream(lexer)
    val parser = new CypherParser(tokens)

    val tree = parser.oC_Query()

    QueryVisitor.visitOC_Query(tree).asJava
  }
}
