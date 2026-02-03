package com.thatdot.quine.language.server

import scala.jdk.CollectionConverters._

import org.eclipse.lsp4j.services.LanguageServer
import org.eclipse.lsp4j.{ClientCapabilities, InitializeParams}

object LanguageServerHelper {
  def getTokenTypesAndModifiers(
    languageServer: LanguageServer,
  ): (List[String], List[String]) = {
    val initializeParams = new InitializeParams()
    initializeParams.setCapabilities(new ClientCapabilities())

    val semanticTokensLegend = languageServer
      .initialize(initializeParams)
      .get()
      .getCapabilities()
      .getSemanticTokensProvider()
      .getLegend()
    (
      semanticTokensLegend.getTokenTypes().asScala.toList,
      semanticTokensLegend.getTokenModifiers().asScala.toList,
    )
  }
}
