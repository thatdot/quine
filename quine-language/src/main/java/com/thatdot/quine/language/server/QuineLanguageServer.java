package com.thatdot.quine.language.server;

import com.thatdot.quine.language.semantic.SemanticType;

import org.eclipse.lsp4j.*;
import org.eclipse.lsp4j.services.LanguageClient;
import org.eclipse.lsp4j.services.LanguageClientAware;
import org.eclipse.lsp4j.services.LanguageServer;
import org.eclipse.lsp4j.services.TextDocumentService;
import org.eclipse.lsp4j.services.WorkspaceService;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class QuineLanguageServer implements LanguageServer, LanguageClientAware {

    private QuineTextDocumentService textDocumentService;
    private LanguageClient languageClient;

    public QuineLanguageServer() {
        this.textDocumentService = new QuineTextDocumentService(new ContextAwareLanguageService());
    }

    @Override
    public CompletableFuture<InitializeResult> initialize(InitializeParams params) {
        CompletableFuture<InitializeResult> result = new CompletableFuture<>();
        ServerCapabilities capabilities = new ServerCapabilities();

        SemanticTokensLegend legend =
                new SemanticTokensLegend(SemanticType.semanticTypesJava(), List.of());

        SemanticTokensWithRegistrationOptions semanticTokensOptions =
                new SemanticTokensWithRegistrationOptions(legend);
        semanticTokensOptions.setRange(true); // Optional, if you want to support partial updates
        semanticTokensOptions.setFull(true); // Supports full document semantic tokens

        capabilities.setSemanticTokensProvider(semanticTokensOptions);

        TextDocumentSyncKind textDocumentSync = TextDocumentSyncKind.Full;
        capabilities.setTextDocumentSync(textDocumentSync);

        DiagnosticRegistrationOptions diagnosticRegOpts = new DiagnosticRegistrationOptions();
        capabilities.setDiagnosticProvider(diagnosticRegOpts);

        CompletionOptions completionOpts = new CompletionOptions();
        completionOpts.setTriggerCharacters(Arrays.asList(".", " "));
        capabilities.setCompletionProvider(completionOpts);

        InitializeResult initResult = new InitializeResult(capabilities);
        result.complete(initResult);
        return result;
    }

    @Override
    public void connect(LanguageClient languageClient) {
        this.languageClient = languageClient;
    }

    @Override
    public CompletableFuture<Object> shutdown() {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public void exit() {}

    @Override
    public TextDocumentService getTextDocumentService() {
        return this.textDocumentService;
    }

    @Override
    public WorkspaceService getWorkspaceService() {
        return null;
    }

    public void setTrace(SetTraceParams params) {}
}
