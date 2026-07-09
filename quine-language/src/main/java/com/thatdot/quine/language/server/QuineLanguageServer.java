package com.thatdot.quine.language.server;

import com.thatdot.quine.language.semantic.SemanticType;

import org.eclipse.lsp4j.*;
import org.eclipse.lsp4j.jsonrpc.services.JsonRequest;
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

    public QuineLanguageServer() {
        this.textDocumentService = new QuineTextDocumentService(new ContextAwareLanguageService());
    }

    /**
     * Construct with an explicit completion name source. A running Quine app passes a source backed
     * by the live runtime function/procedure registries, so the editor offers user-defined
     * functions and procedures as completions; the no-arg constructor uses the static
     * documentation registries.
     */
    public QuineLanguageServer(NameCompletionSource nameSource) {
        this.textDocumentService =
                new QuineTextDocumentService(new ContextAwareLanguageService(nameSource));
    }

    @Override
    public CompletableFuture<InitializeResult> initialize(InitializeParams params) {
        CompletableFuture<InitializeResult> result = new CompletableFuture<>();
        ServerCapabilities capabilities = new ServerCapabilities();

        SemanticTokensLegend legend =
                new SemanticTokensLegend(SemanticType.semanticTypesJava(), List.of());

        SemanticTokensWithRegistrationOptions semanticTokensOptions =
                new SemanticTokensWithRegistrationOptions(legend);
        semanticTokensOptions.setFull(true); // Supports full document semantic tokens

        capabilities.setSemanticTokensProvider(semanticTokensOptions);

        // Incremental: the editor's Monaco LSP client always sends ranged content changes (it
        // builds them from Monaco's change events regardless of this advertised kind), and
        // QuineTextDocumentService.didChange applies them. Advertising Incremental keeps the
        // capability honest about what the server receives and handles.
        capabilities.setTextDocumentSync(TextDocumentSyncKind.Incremental);

        // Hover documentation for Quine-specific Cypher functions (textDocument/hover).
        capabilities.setHoverProvider(true);

        DiagnosticRegistrationOptions diagnosticRegOpts = new DiagnosticRegistrationOptions();
        capabilities.setDiagnosticProvider(diagnosticRegOpts);

        CompletionOptions completionOpts = new CompletionOptions();
        completionOpts.setTriggerCharacters(Arrays.asList(".", " "));
        capabilities.setCompletionProvider(completionOpts);

        InitializeResult initResult = new InitializeResult(capabilities);
        result.complete(initResult);
        return result;
    }

    /**
     * Classifies an open document's query as node-returning or tabular.
     *
     * <p>{@code quine/queryKind} is a custom request in the server's own method namespace (LSP
     * 3.17 reserves only the {@code $/} prefix; other non-protocol methods are free for vendor
     * extensions). lsp4j discovers this handler through its {@link JsonRequest} annotation when
     * the server object is passed to {@code Launcher.Builder.setLocalService}. It is a request —
     * never a server-push notification — so the verdict travels back to the client as a JSON-RPC
     * response carrying a {@code result} field.
     *
     * <p>See {@link QueryKindParams} and {@link QueryKindResult} for the wire contract, including
     * the explicit {@code "unknown"} verdict for buffers that cannot be classified.
     */
    @JsonRequest("quine/queryKind")
    public CompletableFuture<QueryKindResult> queryKind(QueryKindParams params) {
        return CompletableFuture.completedFuture(textDocumentService.queryKind(params));
    }

    @Override
    public void connect(LanguageClient languageClient) {
        // This server pushes nothing to the client (diagnostics are pull-only), so the client
        // handle is not retained.
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

    @Override
    public void setTrace(SetTraceParams params) {}
}
