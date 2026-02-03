package com.thatdot.quine.language.server;

import com.thatdot.quine.language.semantic.SemanticToken;
import com.thatdot.quine.language.semantic.SemanticType;

import org.eclipse.lsp4j.*;
import org.eclipse.lsp4j.jsonrpc.messages.Either;
import org.eclipse.lsp4j.services.TextDocumentService;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class QuineTextDocumentService implements TextDocumentService {

    private final ContextAwareLanguageService cals;
    private final Map<String, TextDocumentItem> textDocumentManager;

    public QuineTextDocumentService(ContextAwareLanguageService cals) {
        super();
        this.cals = cals;
        this.textDocumentManager = new HashMap<>();
    }

    public TextDocumentItem getTextDocument(String uri) {
        return textDocumentManager.get(uri);
    }

    @Override
    public void didOpen(DidOpenTextDocumentParams params) {
        TextDocumentItem textDocumentItem = params.getTextDocument();
        textDocumentManager.put(textDocumentItem.getUri(), textDocumentItem);
    }

    @Override
    public void didChange(DidChangeTextDocumentParams params) {
        String uri = params.getTextDocument().getUri();
        String text = params.getContentChanges().get(0).getText();
        TextDocumentItem textDocument = textDocumentManager.get(uri);
        textDocument.setText(text);
    }

    @Override
    public void didClose(DidCloseTextDocumentParams params) {
        textDocumentManager.remove(params.getTextDocument().getUri());
    }

    @Override
    public void didSave(DidSaveTextDocumentParams params) {
    }

    @Override
    public CompletableFuture<Either<List<CompletionItem>, CompletionList>> completion(
            CompletionParams position) {
        List<CompletionItem> completionItems = new ArrayList<>();

        List<String> completions = cals.edgeCompletions("");
        for (String suggestion : completions) {
            CompletionItem ci = new CompletionItem(suggestion);
            ci.setInsertText(suggestion);
            completionItems.add(ci);
        }
        return CompletableFuture.supplyAsync(() -> Either.forLeft(completionItems));
    }

    @Override
    public CompletableFuture<DocumentDiagnosticReport> diagnostic(DocumentDiagnosticParams params) {
        String uri = params.getTextDocument().getUri();
        String text = textDocumentManager.get(uri).getText();
        List<com.thatdot.quine.language.diagnostic.Diagnostic> errors = cals.parseErrors(text);
        List<Diagnostic> diagnostics = new ArrayList<>();
        for (com.thatdot.quine.language.diagnostic.Diagnostic diag : errors) {
            // TODO: Actually implement the Range of a diagnostic item.
            // Will be implemented in (QU-2111)[https://thatdot.atlassian.net/browse/QU-2111]

            Position start = new Position(0, 0);
            Position end = new Position(0, 4);

            Range r = new Range(start, end);

            Diagnostic d = new Diagnostic();
            d.setRange(r);
            d.setMessage(diag.message());
            diagnostics.add(d);
        }

        RelatedFullDocumentDiagnosticReport rfddr = new RelatedFullDocumentDiagnosticReport();
        rfddr.setItems(diagnostics);
        DocumentDiagnosticReport ddr = new DocumentDiagnosticReport(rfddr);
        return CompletableFuture.supplyAsync(() -> ddr);
    }

    @Override
    public CompletableFuture<SemanticTokens> semanticTokensRange(SemanticTokensRangeParams params) {
        return semanticTokensFull(
                new SemanticTokensParams(
                        new TextDocumentIdentifier(params.getTextDocument().getUri())));
    }

    @Override
    public CompletableFuture<SemanticTokens> semanticTokensFull(SemanticTokensParams params) {
        String uri = params.getTextDocument().getUri();
        String text = textDocumentManager.get(uri).getText();

        List<Integer> data = new ArrayList<>();

        int previousLine = 1;
        int prevCharOnLine = 0;
        boolean newLine = false;

        List<SemanticToken> tokens = cals.semanticAnalysis(text);

        for (SemanticToken token : tokens) {
            int tokenLine = token.line();
            int lineDelta = tokenLine - previousLine;
            data.add(lineDelta);

            newLine = lineDelta > 0;

            if (newLine) {
                previousLine = tokenLine;
            }

            int posDelta = 0;

            if (newLine) {
                posDelta = token.charOnLine();
            } else {
                posDelta = token.charOnLine() - prevCharOnLine;
            }
            prevCharOnLine = token.charOnLine();

            data.add(posDelta);

            data.add(token.length());

            data.add(SemanticType.toInt(token.semanticType()));

            data.add(token.modifiers());
        }

        return CompletableFuture.completedFuture(new SemanticTokens(data));
    }
}
