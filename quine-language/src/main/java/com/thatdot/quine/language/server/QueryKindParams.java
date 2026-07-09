package com.thatdot.quine.language.server;

import org.eclipse.lsp4j.TextDocumentIdentifier;

/**
 * Parameters of the {@code quine/queryKind} request.
 *
 * <p>The request addresses the buffer to classify the same way pull diagnostics do: by the text
 * document's URI. Wire shape:
 *
 * <pre>{@code { "textDocument": { "uri": "file:///..." } } }</pre>
 */
public class QueryKindParams {

    private TextDocumentIdentifier textDocument;

    public QueryKindParams() {}

    public QueryKindParams(TextDocumentIdentifier textDocument) {
        this.textDocument = textDocument;
    }

    public TextDocumentIdentifier getTextDocument() {
        return textDocument;
    }

    public void setTextDocument(TextDocumentIdentifier textDocument) {
        this.textDocument = textDocument;
    }
}
