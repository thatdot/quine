package com.thatdot.quine.language.server;

import com.thatdot.quine.language.semantic.SemanticToken;
import com.thatdot.quine.language.semantic.SemanticType;

import org.eclipse.lsp4j.*;
import org.eclipse.lsp4j.jsonrpc.messages.Either;
import org.eclipse.lsp4j.services.TextDocumentService;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

public class QuineTextDocumentService implements TextDocumentService {

    // java.util.logging is the LSP layer's logging facility (lsp4j logs through it too); this module
    // declares no other logging dependency. The Throwable-degrading handlers below log here.
    private static final Logger LOGGER = Logger.getLogger(QuineTextDocumentService.class.getName());

    private final ContextAwareLanguageService cals;
    private final Map<String, TextDocumentItem> textDocumentManager;

    public QuineTextDocumentService(ContextAwareLanguageService cals) {
        super();
        this.cals = cals;
        // Notifications (didOpen/didChange/didClose) mutate this map while request handlers
        // (completion/hover/diagnostic/semanticTokens/queryKind) read it; lsp4j may run a handler's
        // CompletableFuture on a different thread, so the store must be safe for concurrent access.
        this.textDocumentManager = new ConcurrentHashMap<>();
    }

    public TextDocumentItem getTextDocument(String uri) {
        return textDocumentManager.get(uri);
    }

    @Override
    public void didOpen(DidOpenTextDocumentParams params) {
        TextDocumentItem textDocumentItem = params.getTextDocument();
        textDocumentManager.put(textDocumentItem.getUri(), textDocumentItem);
    }

    /**
     * Applies a document change to the stored text.
     *
     * <p>Per LSP 3.17 a {@code TextDocumentContentChangeEvent} either carries a range — the change
     * replaces that range of the document — or omits it, in which case the change's text replaces
     * the whole document. Changes are applied in array order: each change's range refers to the
     * document state produced by the change before it. The server advertises {@code Incremental}
     * sync and the editor's Monaco LSP client sends ranged changes; the whole-document
     * (absent-range) shape is still handled so a full-content change applies correctly too.
     */
    @Override
    public void didChange(DidChangeTextDocumentParams params) {
        String uri = params.getTextDocument().getUri();
        TextDocumentItem textDocument = textDocumentManager.get(uri);
        // A change for a document this service has never seen (not opened, or already closed) has
        // nothing to apply to and is dropped, rather than dereferencing a null item.
        if (textDocument == null) {
            return;
        }
        for (TextDocumentContentChangeEvent change : params.getContentChanges()) {
            textDocument.setText(applyContentChange(textDocument.getText(), change));
        }
    }

    /** Computes the text a single content change produces when applied to {@code text}. */
    private static String applyContentChange(String text, TextDocumentContentChangeEvent change) {
        Range range = change.getRange();
        if (range == null) {
            return change.getText();
        }
        int start = offsetAt(text, range.getStart());
        int end = Math.max(offsetAt(text, range.getEnd()), start);
        return text.substring(0, start) + change.getText() + text.substring(end);
    }

    /**
     * Converts an LSP position to a UTF-16 offset into the text.
     *
     * <p>LSP 3.17 positions are 0-based lines and 0-based characters in UTF-16 code units (the
     * protocol's default position encoding). Lines are split on {@code \n} only — matching ANTLR's
     * line model (its lexer advances a line only on {@code \n}) and {@link TokenPosition} on the
     * code-point side, so a caret resolves to the same line on both sides of the ANTLR/LSP boundary.
     * A {@code \r\n} buffer is unaffected (one {@code \n} per line ⇒ identical line numbers; the
     * trailing {@code \r} is line content, never a caret target). Out-of-bounds positions are
     * clamped: a character beyond the line length defaults to the line length (as the spec
     * requires), and a line beyond the document maps to the end of the text.
     */
    private static int offsetAt(String text, Position position) {
        int length = text.length();
        int lineStart = 0;
        for (int line = 0; line < position.getLine() && lineStart < length; lineStart++) {
            if (text.charAt(lineStart) == '\n') {
                line++;
            }
        }
        int lineEnd = lineStart;
        while (lineEnd < length && text.charAt(lineEnd) != '\n') {
            lineEnd++;
        }
        return Math.min(lineStart + Math.max(position.getCharacter(), 0), lineEnd);
    }

    @Override
    public void didClose(DidCloseTextDocumentParams params) {
        textDocumentManager.remove(params.getTextDocument().getUri());
    }

    @Override
    public void didSave(DidSaveTextDocumentParams params) {
    }

    /**
     * Computes completion items at the caret position.
     *
     * <p>Items are the keywords the Cypher grammar allows at the caret, derived from the parser's
     * ATN. When the caret touches a partially typed word, candidates are computed at that word's
     * start and the full set valid there is returned: per LSP 3.17 the client is responsible for
     * filtering and sorting completion items against the typed text ({@code filterText} and
     * {@code sortText} default to the label).
     *
     * <p>Items come from two sources: the keywords the grammar allows, and the function and
     * procedure names {@link ContextAwareLanguageService#nameCompletions} offers (Quine's custom
     * functions and procedures with their signature and Markdown documentation, and the standard
     * openCypher built-ins by name). A name item is a {@link CompletionItemKind#Function}, or a
     * {@link CompletionItemKind#Method} for a procedure — LSP 3.17 has no dedicated procedure
     * kind, and Method is the conventional choice for stored procedures.
     *
     * <p>Every item carries a {@code textEdit} replacing the word prefix between the start of the
     * word the caret touches and the caret itself. LSP 3.17 recommends {@code textEdit} over
     * {@code insertText} because the latter "is subject to interpretation by the client side";
     * concretely, Monaco's LSP client anchors an item without a {@code textEdit} to an empty
     * range at the caret, which breaks prefix filtering (the typed word is ignored) and makes
     * accepting an item insert next to the typed prefix instead of replacing it ({@code RETU} +
     * accept RETURN ⇒ {@code RETURETURN}). The range is single-line and contains the requested
     * position, as the spec requires.
     *
     * <p>The same {@link #caretWordPrefixRange} range serves keyword and name items, including
     * dotted names: its word-character predicate excludes {@code '.'}, so the range already
     * stops at the last dot and covers only the segment after it (the {@code time} of {@code
     * reify.ti}). {@link ContextAwareLanguageService#nameCompletions} computes each name item's
     * {@code insertText} to be exactly the text that, substituted into that range, reconstructs
     * the full name — the full label when no dot precedes the caret, the trailing segment when
     * one does — so accepting {@code reify.}{@code →}{@code reify.time} never doubles the
     * namespace.
     */
    @Override
    public CompletableFuture<Either<List<CompletionItem>, CompletionList>> completion(
            CompletionParams position) {
        List<CompletionItem> completionItems = new ArrayList<>();

        TextDocumentItem document = textDocumentManager.get(position.getTextDocument().getUri());
        if (document != null) {
            Position caret = position.getPosition();
            Range editRange = caretWordPrefixRange(document.getText(), caret);

            List<String> keywords =
                    cals.keywordCompletions(
                            document.getText(), caret.getLine(), caret.getCharacter());
            for (String keyword : keywords) {
                CompletionItem item = new CompletionItem(keyword);
                item.setKind(CompletionItemKind.Keyword);
                item.setInsertText(keyword);
                item.setTextEdit(Either.forLeft(new TextEdit(editRange, keyword)));
                completionItems.add(item);
            }

            List<NameCompletion> names =
                    cals.nameCompletions(
                            document.getText(), caret.getLine(), caret.getCharacter());
            for (NameCompletion name : names) {
                CompletionItem item = new CompletionItem(name.label());
                item.setKind(name.kind().itemKind());
                item.setInsertText(name.insertText());
                item.setFilterText(name.filterText());
                item.setTextEdit(Either.forLeft(new TextEdit(editRange, name.insertText())));
                name.detail().ifPresent(item::setDetail);
                name.documentation()
                        .ifPresent(
                                doc ->
                                        item.setDocumentation(
                                                new MarkupContent(MarkupKind.MARKDOWN, doc)));
                completionItems.add(item);
            }
        }
        return CompletableFuture.completedFuture(Either.forLeft(completionItems));
    }

    /**
     * Computes the range of the word prefix immediately before the caret: from the first
     * character of the run of letters, digits, and underscores the caret touches, to the caret.
     * When the caret does not touch such a run the range is empty, at the caret.
     *
     * <p>The caret is clamped the way {@link #offsetAt} clamps positions (a character beyond the
     * line length defaults to the line length, a line beyond the document maps to the last line),
     * and columns are 0-based UTF-16 code units per LSP 3.17's default position encoding. The
     * word-character predicate matches the candidate computation's notion of a word the user may
     * be in the middle of typing.
     */
    private static Range caretWordPrefixRange(String text, Position position) {
        int length = text.length();
        int line = 0;
        int lineStart = 0;
        // Split on \n only, matching offsetAt and ANTLR's line model (see offsetAt's note).
        while (line < position.getLine() && lineStart < length) {
            if (text.charAt(lineStart) == '\n') {
                line++;
            }
            lineStart++;
        }
        int lineEnd = lineStart;
        while (lineEnd < length && text.charAt(lineEnd) != '\n') {
            lineEnd++;
        }
        int caretColumn =
                Math.min(Math.max(position.getCharacter(), 0), lineEnd - lineStart);
        int wordStartColumn = caretColumn;
        while (wordStartColumn > 0 && isWordChar(text.charAt(lineStart + wordStartColumn - 1))) {
            wordStartColumn--;
        }
        return new Range(new Position(line, wordStartColumn), new Position(line, caretColumn));
    }

    /** A character of a word the user may be in the middle of typing. */
    private static boolean isWordChar(char c) {
        return Character.isLetterOrDigit(c) || c == '_';
    }

    /**
     * Computes hover documentation for a position.
     *
     * <p>Hover content exists only for Quine-specific function and procedure invocations: {@link
     * ContextAwareLanguageService#invocationHoverAt} resolves the invoked name the position
     * touches and answers its documentation from the function-doc and procedure-doc registries.
     * Per LSP 3.17 the result is {@code null} when there is nothing to hover — positions on
     * keywords, variables, literals, standard Cypher functions, unregistered procedures, or in
     * documents this service has never seen. A non-null result carries Markdown {@code
     * MarkupContent} and the range of the hovered name, converted from ANTLR conventions
     * (1-based line, 0-based code-point column) to LSP conventions (0-based line, 0-based UTF-16
     * column) like diagnostic ranges are.
     */
    @Override
    public CompletableFuture<Hover> hover(HoverParams params) {
        TextDocumentItem document = textDocumentManager.get(params.getTextDocument().getUri());
        if (document == null) {
            return CompletableFuture.completedFuture(null);
        }
        Position caret = params.getPosition();
        return CompletableFuture.completedFuture(
                cals.invocationHoverAt(document.getText(), caret.getLine(), caret.getCharacter())
                        .map(
                                invocationHover -> {
                                    // ANTLR increments its line counter on '\n' only, so splitting
                                    // on '\n' yields lines whose indices match ANTLR's numbering.
                                    String[] lines = document.getText().split("\n", -1);
                                    Range range =
                                            new Range(
                                                    toLspPosition(
                                                            lines,
                                                            invocationHover.startLine(),
                                                            invocationHover.startColumn()),
                                                    toLspPosition(
                                                            lines,
                                                            invocationHover.endLine(),
                                                            invocationHover.endColumn()));
                                    return new Hover(
                                            new MarkupContent(
                                                    MarkupKind.MARKDOWN,
                                                    invocationHover.markdown()),
                                            range);
                                })
                        .orElse(null));
    }

    /**
     * Computes the pull diagnostics for a document.
     *
     * <p>An empty or whitespace-only document reports no diagnostics: it is the query bar's
     * resting state, and the parse error the grammar produces for it (the statement is missing)
     * points at nothing the user can act on. The suppression is an editor-presentation decision
     * scoped to this LSP layer; {@link ContextAwareLanguageService#parseErrors} keeps reporting
     * that a blank query does not parse, so the compiler pipeline's other callers are unaffected.
     * A document this service has never seen likewise reports no diagnostics.
     */
    @Override
    public CompletableFuture<DocumentDiagnosticReport> diagnostic(DocumentDiagnosticParams params) {
        String uri = params.getTextDocument().getUri();
        TextDocumentItem document = textDocumentManager.get(uri);
        List<Diagnostic> diagnostics = new ArrayList<>();
        if (document != null && !document.getText().isBlank()) {
            String text = document.getText();
            List<com.thatdot.quine.language.diagnostic.Diagnostic> errors = cals.parseErrors(text);
            // ANTLR increments its line counter on '\n' only, so splitting on '\n'
            // yields lines whose indices match ANTLR's line numbering.
            String[] lines = text.split("\n", -1);
            for (com.thatdot.quine.language.diagnostic.Diagnostic diag : errors) {
                Diagnostic d = new Diagnostic();
                d.setRange(diagnosticRange(diag, lines));
                d.setMessage(diag.message());
                d.setSeverity(diagnosticSeverity(diag));
                diagnostics.add(d);
            }
        }

        RelatedFullDocumentDiagnosticReport rfddr = new RelatedFullDocumentDiagnosticReport();
        rfddr.setItems(diagnostics);
        DocumentDiagnosticReport ddr = new DocumentDiagnosticReport(rfddr);
        return CompletableFuture.completedFuture(ddr);
    }

    /**
     * Computes the LSP severity of a diagnostic.
     *
     * <p>Every diagnostic carries an explicit severity: LSP 3.17 leaves the interpretation of an
     * omitted severity to the client, so clients could not otherwise tell errors apart from
     * advisory diagnostics. The mapping by diagnostic kind is:
     *
     * <ul>
     *   <li>{@code ParseError}, {@code SymbolAnalysisError}, {@code TypeCheckError} ⇒ {@link
     *       DiagnosticSeverity#Error}: each reports a query the compiler pipeline cannot accept.
     *   <li>{@code SymbolAnalysisWarning} ⇒ {@link DiagnosticSeverity#Warning}: advisory only —
     *       the pipeline's own error accounting ({@code hasErrors}) excludes this kind.
     * </ul>
     */
    private static DiagnosticSeverity diagnosticSeverity(
            com.thatdot.quine.language.diagnostic.Diagnostic diag) {
        // The error/warning split is classified exhaustively on the sealed Diagnostic itself, so a
        // new variant must choose its severity rather than silently defaulting here.
        return diag.isWarning() ? DiagnosticSeverity.Warning : DiagnosticSeverity.Error;
    }

    /**
     * Computes the LSP range a diagnostic applies to.
     *
     * <p>Parse errors carry the offending token's position in ANTLR conventions (1-based line,
     * 0-based code-point column, exclusive end); the result is in LSP conventions (0-based line,
     * 0-based UTF-16 column, exclusive end). Diagnostics without position information map to a
     * range covering the whole document.
     *
     * @param diag the diagnostic to locate
     * @param lines the document text split on '\n' (ANTLR's line separator)
     */
    private static Range diagnosticRange(
            com.thatdot.quine.language.diagnostic.Diagnostic diag, String[] lines) {
        if (diag instanceof com.thatdot.quine.language.diagnostic.Diagnostic.ParseError) {
            com.thatdot.quine.language.diagnostic.Diagnostic.ParseError parseError =
                    (com.thatdot.quine.language.diagnostic.Diagnostic.ParseError) diag;
            Position start =
                    toLspPosition(lines, parseError.line(), parseError.charPositionInLine());
            Position end = toLspPosition(lines, parseError.endLine(), parseError.endChar());
            return new Range(start, end);
        }
        String lastLine = lines[lines.length - 1];
        return new Range(new Position(0, 0), new Position(lines.length - 1, lastLine.length()));
    }

    /**
     * Converts an ANTLR position to an LSP position.
     *
     * <p>ANTLR lines are 1-based and columns are 0-based counts of Unicode code points (the unit
     * of its CodePointCharStream); LSP lines are 0-based and characters are 0-based offsets in
     * UTF-16 code units (the protocol's default position encoding). Out-of-bounds positions are
     * clamped to the document.
     */
    private static Position toLspPosition(String[] lines, int antlrLine, int codePointColumn) {
        int lineIndex = Math.min(Math.max(antlrLine - 1, 0), lines.length - 1);
        String lineText = lines[lineIndex];
        int codePointsInLine = lineText.codePointCount(0, lineText.length());
        int clampedColumn = Math.min(Math.max(codePointColumn, 0), codePointsInLine);
        int utf16Column = lineText.offsetByCodePoints(0, clampedColumn);
        return new Position(lineIndex, utf16Column);
    }

    /**
     * Computes the {@code quine/queryKind} verdict for a document.
     *
     * <p>The stored text of the addressed document is classified by {@link
     * ContextAwareLanguageService#queryKind}; a document this service has never seen (not opened,
     * or already closed) classifies as {@link QueryKind#UNKNOWN} so the request always
     * answers deterministically.
     */
    public QueryKindResult queryKind(QueryKindParams params) {
        TextDocumentItem document = textDocumentManager.get(params.getTextDocument().getUri());
        if (document == null) {
            return new QueryKindResult(QueryKind.UNKNOWN);
        }
        return new QueryKindResult(cals.queryKind(document.getText()));
    }

    /**
     * Computes the semantic tokens of a document, delta-encoded per LSP 3.17.
     *
     * <p>No failure may escape this handler: lsp4j's {@code RemoteEndpoint} responds to a
     * throwing request handler but re-throws {@code Error}s, and its listener thread
     * ({@code ConcurrentMessageProcessor}) catches only {@code Exception} — so an escaping
     * {@code Error} kills the thread that reads every subsequent message, leaving the whole
     * LSP session unanswerable. The handler therefore catches {@code Throwable} and degrades
     * to an empty token list, a valid {@code textDocument/semanticTokens/full} result that
     * merely renders no semantic highlighting for the request. The caught {@code Throwable} is
     * logged so the degrade is observable rather than silent.
     */
    @Override
    public CompletableFuture<SemanticTokens> semanticTokensFull(SemanticTokensParams params) {
        try {
            return CompletableFuture.completedFuture(computeSemanticTokens(params));
        } catch (Throwable t) {
            LOGGER.log(Level.WARNING, "semanticTokens/full failed; returning no semantic tokens", t);
            return CompletableFuture.completedFuture(new SemanticTokens(new ArrayList<>()));
        }
    }

    private SemanticTokens computeSemanticTokens(SemanticTokensParams params) {
        String uri = params.getTextDocument().getUri();
        TextDocumentItem document = textDocumentManager.get(uri);
        // A document this service has never seen has no tokens to highlight.
        if (document == null) {
            return new SemanticTokens(new ArrayList<>());
        }
        String text = document.getText();
        // ANTLR reports a token's column and length in Unicode code points; LSP semantic tokens are
        // UTF-16 code units (the protocol's default position encoding). Convert through
        // toLspPosition, whose column clamp additionally clips a token whose code-point span crosses
        // a newline to its start line — the only shape the deltaLine/deltaStartChar/length encoding
        // can represent.
        String[] lines = text.split("\n", -1);

        List<Integer> data = new ArrayList<>();

        int previousLine = 1;
        int prevUtf16Column = 0;

        // The visitors emit tokens in traversal order, not always ascending by column. LSP 3.17
        // requires semantic tokens in ascending (line, startChar) order; delta-encoding an unsorted
        // stream produces a negative deltaStartChar that garbles every downstream token. Sort first.
        List<SemanticToken> tokens = new ArrayList<>(cals.semanticAnalysis(text));
        tokens.sort(
                Comparator.comparingInt(SemanticToken::line)
                        .thenComparingInt(SemanticToken::charOnLine));

        for (SemanticToken token : tokens) {
            int tokenLine = token.line();
            int utf16Column = toLspPosition(lines, tokenLine, token.charOnLine()).getCharacter();
            int utf16EndColumn =
                    toLspPosition(lines, tokenLine, token.charOnLine() + token.length()).getCharacter();

            int lineDelta = tokenLine - previousLine;
            data.add(lineDelta);

            boolean newLine = lineDelta > 0;
            if (newLine) {
                previousLine = tokenLine;
            }

            data.add(newLine ? utf16Column : utf16Column - prevUtf16Column);
            prevUtf16Column = utf16Column;

            data.add(utf16EndColumn - utf16Column);

            data.add(SemanticType.toInt(token.semanticType()));

            data.add(token.modifiers());
        }

        return new SemanticTokens(data);
    }
}
