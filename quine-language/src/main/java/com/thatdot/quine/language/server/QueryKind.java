package com.thatdot.quine.language.server;

/**
 * The verdict of the {@code quine/queryKind} request: how a buffer's final result set should be
 * rendered, or that there is nothing runnable. The verdict is computed deterministically for any
 * buffer state, so a client can always rely on exactly one of these.
 *
 * <p>Each constant carries the lowercase <em>wire value</em> the in-tree query editor matches in
 * {@code public/query-editor/src/queryKind.ts}. The verdict travels the wire as that string (see
 * {@link QueryKindResult}, which serializes {@link #wireValue()} rather than the enum itself —
 * lsp4j's gson encodes a bare enum as its ordinal integer). {@code QueryEditorParityTest} guards the
 * wire values against the TypeScript union.
 */
public enum QueryKind {

    /** Every result column is provably a node; the result set is graph-renderable. */
    NODE("node"),

    /**
     * The buffer is a well-formed query that is not provably node-returning: at least one result
     * column may hold a non-node value (a property, aggregate, literal, or procedure yield).
     * Tabular rendering accepts values of any shape, so this verdict is always safe to execute.
     */
    TABLE("table"),

    /**
     * The query is well-formed but produces no result rows: either the final part has no RETURN
     * clause (e.g. {@code MATCH (n) SET n.x = 1}, {@code CREATE (n)}), or the query is a standalone
     * call of a procedure with no output columns (e.g. {@code CALL util.sleep(10)}). The query runs
     * for its side effects only.
     */
    SIDE_EFFECTS("sideEffects"),

    /**
     * The buffer cannot be classified as a query: it is empty or whitespace-only, it does not
     * lex/parse, or symbol analysis rejects it. This is distinct from {@link #TABLE} so a client
     * can tell "run it as a table query" apart from "there is nothing runnable here".
     */
    UNKNOWN("unknown");

    private final String wire;

    QueryKind(String wire) {
        this.wire = wire;
    }

    /** The lowercase token this verdict serializes to on the {@code quine/queryKind} wire. */
    public String wireValue() {
        return wire;
    }

    /** The verdict for a wire token; any unrecognized (or null) token maps to {@link #UNKNOWN}. */
    public static QueryKind fromWire(String wire) {
        for (QueryKind kind : values()) {
            if (kind.wire.equals(wire)) {
                return kind;
            }
        }
        return UNKNOWN;
    }
}
