package com.thatdot.quine.language.server;

/**
 * Result of the {@code quine/queryKind} request. Wire shape:
 *
 * <pre>{@code { "kind": "node" | "table" | "sideEffects" | "unknown" } }</pre>
 *
 * <p>{@code kind} is always present, so a client can rely on a deterministic verdict for any buffer
 * state. The verdict is modeled as the {@link QueryKind} enum throughout the server; it is held here
 * as its {@link QueryKind#wireValue() wire token} so lsp4j's gson emits the agreed string (a bare
 * enum field would serialize as its ordinal integer). See {@link QueryKind} for what each verdict
 * means.
 */
public class QueryKindResult {

    private String kind;

    public QueryKindResult() {}

    public QueryKindResult(QueryKind kind) {
        this.kind = kind.wireValue();
    }

    /** The verdict, decoded from its wire token (an absent/unknown token reads as {@link QueryKind#UNKNOWN}). */
    public QueryKind getKind() {
        return QueryKind.fromWire(kind);
    }

    public void setKind(QueryKind kind) {
        this.kind = kind.wireValue();
    }
}
