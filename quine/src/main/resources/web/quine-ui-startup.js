// Given some value meant to represent time, return either integer milliseconds or undefined
function parseMillis(atTime) {
    if (atTime === undefined || atTime === null) return undefined;

    // Input is a string number
    var isPositiveNumberString = typeof (atTime) === "string" && atTime.match(/^\d+$/);
    if (isPositiveNumberString) return Number.parseInt(atTime);

    // Try to parse a date
    var dateStringMillis = Date.parse(atTime);
    if (!isNaN(dateStringMillis)) return dateStringMillis;

    return undefined;
}

var network = undefined;
var urlParams = new URLSearchParams(window.location.search);

// Template variable - replaced by backend with config value
// WARNING: Do NOT change the 'true' literal below! The backend searches for the exact string
// "/*{{DEFAULT_V2_API}}*/true" and replaces it with the config value (true or false).
// See: BrowserBundleRoutes.scala getJsWithInjectedConfig
var defaultQueriesOverV2Api = /*{{DEFAULT_V2_API}}*/true;

// Template variable - replaced by backend with the qp.enabled config value.
// WARNING: Do NOT change the 'false' literal below! The backend searches for the exact string
// "/*{{QP_ENABLED}}*/false" and replaces it with the config value (true or false).
// See: BrowserBundleRoutes.scala getJsWithInjectedConfig. When false, the query editor stays on
// basic (Monarch-only) highlighting and does not connect to the language server.
var qpEnabled = /*{{QP_ENABLED}}*/false;

// Compute the path prefix (Quine's base path with a terminal slash).
// This ASSUMES that any valid URL accessing this single-page application will be Quine's
// base URL followed by a single segment identifying the page (e.g., `metrics`, `docs`).
const pathPrefix = window.location.pathname.slice(0, window.location.pathname.lastIndexOf("/") + 1);

const fragment = window.location.hash.replace(/^#/, "");

// Perform a client-side redirect of /dashboard to /explorer when a non-key-value-structured
// fragment is present. This maintains backwards compatibility with URLs like
// `localhost:8080/#CALL%20recentNodes(10)`.
if (window.location.pathname.endsWith("/dashboard") && fragment && !fragment.includes("=")) {
    window.location.replace(window.location.pathname.replace(/dashboard$/, "explorer") + window.location.hash);
}

// Parse fragment parameters. For backwards compatibility, parse a fragment like
// `#CALL%20recentNodes(10)` as if it were `#query=CALL%20recentNodes(10)`.
// Only parsed on the explorer page; empty otherwise.
const fragmentParams = !window.location.pathname.endsWith("/explorer")
    ? new URLSearchParams()
    : fragment === "" || fragment.includes("=")
        ? new URLSearchParams(fragment)
        : new URLSearchParams("query=" + fragment);

// Remove the fragment without reloading the page (only on explorer page)
if (window.location.pathname.endsWith("/explorer") && window.location.hash) {
    history.replaceState(null, "", window.location.pathname);
}

window.onload = function() {
    quineBrowser.quineAppMount(document.getElementById("root"), {
        initialQuery: fragmentParams.get("query") || "",
        isQueryBarVisible: urlParams.get("interactive") != "false",
        layout: urlParams.get("layout") || "graph",
        queriesOverWs: urlParams.get("wsQueries") != "false",
        queriesOverV2Api: urlParams.get("v2Api") !== null ? urlParams.get("v2Api") != "false" : defaultQueriesOverV2Api,
        qpEnabled: qpEnabled,
        queryHistoricalTime: parseMillis(urlParams.get("atTime")),
        onNetworkCreate: function(n) {
            network = n;
        },
        documentationUrl: "docs/openapi.json",
        documentationV2Url: "api/v2/openapi.json",
        baseURI: pathPrefix,
        serverUrl: pathPrefix.replace(/\/$/, ""),
    });
};
