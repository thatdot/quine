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

var apiPaths = ["dashboard", "v2docs", "docs"];

function deriveProxySafeBaseURI() {
    return apiPaths.reduce((incrementalDerivationString, terminalPath) => {
        var regexA = new RegExp(`${terminalPath}$`);
        var regexB = new RegExp(`${terminalPath}\/$`);
        return incrementalDerivationString.replace(regexA,"").replace(regexB,"");
    }, window.location.pathname);
};

var derivedBaseURI = deriveProxySafeBaseURI();

window.onload = function() {
    quineBrowser.quineAppMount(document.getElementById("root"), {
        initialQuery: decodeURIComponent(window.location.hash.replace(/^#/, "")),
        isQueryBarVisible: urlParams.get("interactive") != "false",
        layout: urlParams.get("layout") || "graph",
        queriesOverWs: urlParams.get("wsQueries") != "false",
        queryHistoricalTime: parseMillis(urlParams.get("atTime")),
        onNetworkCreate: function(n) {
            network = n;
        },
        documentationUrl: "docs/openapi.json?relative=true",
        documentationV2Url: "api/v2/openapi.json",
        baseURI: derivedBaseURI,
        serverUrl: derivedBaseURI.replace(/\/$/, ""),
        isQuineOSS: true,
    });
};
