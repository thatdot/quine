// Development startup script for Quine UI
// This is based on quine-ui-startup.js but configured for local development

// Given some value meant to represent time, return either integer milliseconds or undefined
function parseMillis(atTime) {
  if (atTime === undefined || atTime === null) return undefined;

  // Input is a string number
  var isPositiveNumberString =
    typeof atTime === "string" && atTime.match(/^\d+$/);
  if (isPositiveNumberString) return Number.parseInt(atTime);

  // Try to parse a date
  var dateStringMillis = Date.parse(atTime);
  if (!isNaN(dateStringMillis)) return dateStringMillis;

  return undefined;
}

var network = undefined;
var urlParams = new URLSearchParams(window.location.search);

// In dev mode, default to v2 API
var defaultQueriesOverV2Api = true;

window.onload = function () {
  console.log("[Dev] Mounting Quine UI...");
  console.log(
    "[Dev] quineBrowser available:",
    typeof quineBrowser !== "undefined"
  );

  if (typeof quineBrowser === "undefined") {
    console.error(
      "[Dev] quineBrowser is not defined! Make sure the Scala.js bundle loaded correctly."
    );
    document.getElementById("root").innerHTML = `
      <div style="padding: 20px; color: red; font-family: sans-serif;">
        <h1>Error: Scala.js bundle not loaded</h1>
        <p>The <code>quineBrowser</code> object is not available.</p>
        <p>Make sure you've run <code>sbt "project quine-browser" fastOptJS::webpack</code></p>
        <p>Check the console for errors.</p>
      </div>
    `;
    return;
  }

  quineBrowser.quineAppMount(document.getElementById("root"), {
    initialQuery: decodeURIComponent(window.location.hash.replace(/^#/, "")),
    isQueryBarVisible: urlParams.get("interactive") != "false",
    layout: urlParams.get("layout") || "graph",
    queriesOverWs: urlParams.get("wsQueries") != "false",
    queriesOverV2Api: urlParams.get("v2Api") !== null ? urlParams.get("v2Api") != "false" : defaultQueriesOverV2Api,
    queryHistoricalTime: parseMillis(urlParams.get("atTime")),
    onNetworkCreate: function (n) {
      network = n;
      console.log("[Dev] Network created:", n);
    },
    documentationUrl: "/docs/openapi.json",
    documentationV2Url: "/api/v2/openapi.json",
    baseURI: "",
    serverUrl: "",
  });

  console.log("[Dev] Quine UI mounted successfully!");
  console.log(
    "[Dev] Try navigating to: http://localhost:5173#MATCH%20(n)%20RETURN%20n%20LIMIT%2010"
  );
};

// Log any errors
window.onerror = function (message, source, lineno, colno, error) {
  console.error("[Dev] Runtime error:", {
    message,
    source,
    lineno,
    colno,
    error,
  });
};
