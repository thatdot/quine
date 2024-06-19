/** Prettify syntax definition for SQL */
PR.registerLangHandler(
  PR.createSimpleLexer(
    [
      // Whitespace
      [PR.PR_PLAIN, /^[\t\n\r \xA0]+/, null, "\t\n\r \xA0"],

      // A double or single quoted string.
      [PR.PR_STRING, /^(?:"(?:[^\"\\]|\\.)*"|'(?:[^\'\\]|\\.)*')/, null, "\"'"],
    ],
    [
      // Comments (single-line and multi-line)
      [PR.PR_COMMENT, /^--(?:.*)/],
      [PR.PR_COMMENT, /^\/\*(?:[\s\S]*?)\*\//],

      // Keywords
      [
        PR.PR_KEYWORD,
        /^(?:select|insert|update|delete|from|where|join|inner|outer|left|right|on|as|and|or|not|in|is|null|like|between|distinct|group|by|order|having|limit|offset|union|all|any|some|into|values|create|table|primary|key|foreign|references|alter|drop|add|column|index|constraint|default|check|unique|if|exists|else|begin|commit|rollback|transaction|grant|revoke|view|procedure|function|trigger|declare|set|show|describe|use|database|schema|with|case|when|then|end)\b/i,
      ],

      // Functions
      [
        PR.PR_KEYWORD,
        /^(?:count|min|max|avg|sum|upper|lower|substring|length|trim|replace|abs|round|sqrt|date|time|timestamp|now|coalesce|ifnull|nullif)\b/i,
      ],

      // Number literals
      [PR.PR_LITERAL, /^(?:-?(?:0|[1-9]\d*)(?:\.\d+)?(?:[eE][+\-]?\d+)?)/],

      // Punctuation
      [PR.PR_PUNCTUATION, /^[(){}\[\],.;*<>!=+\/-]/],

      // Plain text (identifiers)
      [PR.PR_PLAIN, /^[a-zA-Z_][\w$]*/],
    ]
  ),
  ["sql"]
);
