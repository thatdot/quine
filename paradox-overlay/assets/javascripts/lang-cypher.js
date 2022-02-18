/** Prettify syntax definition for Cypher (not copied from anywhere) */
PR.registerLangHandler(
  PR.createSimpleLexer(
    [
      // Whitespace
      [ PR.PR_PLAIN, /^[\t\n\r \xA0]+/, null, '\t\n\r \xA0' ],

      // A double or single quoted, possibly multi-line, string.
      [ PR.PR_STRING, /^(?:"(?:[^\"\\]|\\.)*"|'(?:[^\'\\]|\\.)*')/, null, '"\'' ],

      // Back-ticked identifiers
      [ PR.PR_LITERAL, /^`(?:[^\r\n\\`]|\\.)*`?/, null, '`' ],
    ],
    [
      // Boolean literal
      [ PR.PR_LITERAL, /^(?:true|false|null)\b/i ],

      // Keywords
      [ PR.PR_KEYWORD, /^(?:all|any|as|asc|ascending|assert|call|case|create|create\s+index|create\s+unique|delete|desc|descending|distinct|drop\s+constraint\s+on|drop\s+index\s+on|end|ends\s+with|fieldterminator|foreach|in|is\s+node\s+key|is\s+null|is\s+unique|limit|load\s+csv\s+from|match|merge|none|not|null|on\s+match|on\s+create|optional\s+match|order\s+by|remove|return|set|skip|single|start|starts\s+with|then|union|union\s+all|unwind|using\s+periodic\s+commit|yield|where|when|with|and|or|not)/i ],

      // Number literal
      [ PR.PR_LITERAL, /^(?:(?:0(?:[0-7]+|X[0-9A-F]+))L?|(?:(?:0|[1-9][0-9]*)(?:(?:\.[0-9]+)?(?:E[+\-]?[0-9]+)?F?|L?))|\\.[0-9]+(?:E[+\-]?[0-9]+)?F?)/i ],

      // Comment (not multi-line)
      [ PR.PR_COMMENT, /^\/(?:\/.*|\*(?:\/|\**[^*/])*(?:\*+\/?)?)/ ],

      // Various operators
      [ PR.PR_PUNCTUATION, /^(?:-\[|<-\[|\]-|\]->|-->|<--)/ ],
      [ PR.PR_PUNCTUATION, /^(?:<|>|<>|=|<=|=>|\(|\)|\[|\]|\||:|,|;)/ ],
      [ PR.PR_PUNCTUATION, /^[.*{}]/ ],

      [ PR.PR_PLAIN, /^[$a-zA-Z_][\w$]*/ ],
    ]
  ),
  ['cypher']
);
