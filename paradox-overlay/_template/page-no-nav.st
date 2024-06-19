<!DOCTYPE html>
<html lang="$page.properties.("material.language"); null="en"$" class="no-js">
  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width,initial-scale=1">
    <meta http-equiv="x-ua-compatible" content="ie=edge">
    <meta name="generator" content="Paradox, paradox-material-theme=$page.properties.("material.theme.version")$, mkdocs-material=3.0.3">

    $partials/language()$

    $ if (page.properties.("description")) $
      <meta name="description" content="$page.properties.("description")$">
    $ elseif (page.properties.("project.description")) $
      <meta name="description" content="$page.properties.("project.description")$">
    $ endif $
    $ if (page.properties.("material.canonical.url")) $
      <link rel="canonical" href="$page.properties.("material.canonical.url")$">
    $ endif $
    $ if (page.properties.("author")) $
      <meta name="author" content="$page.properties.("author")$">
    $ elseif (page.properties.("material.author")) $
      <meta name="author" content="$page.properties.("site.author")$">
    $ endif $
    <link rel="shortcut icon" href="$page.base$$page.properties.("material.favicon")$">
    $ if (page.properties.("title")) $
      <title>$page.properties.("title")$</title>
    $ elseif (page.title) $
      <title>$page.title$$ if (!page.home.active) $ · $page.home.title$$ endif $</title>
    $ else $
      <title>$page.home.title$</title>
    $ endif $
    $ if (page.properties.("material.custom.stylesheet")) $
      <link rel="stylesheet" href="$page.base$$page.properties.("material.custom.stylesheet")$">
    $ endif $
    <link rel="stylesheet" href="$page.base$assets/stylesheets/application.451f80e5.css">
    $ if (page.properties.("material.color.primary")) $
      <link rel="stylesheet" href="$page.base$assets/stylesheets/application-palette.22915126.css">
      $ if (page.properties.("material.color.primary.theme")) $
        <meta name="theme-color" content="$page.properties.("material.color.primary.theme")$" />
      $ endif $
    $ endif $
    <link rel="stylesheet" href="$page.base$lib/material__tabs/dist/mdc.tabs.min.css">
    <link rel="stylesheet" href="$page.base$lib/prettify/prettify.css">
    <script src="$page.base$assets/javascripts/modernizr.1aa3b519.js"></script>
    $ if (page.properties.("material.font.text")) $
      <link rel="stylesheet" href="https://fonts.googleapis.com/css?family=$page.properties.("material.font.text.url")$:300,400,400i,700|$page.properties.("material.font.code.url")$">
      <style>
        body,input{font-family:"$page.properties.("material.font.text")$","Helvetica Neue",Helvetica,Arial,sans-serif}
        code,kbd,pre{font-family:"$page.properties.("material.font.code")$","Courier New",Courier,monospace}
      </style>
    $ endif $
    <link rel="stylesheet" href="$page.base$assets/fonts/font-awesome.css">
    <link rel="stylesheet" href="$page.base$assets/fonts/material-icons.css">
    <link rel="stylesheet" href="$page.base$assets/stylesheets/paradox-material-theme.css">
  </head>
  <body
    $ if (page.properties.("material.color.primary")) $
      data-md-color-primary="$page.properties.("material.color.primary")$"
    $ endif $
    $ if (page.properties.("material.color.accent")) $
      data-md-color-accent="$page.properties.("material.color.accent")$"
    $ endif $
  >
    <input class="md-toggle" data-md-toggle="drawer" type="checkbox" id="__drawer" autocomplete="off">
    <input class="md-toggle" data-md-toggle="search" type="checkbox" id="__search" autocomplete="off">
    <label class="md-overlay" data-md-component="overlay" for="__drawer"></label>
    $! FIXME: Skip to content (0e1850280a1c25d3bc697419288d97582dacbbc3) !$
    $partials/header()$
    <div class="md-container">
      $ if (page.properties.("material.hero")) $
        $partials/hero()$
      $ endif $
      <main class="md-main">
        <div class="md-main__inner md-grid" data-md-component="container">
          <div class="md-sidebar md-sidebar--primary" data-md-component="navigation" style="display:none;">
            <div class="md-sidebar__scrollwrap">
              <div class="md-sidebar__inner">
                $partials/nav()$
              </div>
            </div>
          </div>
            <article class="md-content__inner md-typeset">
              <div class="md-content__searchable">
                $page.content$
              </div>
              $ if (page.source_url && page.property_is.("github_edit_link").("true")) $
                <div>
                  <a href="$page.source_url$" title="Edit this page" class="md-source-file md-edit">
                    Edit this page
                  </a>
                </div>
              $ endif $
              <div class="print-only">
                <span class="md-source-file md-version">
                  $page.properties.("project.version.short")$
                </span>
              </div>
            </article>
        </div>
      </main>
      $partials/footer()$
    </div>
    <script src="$page.base$assets/javascripts/application.583bbe55.js"></script>
    <script src="$page.base$assets/javascripts/paradox-material-theme.js"></script>
    $!

      The ending "." enables search to work without providing the site URL, since the
      theme JavaScript fetches the index by concatenating the base URL to an absolute
      path: "/search/search_index.json".

      It originally was "./" but this breaks previewSite because it leads to a double
      slash when fetching the index, i.e. ".//search/...". To complete the workaround
      all locations in the search index now start with a "/" so the JavaScript can
      safely do: `url.base + doc.location`.

    !$
    <script>app.initialize({version:"0.17",url:{base:"$page.base$."}})</script>
    <script type="text/javascript" src="$page.base$lib/prettify/prettify.js"></script>
    <script type="text/javascript" src="$page.base$lib/prettify/lang-scala.js"></script>
    <script type="text/javascript" src="$page.base$assets/javascripts/lang-cypher.js"></script>
    <script type="text/javascript" src="$page.base$assets/javascripts/lang-yaml.js"></script>
    <script type="text/javascript" src="$page.base$assets/javascripts/lang-sql.js"></script>
    $ if (page.properties.("material.custom.javascript")) $
      <script src="$page.base$$page.properties.("material.custom.javascript")$"></script>
    $ endif $
    <script type="text/javascript">

      // Work around https://github.com/lightbend/paradox/issues/454
      document.addEventListener("DOMContentLoaded", function(event) {
        if (window.prettyPrint) {
          var codeElems = document.body.getElementsByTagName("code");
          for (var i = 0; i < codeElems.length; ++i) {
             var codeElem = codeElems[i];
             var languageClassesToLift = [];
             codeElem.classList.forEach(function (className) {
               if (className.startsWith("language-")) languageClassesToLift.push(className);
             });
             if (codeElem.parentElement.classList.contains("prettyprint")) {
               DOMTokenList.prototype.add.apply(
                 codeElem.parentElement.classList,
                 languageClassesToLift
               );
             }
          }
          prettyPrint();
        }
      });
    </script>
    $ if (page.properties.("material.google.analytics")) $
      $partials/integrations/analytics()$
    $ endif $
  </body>
</html>
