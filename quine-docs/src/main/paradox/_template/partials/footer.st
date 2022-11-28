<footer class="md-footer">
    $ if (page.next.html || page.prev.html) $
      <div class="md-footer-nav">
        <nav class="md-footer-nav__inner md-grid">
          $ if (page.prev.html) $
            <a href="$page.prev.href$" title="$page.prev.title$" class="md-flex md-footer-nav__link md-footer-nav__link--prev" rel="prev">
              <div class="md-flex__cell md-flex__cell--shrink">
                <i class="md-icon md-icon--arrow-back md-footer-nav__button"></i>
              </div>
              <div class="md-flex__cell md-flex__cell--stretch md-footer-nav__title">
                <span class="md-flex__ellipsis">
                  <span class="md-footer-nav__direction">
                    Previous
                  </span>
                  $page.prev.title$
                </span>
              </div>
            </a>
          $ endif $
          $ if (page.next.html) $
            <a href="$page.next.href$" title="$page.next.title$" class="md-flex md-footer-nav__link md-footer-nav__link--next" rel="next">
              <div class="md-flex__cell md-flex__cell--stretch md-footer-nav__title">
                <span class="md-flex__ellipsis">
                  <span class="md-footer-nav__direction">
                    Next
                  </span>
                  $page.next.title$
                </span>
              </div>
              <div class="md-flex__cell md-flex__cell--shrink">
                <i class="md-icon md-icon--arrow-forward md-footer-nav__button"></i>
              </div>
            </a>
          $ endif $
        </nav>
      </div>
    $endif$
</footer>
<div class="footer wf-section">
  <div class="container">
    <div class="w-layout-grid footer-columns">
      <ul id="w-node-_7c1f1a51-ff8c-c157-7617-ee9e8e70ec1f-8e70ec0c" role="list" class="footer-list w-list-unstyled">
        <li class="footer-list-item">
          <h3 class="h3-footer"><strong>Docs</strong></h3>
        </li>
        <li class="footer-list-item">
          <a href="https://docs.quine.io/core-concepts/architecture.html" class="footer-link">Architecture</a>
        </li>
        <li class="footer-list-item">
          <a href="https://docs.quine.io/core-concepts/streaming-systems.html" class="footer-link">Quine and Event Streams</a>
        </li>
        <li class="footer-list-item">
          <a href="https://docs.quine.io/getting-started/standing-queries-tutorial.html" class="footer-link">Standing Query Overview</a>
        </li>
        <li class="footer-list-item">
          <a href="https://docs.quine.io/getting-started/ingest-streams-tutorial.html" class="footer-link">Ingest Streams Overview</a>
        </li>
        <li class="footer-list-item">
          <a href="https://docs.quine.io/getting-started/exploration-ui.html" class="footer-link">Exploration UI</a>
        </li>
      </ul>
      <ul id="w-node-_7c1f1a51-ff8c-c157-7617-ee9e8e70ec3f-8e70ec0c" role="list" class="footer-list w-list-unstyled">
        <li class="footer-list-item">
          <h3 class="h3-footer"><strong>Recipes</strong></h3>
        </li>
        <li class="footer-list-item">
          <a href="https://docs.quine.io/getting-started/recipes-tutorial.html" class="footer-link">Recipes Overview</a>
        </li>
        <li class="footer-list-item">
          <a href="https://quine.io/recipes/ethereum-tag-propagation" class="footer-link">Ethereum</a>
        </li>
        <li class="footer-list-item">
          <a href="https://quine.io/recipes/cdn-cache-efficiency-by-segment" class="footer-link">CDN Cache Efficiency</a>
        </li>
        <li class="footer-list-item">
          <a href="https://quine.io/recipes/apache-log-analytics" class="footer-link">Apache Log Analytics</a>
        </li>
      </ul>
      <ul id="w-node-_7c1f1a51-ff8c-c157-7617-ee9e8e70ec2f-8e70ec0c" role="list" class="footer-list w-list-unstyled">
        <li class="footer-list-item">
          <h3 class="h3-footer"><strong>Setup</strong></h3>
        </li>
        <li class="footer-list-item">
          <a href="https://quine.io/download" class="footer-link">Download</a>
        </li>
        <li class="footer-list-item">
          <a href="https://docs.quine.io/getting-started/quick-start.html" class="footer-link">Getting Started</a>
        </li>
      </ul>
      <ul id="w-node-_7c1f1a51-ff8c-c157-7617-ee9e8e70ec4f-8e70ec0c" role="list" class="footer-list w-list-unstyled">
        <li class="footer-list-item">
          <h3 class="h3-footer"><strong>Community</strong></h3>
        </li>
        <li class="footer-list-item">
          <a href="https://github.com/thatdot/quine" target="_blank" class="footer-link">GitHub</a>
        </li>
        <li class="footer-list-item">
          <a data-label="quine slack footer" data-ctegory="Slack Signup" data-action="Link click" href="https://that.re/quine-slack" target="_blank" class="footer-link">Slack</a>
        </li>
      </ul>
    </div>
    <div class="w-layout-grid footer-legal-grid">
      <div id="w-node-_7c1f1a51-ff8c-c157-7617-ee9e8e70ec6b-8e70ec0c" class="copyright-text">© 2022 thatDot, Inc.</div>
      <a href="index.html" aria-current="page" class="footer-logo-link w-inline-block w--current"><img src="https://uploads-ssl.webflow.com/61d5ee2c68a4d5d61588037b/621eca801bbf60a31b20a6b4_Quine%20Logo%20-%20White.svg" loading="lazy" width="257.5" alt="The Quine logo - a Q with an arrow, like a graph edge, as the stroke in the Q. White version."></a>
      <div class="legal-links">
        <a href="code-of-conduct.html" class="footer-link">Code of Conduct</a>
      </div>
    </div>
  </div>
</div>
