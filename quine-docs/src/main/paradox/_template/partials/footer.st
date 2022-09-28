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
        <ul id="w-node-_7c1f1a51-ff8c-c157-7617-ee9e8e70ec0f-8e70ec0c" role="list" class="footer-list w-list-unstyled">
          <li class="footer-list-item">
            <h3 class="h3-footer"><strong>About</strong></h3>
          </li>
          <li class="footer-list-item">
            <a href="https://quine.io/about" class="footer-link">What is Quine?</a>
          </li>
        </ul>
        <ul id="w-node-_7c1f1a51-ff8c-c157-7617-ee9e8e70ec1f-8e70ec0c" role="list" class="footer-list w-list-unstyled">
          <li class="footer-list-item">
            <h3 class="h3-footer"><strong>Docs</strong></h3>
          </li>
          <li class="footer-list-item">
            <a href="/core-concepts/architecture.html" class="footer-link">Architecture</a>
          </li>
          <li class="footer-list-item">
            <a href="/getting-started/standing-queries-tutorial.html" class="footer-link">Standing Query Tutorial</a>
          </li>
          <li class="footer-list-item">
            <a href="/getting-started/ingest-streams-tutorial.html" class="footer-link">Ingest Streams Tutorial</a>
          </li>
          <li class="footer-list-item">
            <a href="/getting-started/exploration-ui.html" class="footer-link">Exploration UI</a>
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
            <a href="/getting-started/index.html" class="footer-link">Getting Started</a>
          </li>
        </ul>
        <div id="w-node-_972d8002-2611-e260-929c-6645d914b4d5-8e70ec0c" class="div-block-22"></div>
        <ul id="w-node-_7c1f1a51-ff8c-c157-7617-ee9e8e70ec3f-8e70ec0c" role="list" class="footer-list w-list-unstyled">
          <li class="footer-list-item">
            <h3 class="h3-footer"><strong>Recipes</strong></h3>
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
          <li class="footer-list-item">
            <a href="/community/writing-a-recipe.html" class="footer-link">Writing a Recipe</a>
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
            <a data-ctegory="Slack Signup" data-action="Link click" data-label="quine slack footer" href="https://quine-io.slack.com/join/shared_invite/zt-171x7z72f-a6Zqhrot3C7cwsR3xi5suA#/shared-invite/email" target="_blank" class="footer-link">Slack</a>
          </li>
          <li class="footer-list-item">
            <a href="https://quine.io/code-of-conduct" class="footer-link">Code of Conduct</a>
          </li>
        </ul>
      </div>
      <div class="w-layout-grid footer-legal-grid">
        <div class="copyright-text"></div>
        <a href="https://quine.io/" aria-current="page" class="w-inline-block w--current"><img src="/assets/images/Quine-Logo-White.svg" loading="lazy" width="257.5" alt=""></a>
      </div>
    </div>
</div>