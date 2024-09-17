<nav class="md-nav md-nav--primary" data-md-level="0" style="visibility: hidden">
  <label class="md-nav__title md-nav__title--site" for="drawer">
    <a href="$page.home.href$" title="$page.home.title$" class="md-nav__button md-logo">
    <span class="md-nav__button md-logo">
      <img src="$page.base$$page.properties.("material.logo")$" height="24">
    </a>
    <a href="$page.home.href$" title="$page.home.title$">
      $page.home.title$
    </a>
  </label>
  $ if (page.properties.("material.repo")) $
    <div class="md-nav__source">
      $source()$
    </div>
  $ endif $
  $page.groups$
  $page.navigation$
  $toc()$
</nav>
<ul style="display: none">
  <li class="md-nav__item md-version" id="project.version">
    <label class="md-nav__link" for="__version">
      <div>Streaming Graph</div>
      <i class="md-icon" title="Streaming Graph Version">label_outline</i> $page.properties.("project.version.short")$
    </label>
    <label class="md-nav__link" for="__noveltyVersion">
      <div>Novelty</div>
      <i class="md-icon" title="Novelty Version">label_outline</i> $page.properties.("project.noveltyVersion.short")$
    </label>
  </li>
</ul>
