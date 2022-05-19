<header class="md-header" data-md-component="header">
  <nav class="md-header-nav md-grid">
    <div class="md-flex">
      <div class="md-flex__cell md-flex__cell--shrink">
        <a
          class="md-header-nav__button md-logo"
          href="https://quine.io/"
          $ if (page.properties.("logo.link.title")) $
            title="$page.properties.("logo.link.title")$"
          $ else $
            title="$page.properties.("page.home.title")$"
          $ endif $
        >
          <img src="$page.base$$page.properties.("material.logo")$" height="24">
        </a>
      </div>
      <div class="md-flex__cell md-flex__cell--shrink">
        <label class="md-icon md-icon--menu md-header-nav__button" for="__drawer"></label>
      </div>
      $header-bar()$
      $ if (page.properties.("material.search")) $
        <div class="md-flex__cell md-flex__cell--shrink">
          <label class="md-icon md-icon--search md-header-nav__button" for="__search"></label>
          $search()$
        </div>
      $ else $
        <form name="search" style="display: none">
          <input type="text" name="query">
        </form>
      $ endif $
      $ if (page.properties.("material.repo") || page.properties.("material.social")) $
        <div class="md-flex__cell md-flex__cell--shrink">
            $source()$
        </div>
      $ endif $
    </div>
  </nav>
</header>

