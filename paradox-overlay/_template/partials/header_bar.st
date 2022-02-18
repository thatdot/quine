<div class="md-flex__cell md-flex__cell--stretch">
  <div class="md-flex__ellipsis md-header-nav__title" data-md-component="title">
    $ if (page.home.active) $
      $page.home.title$
    $ else $
      <span class="md-header-nav__topic">
        $page.home.title$
      </span>
      <span class="md-header-nav__topic">
        $page.title$
      </span>
    $ endif $
  </div>
</div>
