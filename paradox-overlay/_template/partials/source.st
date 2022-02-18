<div class="md-source">
$ if (page.properties.("material.repo")) $
<a href="$page.properties.("material.repo")$"
   title="Go to repository"
   data-md-source="$page.properties.("material.repo.type")$">
  <div class="md-source__icon">
    <i class="fa fa-$page.properties.("material.repo.type")$"></i>
  </div>
  $ if (page.properties.("material.repo.name")) $
  <div class="md-source__repository">
    $page.properties.("material.repo.name")$
  </div>
  $ endif $
</a>
$ endif $
$ if (page.properties.("material.social")) $
<a href="$page.properties.("material.social")$"
   title="Go to community channel"
   data-md-source="$page.properties.("material.social.type")$">
  <div class="md-source__icon">
    <i class="fa fa-$page.properties.("material.social.type")$"></i>
  </div>
  $ if (page.properties.("material.social.name")) $
  <div class="md-source__repository">
    $page.properties.("material.social.name")$
  </div>
  $ endif $
</a>
$ endif $
</div>
