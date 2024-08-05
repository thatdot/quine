## See For Yourself

<div class="timeline-wrapper">
    <div class="process-row">
        <div class="process-col-right">
            <div class="process-text-block">
                <h3 class="heading-13">Launch the Quine app</h3>
                <p class="praragraph -18px">Download the code from Github or click on a Quine Recipe to install a copy of the code pre-configured to deliver a given use case.</p>
            </div>
            <div class="process-icon-wrapper icon1">
                <p class="paragraph-7">1</p>
            </div>
            <div class="process-code-block">
                <div data-scroll="mid" class="code-wrapper-tab-2 process">
                    <div class="html-embed-2 w-embed">
                        <pre class="prettyprint">
                            <code class="language-bash">❯ java -jar quine.jar
Cluster is ready!
Connect web server available at http://0.0.0.0:8080</code></pre>
                    </div>
                </div>
            </div>
        </div>
    </div>
    <div class="process-row">
        <div class="process-col-right">
            <div class="process-text-block">
                <h3 class="heading-14">Ingest Data, Build Your Graph</h3>
                <p class="praragraph -18px">A curl against the HTTP API sets the ingest source, format and mapping of data elements to our streaming graph. The graph data model mitigates out-of-order data delivery issues.</p>
            </div>
            <div class="process-icon-wrapper icon2">
                <p class="paragraph-7">2</p>
            </div>
            <div class="process-code-block">
                <div data-scroll="mid" class="code-wrapper-tab-2 process">
                    <div class="html-embed-2 w-embed">
                        <pre class="prettyprint">
                            <code class="language-bash">❯ curl 'http://localhost:8080/api/v1/ingest/data-source' \
  -H 'accept: */*' \
  -H 'Content-Type: application/json' \
  -d '{
    "type": "FileIngest",
    "path": "/directory/data-source",
    "format": {
      "type": "CypherLine",
      "query": "CREATE ($that)"
    }
  }'</code></pre>
                    </div>
                </div>
            </div>
        </div>
    </div>
    <div class="process-row">
        <div class="process-col-right">
            <div class="process-text-block">
                <h3 class="heading-15">Define Standing Queries</h3>
                <p class="praragraph -18px">A 2nd curl to the HTTP API sets the standing query pattern to identify, and actions to be triggered upon a match. Standing queries are distributed across the streaming graph and continuously executed as new data in ingested. Partial matches are stored in the data incrementally enabling high-throughput and real-time operations.</p>
            </div>
            <div class="process-code-block">
                <div data-scroll="mid" class="code-wrapper-tab-2 process">
                    <div class="html-embed-2 w-embed">
                        <pre class="prettyprint">
                            <code class="language-bash">❯ curl 'http://localhost:8080/api/v1/query/standing/data-output \
  -H 'accept: */*' \
  -H 'Content-Type: application/json' \
  -d '{
    "pattern": {
      "query": "MATCH (n) RETURN id(n)",
      "type": "Cypher"
    },
    "outputs": {
      "output-1": {
        "type": "File",
        "path": "/directory/data-result"
      }
    }
  }'</code></pre>
                    </div>
                </div>
            </div>
            <div class="process-icon-wrapper icon3">
                <p class="paragraph-7">3</p>
            </div>
        </div>
    </div>
    <div class="process-row last-row">
        <div class="process-col-right">
            <div class="process-text-block">
                <h3 class="heading-16">Explore Your Data</h3>
                <p class="praragraph -18px">Directing a browser to localhost port 8080 brings you to the Data Exploration UI, where you can explore the graph, interact with the REST API or view operational metrics.</p>
            </div>
            <div class="process-icon-wrapper icon4">
                <p class="paragraph-7">4</p>
            </div>
            <div class="process-code-block">
                <img src="https://assets.website-files.com/placeholder.svg" loading="lazy" alt=""/>
                <img src="https://assets.website-files.com/61d5ee2c68a4d5d61588037b/61fd89282dd1ce3c75e2709e-data-exploration-ui.png" loading="lazy" sizes="100vw" srcset="https://assets.website-files.com/61d5ee2c68a4d5d61588037b/61fd89282dd1ce3c75e2709e-data-exploration-ui-p-500.png 500w, https://assets.website-files.com/61d5ee2c68a4d5d61588037b/61fd89282dd1ce3c75e2709e-data-exploration-ui-p-800.png 800w, https://assets.website-files.com/61d5ee2c68a4d5d61588037b/61fd89282dd1ce3c75e2709e-data-exploration-ui-p-1080.png 1080w, https://assets.website-files.com/61d5ee2c68a4d5d61588037b/61fd89282dd1ce3c75e2709e-data-exploration-ui-p-1600.png 1600w, https://assets.website-files.com/61d5ee2c68a4d5d61588037b/61fd89282dd1ce3c75e2709e-data-exploration-ui.png 2739w" alt="" class="image-28"/>
            </div>
        </div>
        <div data-w-id="363d43ef-a244-c384-925e-6cebfbbf43de" style="opacity:0" class="process-row"></div>
    </div>
</div>
