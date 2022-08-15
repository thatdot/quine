---
description: Quine streaming graph ingest events from standard in
---
# Standard In

Quine fully supports reading from Standard In. Together with writing @ref:[Standing Queries to Standard Out](../standing-query-outputs.md), Quine is a powerful tool for any command-line data processing task.

The following is a simple @ref[Recipe](../../core-concepts/about-recipes.md) that ingests each line of input from Standard In as a node in the graph. It also uses a Standing Query to write every to Standard Out:

@@snip [pipe.yaml]($quine$/recipes/pipe.yaml)

To run this Recipe, pipe data from other program into Quine. This example uses the Unix `find` program as a data source:

```
❯ find /dev | java -jar quine.jar -r pipe
Graph is ready
Running Recipe Pipe
Running Standing Query STANDING-1
Running Ingest Stream INGEST-1
2022-02-22 15:33:21,995 Standing query `output-1` match: {"meta":{"isPositiveMatch":true,"resultId":"54754813-574b-b86e-5e8e-6968ef4ce2e5"},"data":{"line":"/dev/ptyu1"}}
2022-02-22 15:33:21,996 Standing query `output-1` match: {"meta":{"isPositiveMatch":true,"resultId":"e2783bf4-3328-1365-0fed-d9a9ab12489c"},"data":{"line":"/dev/ptyu4"}}
2022-02-22 15:33:21,997 Standing query `output-1` match: {"meta":{"isPositiveMatch":true,"resultId":"845cac7b-7994-baf8-982f-06adc4a818ff"},"data":{"line":"/dev/ptytf"}}
2022-02-22 15:33:21,998 Standing query `output-1` match: {"meta":{"isPositiveMatch":true,"resultId":"ccdb9fc6-969e-16a0-2a10-8a0e2b594ec1"},"data":{"line":"/dev/ttyu8"}}
...

Quine app web server available at http://0.0.0.0:8080
INGEST-1 status is completed and ingested 360

 | => STANDING-1 count 360
```
