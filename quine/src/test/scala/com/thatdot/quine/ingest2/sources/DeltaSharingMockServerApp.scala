package com.thatdot.quine.ingest2.sources

import scala.concurrent.Await
import scala.concurrent.duration._

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.http.scaladsl.server.Route

/** Standalone Delta Sharing mock server for manual testing.
  *
  * Run with:
  *   sbt 'quine/Test/runMain com.thatdot.quine.ingest2.sources.DeltaSharingMockServerApp'
  *
  * The server runs on port 18080 and also exposes control endpoints on port 18081
  * for pushing test data. It stays running until killed (Ctrl+C).
  *
  * === Setup ===
  *
  * 1. Start this mock server (terminal 1)
  * 2. Start Quine: sbt 'quine/run' (terminal 2)
  * 3. Create the ingest (terminal 3):
  *
  *   curl -X POST 'http://localhost:8080/api/v2/graph/quine/ingests' \
  *     -H 'Content-Type: application/json' \
  *     -d '{
  *       "name": "delta-test",
  *       "source": {
  *         "type": "DeltaSharingCdf",
  *         "endpoint": "http://127.0.0.1:18080",
  *         "auth": {
  *           "type": "BearerToken",
  *           "bearerToken": "test-bearer-token-12345"
  *         },
  *         "shareName": "test-share",
  *         "schemaName": "test-schema",
  *         "tableName": "user_delta",
  *         "snapshotOnFirstRun": true,
  *         "pollIntervalMs": 5000
  *       },
  *       "query": "WITH $that AS row WHERE coalesce(row._change_type, 'insert') <> 'update_preimage' MATCH (n) WHERE id(n) = idFrom('user', row.id) CALL { WITH n, row WITH n, row WHERE row._change_type = 'delete' DETACH DELETE n } CALL { WITH n, row WITH n, row WHERE coalesce(row._change_type, 'insert') IN ['insert', 'update_postimage'] SET n = row, n:User }",
  *       "parameter": "that"
  *     }'
  *
  * === Push test data via HTTP (terminal 3) ===
  *
  *   # Push 3 inserts:
  *   curl -X POST http://localhost:18081/push/inserts
  *
  *   # Push updates (modifies Alice and Bob):
  *   curl -X POST http://localhost:18081/push/updates
  *
  *   # Push a delete (removes Carol):
  *   curl -X POST http://localhost:18081/push/deletes
  *
  *   # Check version:
  *   curl http://localhost:18081/status
  *
  * === Query Quine to see the data (terminal 3) ===
  *
  *   curl 'http://localhost:8080/api/v1/query/cypher' \
  *     -H 'Content-Type: text/plain' \
  *     -d 'MATCH (n:User) RETURN n.firstname, n.lastname, n.email LIMIT 20'
  */
object DeltaSharingMockServerApp extends App {

  implicit val system: ActorSystem = ActorSystem("delta-sharing-mock")

  val mockServer = new DeltaSharingMockServer(host = "127.0.0.1", port = 18080)
  mockServer.start()

  // Pre-load snapshot data
  mockServer.setSnapshot(
    Seq(
      UserRow(1, "Alice", "Smith", "alice@example.com"),
      UserRow(2, "Bob", "Jones", "bob@example.com"),
      UserRow(3, "Carol", "Davis", "carol@example.com"),
      UserRow(4, "Dave", "Wilson", "dave@example.com"),
      UserRow(5, "Eve", "Brown", "eve@example.com"),
    ),
  )

  // Control server on a separate port for pushing test data
  var nextId = 100L

  import org.apache.pekko.http.scaladsl.Http
  import org.apache.pekko.http.scaladsl.server.Directives._

  val controlRoutes: Route =
    concat(
      path("push" / "inserts") {
        post {
          val rows = Seq(
            UserRow(nextId, s"User$nextId", s"Last$nextId", s"user$nextId@example.com"),
            UserRow(nextId + 1, s"User${nextId + 1}", s"Last${nextId + 1}", s"user${nextId + 1}@example.com"),
            UserRow(nextId + 2, s"User${nextId + 2}", s"Last${nextId + 2}", s"user${nextId + 2}@example.com"),
          )
          nextId += 3
          val v = mockServer.pushInserts(rows)
          val names = rows.map(r => s"${r.firstname} ${r.lastname}").mkString(", ")
          complete(s"Pushed 3 INSERT events at version $v: $names\n")
        }
      },
      path("push" / "updates") {
        post {
          val v = mockServer.pushUpdates(
            Seq(
              UserRow(1, "Alice", "Smith-Updated", "alice.updated@example.com"),
              UserRow(2, "Bob", "Jones-Updated", "bob.updated@example.com"),
            ),
          )
          complete(s"Pushed 2 UPDATE events at version $v: Alice Smith-Updated, Bob Jones-Updated\n")
        }
      },
      path("push" / "deletes") {
        post {
          val v = mockServer.pushDeletes(
            Seq(
              UserRow(3, "Carol", "Davis", "carol@example.com"),
            ),
          )
          complete(s"Pushed 1 DELETE event at version $v: Carol Davis\n")
        }
      },
      path("status") {
        get {
          complete(s"Mock Delta Sharing server running. Next insert ID: $nextId\n")
        }
      },
    )

  val controlBinding: Http.ServerBinding = Await.result(
    Http().newServerAt("127.0.0.1", 18081).bind(controlRoutes),
    5.seconds,
  )

  println()
  println("=== Delta Sharing Mock Server ===")
  println("Delta Sharing protocol: http://127.0.0.1:18080")
  println("Control API:            http://127.0.0.1:18081")
  println("Bearer token:           test-bearer-token-12345")
  println()
  println("Snapshot loaded with 5 users (Alice, Bob, Carol, Dave, Eve)")
  println()
  println("Push test data via HTTP:")
  println("  curl -X POST http://localhost:18081/push/inserts")
  println("  curl -X POST http://localhost:18081/push/updates")
  println("  curl -X POST http://localhost:18081/push/deletes")
  println("  curl http://localhost:18081/status")
  println()
  println("Press Ctrl+C to stop.")

  // Keep alive until killed
  Runtime.getRuntime.addShutdownHook(new Thread(() => {
    println("\nShutting down...")
    Await.result(controlBinding.unbind(), 5.seconds)
    mockServer.stop()
    mockServer.cleanup()
    Await.result(system.terminate(), 5.seconds)
    println("Done.")
  }))

  // Block forever
  Thread.currentThread().join()
}
