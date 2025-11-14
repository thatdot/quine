package com.thatdot.quine.app.config

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

class ClickHouseSecurityTest extends AnyFunSpec with Matchers {

  // Helper to set environment variables for testing using reflection
  def setEnv(key: String, value: String): Unit = {
    val env = System.getenv()
    val cl = env.getClass
    val field = cl.getDeclaredField("m")
    field.setAccessible(true)
    val writableEnv = field.get(env).asInstanceOf[java.util.Map[String, String]]
    val _ = writableEnv.put(key, value)
  }

  // Helper to remove environment variables for testing
  def unsetEnv(key: String): Unit = {
    val env = System.getenv()
    val cl = env.getClass
    val field = cl.getDeclaredField("m")
    field.setAccessible(true)
    val writableEnv = field.get(env).asInstanceOf[java.util.Map[String, String]]
    val _ = writableEnv.remove(key)
  }

  describe("ClickHouse configuration security") {
    it("should throw SecurityException when env vars are not set") {
      // Ensure the env vars are not set
      unsetEnv("CLICKHOUSE_USER")
      unsetEnv("CLICKHOUSE_PASSWORD")

      val exception = intercept[SecurityException] {
        PersistenceAgentType.ClickHouse(
          url = "http://localhost:8123",
          database = "quine",
        )
      }

      exception.getMessage should (include("CLICKHOUSE_USER") or include("CLICKHOUSE_PASSWORD"))
      exception.getMessage should include("must be set")
      exception.getMessage should include("security reasons")
    }

    it("should throw SecurityException when CLICKHOUSE_USER env var is not set") {
      // Set password but not username
      setEnv("CLICKHOUSE_PASSWORD", "test_pass")
      unsetEnv("CLICKHOUSE_USER")

      val exception = intercept[SecurityException] {
        PersistenceAgentType.ClickHouse(
          url = "http://localhost:8123",
          database = "quine",
        )
      }

      exception.getMessage should include("CLICKHOUSE_USER")
      exception.getMessage should include("must be set")
      exception.getMessage should include("security reasons")

      // Cleanup
      unsetEnv("CLICKHOUSE_PASSWORD")
    }

    it("should throw SecurityException when CLICKHOUSE_PASSWORD env var is not set") {
      // Set username but not password
      setEnv("CLICKHOUSE_USER", "test_user")
      unsetEnv("CLICKHOUSE_PASSWORD")

      val exception = intercept[SecurityException] {
        PersistenceAgentType.ClickHouse(
          url = "http://localhost:8123",
          database = "quine",
        )
      }

      exception.getMessage should include("CLICKHOUSE_PASSWORD")
      exception.getMessage should include("must be set")
      exception.getMessage should include("security reasons")

      // Cleanup
      unsetEnv("CLICKHOUSE_USER")
    }

    it("should use defaults for URL and database when env vars are not set") {
      // Set credentials via env vars
      setEnv("CLICKHOUSE_USER", "test_user")
      setEnv("CLICKHOUSE_PASSWORD", "test_password")
      // But don't set URL or database
      unsetEnv("CLICKHOUSE_URL")
      unsetEnv("CLICKHOUSE_DATABASE")

      val config = PersistenceAgentType.ClickHouse()

      config.url shouldBe "http://localhost:8123"
      config.database shouldBe "quine"
      config.username shouldBe "test_user"
      config.password shouldBe "test_password"

      // Cleanup
      unsetEnv("CLICKHOUSE_USER")
      unsetEnv("CLICKHOUSE_PASSWORD")
    }

    it("should read credentials from environment variables") {
      // Set all env vars
      setEnv("CLICKHOUSE_USER", "env_user")
      setEnv("CLICKHOUSE_PASSWORD", "env_password")
      setEnv("CLICKHOUSE_URL", "http://example.com:8123")
      setEnv("CLICKHOUSE_DATABASE", "test_db")

      val config = PersistenceAgentType.ClickHouse()

      config.url shouldBe "http://example.com:8123"
      config.database shouldBe "test_db"
      config.username shouldBe "env_user"
      config.password shouldBe "env_password"

      // Cleanup
      unsetEnv("CLICKHOUSE_USER")
      unsetEnv("CLICKHOUSE_PASSWORD")
      unsetEnv("CLICKHOUSE_URL")
      unsetEnv("CLICKHOUSE_DATABASE")
    }

    it("should accept explicit username and password") {
      // This test verifies that explicit parameters override env vars
      setEnv("CLICKHOUSE_USER", "env_user")
      setEnv("CLICKHOUSE_PASSWORD", "env_password")

      val config = PersistenceAgentType.ClickHouse(
        url = "http://localhost:8123",
        database = "quine",
        username = "explicit_user",
        password = "explicit_password",
      )

      config.url shouldBe "http://localhost:8123"
      config.database shouldBe "quine"
      config.username shouldBe "explicit_user"
      config.password shouldBe "explicit_password"

      // Cleanup
      unsetEnv("CLICKHOUSE_USER")
      unsetEnv("CLICKHOUSE_PASSWORD")
    }
  }
}
