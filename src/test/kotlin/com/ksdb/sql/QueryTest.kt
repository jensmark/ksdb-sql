package com.ksdb.sql

import io.zonky.test.db.postgres.embedded.EmbeddedPostgres
import org.junit.jupiter.api.Test
import java.time.Instant
import javax.sql.DataSource
import kotlin.test.assertEquals


data class TestType(
  val id: Long,
  val name: String,
  val updateAt: Instant
)

fun withEmbeddedPostgres(testCallback: (DataSource) -> Unit) {
  val embeddedPostgres = EmbeddedPostgres.start()
  val dataSource = embeddedPostgres.postgresDatabase

  transactional(dataSource) {
    query { "create table test_table (id bigserial primary key, name varchar, update_at timestamp with time zone)" } just execute()
    query { "insert into test_table(name, update_at) values (:name, :updateAt)" } bind {
      set("name", "hello world")
      set("updateAt", Instant.parse("2025-01-01T00:00:00Z"))
    } just execute()
  }

  testCallback(dataSource)

  transactional(dataSource) {
    query { "drop table test_table" } just execute()
  }
}

class QueryTest {

  @Test
  fun `should execute query and transform result as one`() = withEmbeddedPostgres {
    val result = transactional(it) { query { "select * from test_table" } execute asOne<TestType>() }
    assertEquals(TestType(1, "hello world", Instant.parse("2025-01-01T00:00:00Z")), result)
  }

  @Test
  fun `should execute query and transform result as list`() = withEmbeddedPostgres {
    val listResult = transactional(it) { query { "select * from test_table" } execute asList<TestType>() }
    assertEquals(listOf(TestType(1, "hello world", Instant.parse("2025-01-01T00:00:00Z"))), listResult)
  }

  @Test
  fun `should execute query and transform result as one with custom transformer`() = withEmbeddedPostgres {
    val customResult = transactional(it) { query { "select * from test_table" } execute asOne { mapOf(
      "id" to getLong("id"),
      "name" to getString("name"),
      "updateAt" to getTimestamp("update_at").toInstant()
    ) } }
    assertEquals(mapOf("id" to 1L, "name" to "hello world", "updateAt" to Instant.parse("2025-01-01T00:00:00Z")), customResult)
  }

  @Test
  fun `should execute query and transform result as list with custom transformer`() = withEmbeddedPostgres {
    val customListResult = transactional(it) { query { "select * from test_table" } execute asList { mapOf(
      "id" to getLong("id"),
      "name" to getString("name"),
      "updateAt" to getTimestamp("update_at").toInstant()
    ) } }
    assertEquals(listOf(mapOf("id" to 1L, "name" to "hello world", "updateAt" to Instant.parse("2025-01-01T00:00:00Z"))), customListResult)
  }

  @Test
  fun `should execute query with named bindings`() = withEmbeddedPostgres {
    val listResult = transactional(it) { query { "select * from test_table where id = :id and name = :name" } bind {
      set("id", 1)
      set("name", "hello world")
    } execute asList<TestType>() }
    assertEquals(listOf(TestType(1, "hello world", Instant.parse("2025-01-01T00:00:00Z"))), listResult)
  }

  @Test
  fun `should execute query with named callback binding`() = withEmbeddedPostgres {
    val listResult = transactional(it) { query { "select * from test_table where name = any(:name)" } bind {
      set("name") { it.createArrayOf("VARCHAR", arrayOf("hello world")) }
    } execute asList<TestType>() }
    assertEquals(listOf(TestType(1, "hello world", Instant.parse("2025-01-01T00:00:00Z"))), listResult)
  }

  @Test
  fun `should execute query with mapped named bindings`() = withEmbeddedPostgres {
    val listResult = transactional(it) { query { "select * from test_table where id = :id and name = :name" } bind {
      setAll("id" to 1, "name" to "hello world")
    } execute asList<TestType>() }
    assertEquals(listOf(TestType(1, "hello world", Instant.parse("2025-01-01T00:00:00Z"))), listResult)
  }

  @Test
  fun `should execute query with mapped model bindings`() = withEmbeddedPostgres {
    val listResult = transactional(it) { query { "select * from test_table where id = :id and name = :name and update_at = :updateAt" } bind {
      set(TestType(1, "hello world", Instant.parse("2025-01-01T00:00:00Z")))
    } execute asList<TestType>() }
    assertEquals(listOf(TestType(1, "hello world", Instant.parse("2025-01-01T00:00:00Z"))), listResult)
  }
}

