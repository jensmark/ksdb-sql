package com.ksdb.sql

import java.sql.PreparedStatement
import java.sql.ResultSet

fun interface Executor<T> {
  fun execute(query: Query): T
}

inline fun <reified T: Any> asList() = object: Executor<List<T>> {
  override fun execute(query: Query): List<T> = query.prepareStatement().executeQuery().asList()
}
inline fun <reified T: Any> asList(crossinline transformer: ResultSet.() -> T) = object: Executor<List<T>> {
  override fun execute(query: Query): List<T> = query.prepareStatement().executeQuery().asList { transformer() }
}

inline fun <reified T: Any> asOne() = object: Executor<T?> {
  override fun execute(query: Query): T? = query.prepareStatement().executeQuery().asList<T>().firstOrNull()
}
inline fun <reified T: Any> asOne(crossinline transformer: ResultSet.() -> T) = object: Executor<T?> {
  override fun execute(query: Query): T? = query.prepareStatement().executeQuery().asList { transformer() }.firstOrNull()
}

fun execute() = object : Executor<Unit> {
  override fun execute(query: Query) {
    query.prepareStatement().execute()
  }
}

infix fun <T> Query.execute(executor: Executor<T>): T = executor.execute(this)
infix fun Query.just(executor: Executor<Unit>) = executor.execute(this)

fun Query.prepareStatement(): PreparedStatement {
  val statement = connection.prepareStatement(sqlStatement)
  bindings.forEach { it(statement) }
  return statement
}