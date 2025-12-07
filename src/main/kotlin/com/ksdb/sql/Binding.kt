package com.ksdb.sql

import java.sql.Connection
import java.sql.Date
import java.sql.PreparedStatement
import java.sql.Timestamp
import java.time.Instant
import java.time.LocalDate
import kotlin.reflect.full.memberProperties

data class SqlWithParameters(val sql: String, val parameterMap: Map<String, Int>)
data class BindingScope(
  val connection: Connection,
  val parameterMap: Map<String, Int>,
  val bindings: MutableList<(PreparedStatement) -> Unit>
)

inline fun <reified T> BindingScope.set(name: String, value: T?) {
  bindings.add {
    when (value) {
      null -> it.setNull(parameterMap.getValue(name), 1111)
      is Instant -> it.setTimestamp(parameterMap.getValue(name), Timestamp.from(value))
      is LocalDate -> it.setDate(parameterMap.getValue(name), Date.valueOf(value))
      else -> it.setObject(parameterMap.getValue(name), value)
    }
  }
}

fun BindingScope.setAll(params: Map<String, *>) {
  params.forEach { set(it.key, it.value) }
}

fun BindingScope.setAll(vararg params: Pair<String, *>) {
  setAll(mapOf(*params))
}

inline fun <reified T: Any> BindingScope.set(value: T) {
  val mapping = T::class.memberProperties.associate { it.name to it.getValue(value, it) }
  setAll(mapping)
}

inline fun <reified T> BindingScope.set(name: String, valueFunc: (Connection) -> T) {
  set(name, valueFunc(connection))
}

infix fun Query.bind(build: BindingScope.() -> Unit): Query {
  val binds = sqlStatement.extractBinds()
  val bindingsScope = BindingScope(connection, binds.parameterMap, mutableListOf()).also { it.build() }
  return copy(sqlStatement = binds.sql, bindings = bindingsScope.bindings)
}

fun String.extractBinds(): SqlWithParameters {
  val bindingsRegex = ":\\w+".toRegex()
  return SqlWithParameters(
    replace(bindingsRegex, "?"),
    bindingsRegex.findAll(this).mapIndexed { index, match ->  match.value.drop(1) to index+1}.toMap()
  )
}