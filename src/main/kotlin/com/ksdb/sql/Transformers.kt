package com.ksdb.sql

import java.sql.JDBCType
import java.sql.ResultSet
import java.sql.ResultSetMetaData
import java.sql.SQLType
import kotlin.reflect.KCallable
import kotlin.reflect.KParameter
import kotlin.reflect.full.primaryConstructor

data class ResultColumn (
  val index: Int,
  val name: String,
  val sqlType: SQLType,
  val parameter: KParameter? = null
)

fun ResultSetMetaData.toColumns(mapper: (String) -> KParameter? = { null }) = (1 .. columnCount).map {
  ResultColumn(it, getColumnName(it), JDBCType.valueOf(getColumnType(it)), mapper(getColumnName(it)))
}
fun ResultSet.getColumns(mapper: (String) -> KParameter? = { null }) = metaData.toColumns(mapper)

inline fun <reified T: Any> ResultSet.asList(crossinline transformer: ResultSet.() -> T): List<T> {
  return sequence<T> {
    while (next()) {
      yield(transformer())
    }
  }.toList()
}

inline fun <reified T: Any> ResultSet.asList(): List<T> {
  val typeConstructor = requireNotNull(T::class.primaryConstructor) { "Mapping type ${T::class.simpleName} must have a primary constructor" }
  val columns = getColumns { typeConstructor.findParameterByNormalizedName(it) }

  return sequence<T> {
    while (next()) {
      val args = columns.associate {
        requireNotNull(it.parameter) { "Unable to map column ${it.name} to model ${T::class.simpleName}" } to getValue(it)
      }
      yield(typeConstructor.callBy(args))
    }
  }.toList()
}

fun ResultSet.getValue(column: ResultColumn): Any? = when(column.sqlType) {
  JDBCType.TIMESTAMP -> getTimestamp(column.index)?.toInstant()
  JDBCType.DATE -> getDate(column.index)?.toLocalDate()
  else -> getObject(column.index)
}

fun KCallable<*>.findParameterByNormalizedName(name: String): KParameter? {
  return parameters.singleOrNull { it.name?.normalizeName() == name.normalizeName() }
}
fun String.normalizeName() = replace("_", "").lowercase()