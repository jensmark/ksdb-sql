package com.ksdb.sql

import org.intellij.lang.annotations.Language
import java.sql.Connection
import java.sql.PreparedStatement


data class Query (
  val connection: Connection,
  val sqlStatement: String,
  val bindings: List<(PreparedStatement) -> Unit> = emptyList(),
)

fun interface SqlStatement {
  @Language("SQL") fun sql(): String
}

fun query(transactionScope: TransactionScope, statement: SqlStatement) = query(transactionScope.connection, statement)
fun query(connection: Connection, statement: SqlStatement): Query {
  return Query(connection, statement.sql())
}

@JvmName("queryExt")
fun Connection.query(statement: SqlStatement) = query(this, statement)

@JvmName("queryExt")
fun TransactionScope.query(statement: SqlStatement) = query(this, statement)
