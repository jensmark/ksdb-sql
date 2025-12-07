package com.ksdb.sql

import java.lang.Exception
import java.sql.Connection
import javax.sql.DataSource

data class TransactionScope(val dataSource: DataSource, val connection: Connection)
val scopedTransaction: ScopedValue<TransactionScope> = ScopedValue.newInstance<TransactionScope>()

fun <T> transactional(dataSource: DataSource, transaction: TransactionScope.() -> T): T {
  if (scopedTransaction.isBound) {
    return scopedTransaction.get().transaction()
  }

  val scope = TransactionScope(dataSource, dataSource.connection).also {
    it.connection.autoCommit = false
  }
  return ScopedValue.where(scopedTransaction, scope).call<T, Exception> {
    try {
      scope.transaction().also { scope.connection.commit() }
    } catch (e: Exception) {
      scope.connection.rollback()
      throw when(e) {
        is RuntimeException -> e
        else -> RuntimeException(e)
      }
    } finally {
      scope.connection.close()
    }
  }
}

fun <T> TransactionScope.transactional(transaction: TransactionScope.() -> T) = transactional(this.dataSource, transaction)