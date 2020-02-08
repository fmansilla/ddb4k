package ar.ferman.dynamodb.dsl

import ar.ferman.dynamodb.dsl.builder.Query
import ar.ferman.dynamodb.dsl.builder.Scan
import ar.ferman.dynamodb.dsl.builder.Update
import kotlinx.coroutines.flow.Flow

interface Table {
    suspend fun create()
    suspend fun delete()
    fun <T : Any> query(block: Query<T>.() -> Unit): Flow<T>
    suspend fun <T : Any> put(value: T, toItem: (T) -> Attributes)
    suspend fun <T : Any> scan(block: Scan<T>.() -> Unit): Flow<T>
    suspend fun update(update: Update.() -> Unit)
}