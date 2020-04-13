package ar.ferman.dynamodb.dsl

import ar.ferman.dynamodb.dsl.builder.Query
import ar.ferman.dynamodb.dsl.builder.Scan
import ar.ferman.dynamodb.dsl.builder.Update
import kotlinx.coroutines.flow.Flow

interface Table<T : Any> {
    suspend fun create()
    suspend fun delete()
    fun query(block: Query<T>.() -> Unit): Flow<T>
    suspend fun put(value: T)
    fun scan(block: Scan<T>.() -> Unit = {}): Flow<T>
    suspend fun update(update: Update<T>.() -> Unit)
}