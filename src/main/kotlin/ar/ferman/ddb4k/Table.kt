package ar.ferman.ddb4k

import ar.ferman.ddb4k.builder.Query
import ar.ferman.ddb4k.builder.Scan
import ar.ferman.ddb4k.builder.Update
import kotlinx.coroutines.flow.Flow

interface Table<T : Any> {
    fun scan(block: Scan<T>.() -> Unit = {}): Flow<T>
    fun query(block: Query<T>.() -> Unit): Flow<T>

    suspend fun put(value: T)
    suspend fun update(update: Update<T>.() -> Unit)

    suspend fun get(key: T): T?
    suspend fun get(keys: Set<T>): List<T>

    suspend fun delete(key: T)
}