package ar.ferman.ddb4k

import ar.ferman.ddb4k.builder.*
import kotlinx.coroutines.flow.Flow

interface Table<T : Any> {
    fun scan(block: Scan<T>.() -> Unit = {}): Flow<T>
    fun query(block: Query<T>.() -> Unit): Flow<T>

    suspend fun put(value: T, block: Put<T>.() -> Unit = {})
    suspend fun update(update: Update<T>.() -> Unit)

    suspend fun get(key: T, block: Get<T>.() -> Unit = {}): T?
    suspend fun get(keys: Set<T>, block: GetBatch<T>.() -> Unit = {}): List<T>

    suspend fun delete(key: T, block: Delete<T>.() -> Unit = {})
}