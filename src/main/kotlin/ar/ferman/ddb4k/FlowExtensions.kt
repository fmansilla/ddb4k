package ar.ferman.ddb4k

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.flow

suspend fun <T, K> Flow<T>.groupBy(getKey: (T) -> K): Map<K, List<T>> {
    val storage = mutableMapOf<K, MutableList<T>>()
    collect {
        storage.getOrPut(getKey(it)) { mutableListOf() } += it
    }
    return storage
}

fun <T, K> Flow<T>.groupingBy(getKey: (T) -> K): Flow<Pair<K, List<T>>> = flow {
    val storage = mutableMapOf<K, MutableList<T>>()
    collect {
        storage.getOrPut(getKey(it)) { mutableListOf() } += it
    }
    storage.forEach { (k, ts) -> emit(k to ts) }
}