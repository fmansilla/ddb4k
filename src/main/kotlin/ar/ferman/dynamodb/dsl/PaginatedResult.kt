package ar.ferman.dynamodb.dsl

abstract class PaginatedResult<T> {
    var firstFetch = true
    var key: Attributes = emptyMap()

    fun hasNext(): Boolean {
        return firstFetch || key.isNotEmpty()
    }

    suspend fun next(): List<T> {
        firstFetch = false
        val (pageContent, lastEvaluatedKey) = requestNextPage(key)
        key = lastEvaluatedKey

        return pageContent
    }

    suspend fun awaitAll(): List<T> {
        val result = mutableListOf<T>()
        while (hasNext()) {
            result.addAll(next())
        }

        return result
    }


    protected abstract suspend fun requestNextPage(lastEvaluatedKey: Attributes): Pair<List<T>, Attributes>
}