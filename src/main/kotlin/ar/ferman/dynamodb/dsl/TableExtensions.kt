package ar.ferman.dynamodb.dsl

suspend fun <T: Any> Table<T>.recreate() {
    try {
        delete()
    } catch (e: Exception) { /* Ignore not existent */
    }
    create()
}