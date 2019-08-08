package ar.ferman.dynamodb.dsl.async

suspend fun Table.createIfNotExist() {
    try {
        delete()
    } catch (e: Exception) { /* Ignore not existent */
    }
    create()
}