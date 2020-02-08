package ar.ferman.dynamodb.dsl

suspend fun Table.recreate() {
    try {
        delete()
    } catch (e: Exception) { /* Ignore not existent */
    }
    create()
}