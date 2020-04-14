package ar.ferman.dynamodb.dsl

import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest

suspend fun <T: Any> Table<T>.recreate(customize: CreateTableRequest.Builder.() -> Unit = {}) {
    try {
        delete()
    } catch (e: Exception) { /* Ignore not existent */
    }
    create(customize)
}