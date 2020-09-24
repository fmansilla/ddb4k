package ar.ferman.ddb4k.builder

import ar.ferman.ddb4k.TableDefinition
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest

class Get<T : Any>(private val tableDefinition: TableDefinition<T>, private val value: T) {
    private val putItemRequest = GetItemRequest.builder().tableName(tableDefinition.tableName)

    fun custom(block: GetItemRequest.Builder.() -> Unit) {
        block(putItemRequest)
    }

    fun build(): GetItemRequest {
        return putItemRequest
            .key(tableDefinition.toItemKey(value))
            .build()
    }
}