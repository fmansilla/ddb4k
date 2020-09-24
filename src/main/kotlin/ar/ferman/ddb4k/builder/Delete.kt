package ar.ferman.ddb4k.builder

import ar.ferman.ddb4k.TableDefinition
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest

class Delete<T : Any>(private val tableDefinition: TableDefinition<T>, private val key: T) {
    private val deleteItemRequest = DeleteItemRequest.builder().tableName(tableDefinition.tableName)

    fun custom(block: DeleteItemRequest.Builder.() -> Unit) {
        block(deleteItemRequest)
    }

    fun build(): DeleteItemRequest {
        return deleteItemRequest
            .key(tableDefinition.toItemKey(key))
            .build()
    }
}