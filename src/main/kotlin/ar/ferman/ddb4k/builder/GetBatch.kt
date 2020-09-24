package ar.ferman.ddb4k.builder

import ar.ferman.ddb4k.TableDefinition
import software.amazon.awssdk.services.dynamodb.model.*

class GetBatch<T : Any>(private val tableDefinition: TableDefinition<T>, private val values: Set<T>) {
    private val putItemRequest = BatchGetItemRequest.builder()

    fun custom(block: BatchGetItemRequest.Builder.() -> Unit) {
        block(putItemRequest)
    }

    fun build(): BatchGetItemRequest {
        if (values.isNotEmpty()) {
            val keys = KeysAndAttributes.builder().keys(values.map(tableDefinition::toItemKey)).build()
            putItemRequest.requestItems(mapOf(tableDefinition.tableName to keys))
        }

        return putItemRequest.build()
    }
}