package ar.ferman.ddb4k.builder

import ar.ferman.ddb4k.TableDefinition
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest

class Put<T : Any>(private val tableDefinition: TableDefinition<T>, private val value: T) {
    private val putItemRequest = PutItemRequest.builder().tableName(tableDefinition.tableName)

    fun custom(block: PutItemRequest.Builder.() -> Unit) {
        block(putItemRequest)
    }

    fun build(): PutItemRequest {
        return putItemRequest
            .item(tableDefinition.toItem(value))
            .build()
    }
}