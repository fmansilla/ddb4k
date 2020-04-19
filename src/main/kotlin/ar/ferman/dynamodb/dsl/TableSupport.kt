package ar.ferman.dynamodb.dsl

import software.amazon.awssdk.services.dynamodb.model.*

class TableSupport<T : Any>(private val tableDefinition: TableDefinition<T>) {

    fun buildPutItemRequest(value: T): PutItemRequest {
        return PutItemRequest.builder().tableName(tableDefinition.tableName).item(tableDefinition.toItem(value)).build()
    }

    fun buildGetItemRequest(value: T): GetItemRequest {
        return GetItemRequest.builder().tableName(tableDefinition.tableName).key(tableDefinition.toItemKey(value))
            .build()
    }

    fun buildBatchGetItemRequest(values: Set<T>): BatchGetItemRequest {
        val keys = KeysAndAttributes.builder().keys(values.map(tableDefinition::toItemKey)).build()
        return BatchGetItemRequest.builder().requestItems(mapOf(tableDefinition.tableName to keys)).build()
    }

    fun buildDeleteItemRequest(value: T): DeleteItemRequest {
        return DeleteItemRequest.builder().tableName(tableDefinition.tableName).key(tableDefinition.toItemKey(value))
            .build()
    }
}
