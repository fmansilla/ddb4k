package ar.ferman.dynamodb.dsl

import software.amazon.awssdk.services.dynamodb.model.*
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition

class TableSupport<T: Any>(private val tableDefinition: TableDefinition<T>) {

    fun buildCreateTableRequest(): CreateTableRequest {
        val keySchemaElements = mutableListOf<KeySchemaElement>()
        val keyAttributeDefinitions = mutableListOf<AttributeDefinition>()

        tableDefinition.hashKey!!.apply {
            keySchemaElements.add(
                KeySchemaElement.builder().attributeName(name).keyType(
                    KeyType.HASH
                ).build()
            )
            keyAttributeDefinitions.add(AttributeDefinition.builder().attributeName(name).attributeType(type.toAttributeType()).build())
        }

        tableDefinition.sortKey?.apply {
            keySchemaElements.add(
                KeySchemaElement.builder().attributeName(name).keyType(
                    KeyType.RANGE
                ).build()
            )
            keyAttributeDefinitions.add(AttributeDefinition.builder().attributeName(name).attributeType(type.toAttributeType()).build())
        }

        return CreateTableRequest.builder()
            .tableName(tableDefinition.tableName)
            .keySchema(keySchemaElements)
            .attributeDefinitions(keyAttributeDefinitions)
            .billingMode(BillingMode.PAY_PER_REQUEST)
            .build()
    }

    fun buildDeleteTableRequest(): DeleteTableRequest {
        return DeleteTableRequest.builder()
            .tableName(tableDefinition.tableName)
            .build()
    }

    fun <T : Any> buildPutItemRequest(toItem: (T) -> Attributes, value: T): PutItemRequest {
        return PutItemRequest.builder().tableName(tableDefinition.tableName).item(toItem(value)).build()
    }

    private fun AttributeType.toAttributeType(): ScalarAttributeType {
        return when (this) {
            AttributeType.STRING -> ScalarAttributeType.S
            AttributeType.NUMBER -> ScalarAttributeType.N
        }
    }
}