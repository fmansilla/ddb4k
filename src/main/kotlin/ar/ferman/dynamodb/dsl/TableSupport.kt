package ar.ferman.dynamodb.dsl

import ar.ferman.dynamodb.dsl.AttributeType.*
import software.amazon.awssdk.services.dynamodb.model.*
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition

class TableSupport<T : Any>(private val tableDefinition: TableDefinition<T>) {
    fun buildCreateTableRequest(customize: CreateTableRequest.Builder.() -> Unit): CreateTableRequest {
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
            .billingProvisioned()
            .apply { customize(this) }
            .build()
    }

    fun buildDeleteTableRequest(): DeleteTableRequest {
        return DeleteTableRequest.builder()
            .tableName(tableDefinition.tableName)
            .build()
    }

    fun buildPutItemRequest(value: T): PutItemRequest {
        return PutItemRequest.builder().tableName(tableDefinition.tableName).item(tableDefinition.toItem(value)).build()
    }

    private fun AttributeType.toAttributeType(): ScalarAttributeType {
        return when (this) {
            STRING -> ScalarAttributeType.S
            INT, LONG, FLOAT, DOUBLE -> ScalarAttributeType.N
            else -> throw RuntimeException("Invalid key attribute type, only String or Number are allowed")//TODO custom exception
        }
    }
}

fun CreateTableRequest.Builder.billingPayPerRequest(): CreateTableRequest.Builder {
    return billingMode(BillingMode.PAY_PER_REQUEST)
}

fun CreateTableRequest.Builder.billingProvisioned(
    readCapacityUnits: Long = 5,
    writeCapacityUnits: Long = 5
): CreateTableRequest.Builder {
    return billingMode(BillingMode.PROVISIONED)
        .provisionedThroughput(
            ProvisionedThroughput.builder()
                .readCapacityUnits(readCapacityUnits)
                .writeCapacityUnits(writeCapacityUnits)
                .build()
        )
}
