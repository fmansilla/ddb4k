package ar.ferman.dynamodb.dsl

import software.amazon.awssdk.services.dynamodb.model.*
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition

class TablesSupport {

    fun <T : Any> buildCreateTableRequest(
        tableDefinition: TableDefinition<T>,
        customize: CreateTableRequest.Builder.() -> Unit
    ): CreateTableRequest {
        val keySchemaElements = mutableListOf<KeySchemaElement>()
        val keyAttributeDefinitions = mutableListOf<AttributeDefinition>()

        tableDefinition.hashKey!!.apply {
            keySchemaElements.add(
                KeySchemaElement.builder().attributeName(name).keyType(
                    KeyType.HASH
                ).build()
            )
            keyAttributeDefinitions.add(AttributeDefinition.builder().attributeName(name).attributeType(type.toScalarAttributeType()).build())
        }

        tableDefinition.sortKey?.apply {
            keySchemaElements.add(
                KeySchemaElement.builder().attributeName(name).keyType(
                    KeyType.RANGE
                ).build()
            )
            keyAttributeDefinitions.add(AttributeDefinition.builder().attributeName(name).attributeType(type.toScalarAttributeType()).build())
        }

        return CreateTableRequest.builder()
            .tableName(tableDefinition.tableName)
            .keySchema(keySchemaElements)
            .attributeDefinitions(keyAttributeDefinitions)
            .billingProvisioned()
            .apply { customize(this) }
            .build()
    }

    fun buildDeleteTableRequest(tableName: String): DeleteTableRequest {
        return DeleteTableRequest.builder()
            .tableName(tableName)
            .build()
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
