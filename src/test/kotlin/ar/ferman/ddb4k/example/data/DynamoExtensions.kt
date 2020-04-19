package ar.ferman.ddb4k.example.data

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model.*
import kotlin.coroutines.resume
import kotlin.coroutines.suspendCoroutine

suspend fun DynamoDbAsyncClient.deleteUserRankingTable() {
    suspendCoroutine<Unit> { continuation ->
        deleteTable {
            it.tableName(ExampleTable.TableName)
        }.whenComplete { _, _ -> continuation.resume(Unit) }
    }
}

suspend fun DynamoDbClient.deleteUserRankingTable() = withContext(Dispatchers.IO) {
    try {
        deleteTable {
            it.tableName(ExampleTable.TableName)
        }
    } catch (e: Exception) {
        //Ignoring if table not found
    }
}

suspend fun DynamoDbAsyncClient.createUserRankingTable() {
    suspendCoroutine<Unit> { continuation ->
        createTable {
            it.tableName(ExampleTable.TableName)
            it.keySchema(
                KeySchemaElement.builder()
                    .attributeName(ExampleTable.UserId).keyType(KeyType.HASH)
                    .build()
//                ,KeySchemaElement.builder()
//                    .attributeName(UserRankingTable.Score).keyType(KeyType.RANGE)
//                    .build()

            )
            it.attributeDefinitions(
                AttributeDefinition.builder()
                    .attributeName(ExampleTable.UserId).attributeType(ScalarAttributeType.S)
                    .build()
//                .AttributeDefinition.builder()
//                    .attributeName(UserRankingTable.Score).attributeType(ScalarAttributeType.N)
//                    .build()
            )
            it.provisionedThroughput(ProvisionedThroughput.builder().readCapacityUnits(1).writeCapacityUnits(1).build())
        }.whenComplete { _, _ -> continuation.resume(Unit) }
    }
}

suspend fun DynamoDbClient.createUserRankingTable() = withContext(Dispatchers.IO) {
    createTable {
        it.tableName(ExampleTable.TableName)
        it.keySchema(
            KeySchemaElement.builder()
                .attributeName(ExampleTable.UserId).keyType(KeyType.HASH)
                .build()
//            ,KeySchemaElement.builder()
//                .attributeName(UserRankingTable.Score).keyType(KeyType.RANGE)
//                .build()

        )
        it.attributeDefinitions(
            AttributeDefinition.builder()
                .attributeName(ExampleTable.UserId).attributeType(ScalarAttributeType.S)
                .build()
//            ,AttributeDefinition.builder()
//                .attributeName(UserRankingTable.Score).attributeType(ScalarAttributeType.N)
//                .build()
        )
        it.provisionedThroughput(ProvisionedThroughput.builder().readCapacityUnits(1).writeCapacityUnits(1).build())
    }
}

