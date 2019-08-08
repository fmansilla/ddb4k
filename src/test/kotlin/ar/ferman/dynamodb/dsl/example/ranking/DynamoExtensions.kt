package ar.ferman.dynamodb.dsl.example.ranking

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
            it.tableName(UserRankingTable.TableName)
        }.whenComplete { _, _ -> continuation.resume(Unit) }
    }
}

suspend fun DynamoDbClient.deleteUserRankingTable() = withContext(Dispatchers.IO) {
    try {
        deleteTable {
            it.tableName(UserRankingTable.TableName)
        }
    } catch (e: Exception) {
        //Ignoring if table not found
    }
}

suspend fun DynamoDbAsyncClient.createUserRankingTable() {
    suspendCoroutine<Unit> { continuation ->
        createTable {
            it.tableName(UserRankingTable.TableName)
            it.keySchema(
                KeySchemaElement.builder()
                    .attributeName(UserRankingTable.UserId).keyType(KeyType.HASH)
                    .build()
//                ,KeySchemaElement.builder()
//                    .attributeName(UserRankingTable.Score).keyType(KeyType.RANGE)
//                    .build()

            )
            it.attributeDefinitions(
                AttributeDefinition.builder()
                    .attributeName(UserRankingTable.UserId).attributeType(ScalarAttributeType.S)
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
        it.tableName(UserRankingTable.TableName)
        it.keySchema(
            KeySchemaElement.builder()
                .attributeName(UserRankingTable.UserId).keyType(KeyType.HASH)
                .build()
//            ,KeySchemaElement.builder()
//                .attributeName(UserRankingTable.Score).keyType(KeyType.RANGE)
//                .build()

        )
        it.attributeDefinitions(
            AttributeDefinition.builder()
                .attributeName(UserRankingTable.UserId).attributeType(ScalarAttributeType.S)
                .build()
//            ,AttributeDefinition.builder()
//                .attributeName(UserRankingTable.Score).attributeType(ScalarAttributeType.N)
//                .build()
        )
        it.provisionedThroughput(ProvisionedThroughput.builder().readCapacityUnits(1).writeCapacityUnits(1).build())
    }
}

