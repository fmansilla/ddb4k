package ar.ferman.dynamodb.dsl.async

import ar.ferman.dynamodb.dsl.AttributeType
import ar.ferman.dynamodb.dsl.Attributes
import ar.ferman.dynamodb.dsl.PaginatedResult
import ar.ferman.dynamodb.dsl.TableDefinition
import ar.ferman.dynamodb.dsl.builder.Query
import ar.ferman.dynamodb.dsl.builder.Scan
import ar.ferman.dynamodb.dsl.builder.Update
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model.*
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException
import kotlin.coroutines.suspendCoroutine

class Table(
    private val dynamoDbClient: DynamoDbAsyncClient,
    private val tableDefinition: TableDefinition
) {

    suspend fun create() {
        val keySchemaElements = mutableListOf<KeySchemaElement>()
        val keyAttributeDefinitions = mutableListOf<AttributeDefinition>()

        tableDefinition.hashKey.apply {
            keySchemaElements.add(KeySchemaElement.builder().attributeName(name).keyType(KeyType.HASH).build())
            keyAttributeDefinitions.add(AttributeDefinition.builder().attributeName(name).attributeType(type.toAttributeType()).build())
        }

        tableDefinition.sortKey?.apply {
            keySchemaElements.add(KeySchemaElement.builder().attributeName(name).keyType(KeyType.RANGE).build())
            keyAttributeDefinitions.add(AttributeDefinition.builder().attributeName(name).attributeType(type.toAttributeType()).build())
        }

        val createTableRequest = CreateTableRequest.builder()
            .tableName(tableDefinition.name)
            .keySchema(keySchemaElements)
            .attributeDefinitions(keyAttributeDefinitions)
            .billingMode(BillingMode.PAY_PER_REQUEST)
            .build()

        return suspendCoroutine { continuation ->
            dynamoDbClient.createTable(createTableRequest).whenComplete { _, error ->
                if (error != null) {
                    continuation.resumeWithException(error)
                } else {
                    continuation.resume(Unit)
                }
            }
        }
    }

    suspend fun delete() {
        val createTableRequest = DeleteTableRequest.builder()
            .tableName(tableDefinition.name)
            .build()

        return suspendCoroutine { continuation ->
            dynamoDbClient.deleteTable(createTableRequest).whenComplete { _, error ->
                if (error != null) {
                    continuation.resumeWithException(error)
                } else {
                    continuation.resume(Unit)
                }
            }
        }
    }

    suspend fun <T : Any> queryPaginated(block: Query<T>.() -> Unit): PaginatedResult<T> {
        val queryBuilder = Query<T>(tableDefinition)

        block.invoke(queryBuilder)

        return object : PaginatedResult<T>() {
            override suspend fun requestNextPage(lastEvaluatedKey: Attributes): Pair<List<T>, Attributes> =
                suspendCoroutine { continuation ->
                    val queryRequest = queryBuilder.build(lastEvaluatedKey)

                    dynamoDbClient.query(queryRequest).whenComplete { result, error ->
                        if (error != null) {
                            continuation.resumeWithException(error)
                        } else {
                            val pageContent =
                                (result?.items()?.mapNotNull { queryBuilder.mapper.invoke(it) } ?: emptyList())
                            val currentLastEvaluatedKey = result?.lastEvaluatedKey() ?: emptyMap()
                            continuation.resume(pageContent to currentLastEvaluatedKey)
                        }
                    }
                }
        }
    }

    //COLD STREAM using Flow
    //Nothing gets executed or emitted until it is being requested for by a consumer
    fun <T : Any> query(block: Query<T>.() -> Unit): Flow<T> {
        val queryBuilder = Query<T>(tableDefinition)

        block.invoke(queryBuilder)

        return flow {
            var lastEvaluatedKey = emptyMap<String, AttributeValue>()

            do {
                val queryRequest = queryBuilder.build(lastEvaluatedKey)
                lateinit var pageContent: List<T>

                suspendCoroutine<Pair<List<T>, Map<String, AttributeValue>>> { continuation ->
                    dynamoDbClient.query(queryRequest).whenComplete { result, error ->
                        if (error != null) {
                            continuation.resumeWithException(error)
                        } else {
                            pageContent =
                                (result?.items()?.mapNotNull { queryBuilder.mapper.invoke(it) } ?: emptyList())
                            lastEvaluatedKey = result?.lastEvaluatedKey() ?: emptyMap()
                            continuation.resume(pageContent to lastEvaluatedKey)
                        }
                    }
                }

                pageContent.forEach { emit(it) }
            } while (lastEvaluatedKey.isNotEmpty())
        }
    }

    //HOT STREAM using producer
    //The code within the producer is invoked and items are emitted with or without the presence of a consumer.
    //Observable and Flowable types in RxJava are an example of a structure that represents a cold stream of items
//    @ExperimentalCoroutinesApi
//    suspend fun <T : Any> query(block: Query<T>.() -> Unit): ReceiveChannel<T> {
//        val queryBuilder = Query<T>(tableDefinition)
//
//        block.invoke(queryBuilder)
//
//        return CoroutineScope(coroutineContext).produce {
//            var lastEvaluatedKey = emptyMap<String, AttributeValue>()
//
//            do {
//                val queryRequest = queryBuilder.build(lastEvaluatedKey)
//                lateinit var pageContent: List<T>
//
//                suspendCoroutine<Pair<List<T>, Map<String, AttributeValue>>> { continuation ->
//                    dynamoDbClient.query(queryRequest).whenComplete { result, error ->
//                        if (error != null) {
//                            continuation.resumeWithException(error)
//                        } else {
//                            pageContent = (result?.items()?.mapNotNull { queryBuilder.mapper.invoke(it) } ?: emptyList())
//                            lastEvaluatedKey = result?.lastEvaluatedKey() ?: emptyMap()
//                            continuation.resume(pageContent to lastEvaluatedKey)
//                        }
//                    }
//                }
//
//                pageContent.forEach { send(it) }
//            } while (lastEvaluatedKey.isNotEmpty())
//        }
//    }

    suspend fun <T : Any> put(value: T, toItem: (T) -> Attributes) {
        val putItemRequest = PutItemRequest.builder().tableName(tableDefinition.name).item(toItem(value)).build()

        return suspendCoroutine { continuation ->
            dynamoDbClient.putItem(putItemRequest).whenComplete { _, error ->
                if (error != null) {
                    continuation.resumeWithException(error)
                } else {
                    continuation.resume(Unit)
                }
            }
        }
    }

    fun <T : Any> scanPaginated(block: Scan<T>.() -> Unit): PaginatedResult<T> {
        val scanBuilder = Scan<T>(tableDefinition)

        block.invoke(scanBuilder)

        return object : PaginatedResult<T>() {
            override suspend fun requestNextPage(lastEvaluatedKey: Attributes): Pair<List<T>, Attributes> =
                suspendCoroutine { continuation ->
                    val queryRequest = scanBuilder.build(lastEvaluatedKey)

                    dynamoDbClient.scan(queryRequest).whenComplete { result, error ->
                        if (error != null) {
                            continuation.resumeWithException(error)
                        } else {
                            val pageContent =
                                (result?.items()?.mapNotNull { scanBuilder.mapper.invoke(it) } ?: emptyList())
                            val currentLastEvaluatedKey = result?.lastEvaluatedKey() ?: emptyMap()
                            continuation.resume(pageContent to currentLastEvaluatedKey)
                        }
                    }
                }
        }
    }

//    @ExperimentalCoroutinesApi
//    suspend fun <T : Any> scan(block: Scan<T>.() -> Unit): ReceiveChannel<T> {
//        val scanBuilder = Scan<T>(tableDefinition)
//
//        block.invoke(scanBuilder)
//
//        return CoroutineScope(coroutineContext).produce {
//            var lastEvaluatedKey = emptyMap<String, AttributeValue>()
//
//            do {
//                val queryRequest = scanBuilder.build(lastEvaluatedKey)
//                lateinit var pageContent: List<T>
//
//                withContext(Dispatchers.IO) {
//                    val result = dynamoDbClient.scan(queryRequest).get()
//                    pageContent = (result?.items()?.mapNotNull { scanBuilder.mapper.invoke(it) } ?: emptyList())
//                    lastEvaluatedKey = result?.lastEvaluatedKey() ?: emptyMap()
//                }
//
//                pageContent.forEach { send(it) }
//            } while (lastEvaluatedKey.isNotEmpty())
//        }
//    }

    suspend fun <T : Any> scan(block: Scan<T>.() -> Unit): Flow<T> {
        val scanBuilder = Scan<T>(tableDefinition)

        block.invoke(scanBuilder)

        return flow {
            var lastEvaluatedKey = emptyMap<String, AttributeValue>()

            do {
                val queryRequest = scanBuilder.build(lastEvaluatedKey)
                lateinit var pageContent: List<T>

                suspendCoroutine<Unit> { continuation ->
                    dynamoDbClient.scan(queryRequest).whenComplete { result, _ ->
                        pageContent = (result?.items()?.mapNotNull { scanBuilder.mapper.invoke(it) } ?: emptyList())
                        lastEvaluatedKey = result?.lastEvaluatedKey() ?: emptyMap()
                        continuation.resume(Unit)
                    }
                }

                pageContent.forEach { emit(it) }
            } while (lastEvaluatedKey.isNotEmpty())

        }
    }


    suspend fun update(update: Update.() -> Unit) {

        val updateBuilder = Update(tableDefinition)
        update(updateBuilder)

        val updateItemRequest = updateBuilder.build()

        return suspendCoroutine { continuation ->
            dynamoDbClient.updateItem(updateItemRequest).whenComplete { _, error ->
                if (error != null) {
                    continuation.resumeWithException(error)
                } else {
                    continuation.resume(Unit)
                }
            }
        }
    }

}

private fun AttributeType.toAttributeType(): ScalarAttributeType {
    return when (this) {
        AttributeType.STRING -> ScalarAttributeType.S
        AttributeType.NUMBER -> ScalarAttributeType.N
    }
}

