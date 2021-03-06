package ar.ferman.ddb4k.async

import ar.ferman.ddb4k.Table
import ar.ferman.ddb4k.TableDefinition
import ar.ferman.ddb4k.builder.*
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException
import kotlin.coroutines.suspendCoroutine


class AsyncClientTable<T : Any>(
    private val dynamoDbClient: DynamoDbAsyncClient,
    private val tableDefinition: TableDefinition<T>
) : Table<T> {

    override fun query(block: Query<T>.() -> Unit): Flow<T> {
        val queryBuilder = Query(tableDefinition)

        block.invoke(queryBuilder)

        return flow {
            var lastEvaluatedKey = emptyMap<String, AttributeValue>()
            val limit = queryBuilder.currentLimit()

            if (limit > 0) {
                var count = 0
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

                    pageContent.asSequence().take(limit - count).forEach {
                        count++
                        emit(it)
                    }
                } while (lastEvaluatedKey.isNotEmpty() && count < limit)
            }
        }
    }

    override suspend fun put(value: T, block: Put<T>.() -> Unit) {
        val putItemRequest = Put(tableDefinition, value).apply(block).build()

        return suspendCoroutine { continuation ->
            dynamoDbClient.putItem(putItemRequest).whenComplete { _, error ->
                if (error != null) continuation.resumeWithException(error)
                else continuation.resume(Unit)
            }
        }
    }

    override fun scan(block: Scan<T>.() -> Unit): Flow<T> {
        val scanBuilder = Scan(tableDefinition)

        block.invoke(scanBuilder)

        return flow {
            var lastEvaluatedKey = emptyMap<String, AttributeValue>()
            val limit = scanBuilder.currentLimit()
            if (limit > 0) {
                var count = 0
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

                    pageContent.asSequence().take(limit - count).forEach {
                        count++
                        emit(it)
                    }
                } while (lastEvaluatedKey.isNotEmpty() && count < limit)
            }
        }
    }


    override suspend fun update(update: Update<T>.() -> Unit) {
        val updateBuilder = Update(tableDefinition)
        update(updateBuilder)

        val updateItemRequest = updateBuilder.build()

        return suspendCoroutine { continuation ->
            dynamoDbClient.updateItem(updateItemRequest).whenComplete { _, error ->
                if (error != null) continuation.resumeWithException(error)
                else continuation.resume(Unit)
            }
        }
    }

    override suspend fun get(key: T, block: Get<T>.() -> Unit): T? {
        val getItemRequest = Get(tableDefinition, key).apply(block).build()

        return suspendCoroutine { continuation ->
            dynamoDbClient.getItem(getItemRequest).whenComplete { response, error ->
                if (error != null) continuation.resumeWithException(error)
                else continuation.resume(response.item()?.takeIf { !it.isNullOrEmpty() }
                    ?.let(tableDefinition::fromItem))
            }
        }
    }

    override suspend fun get(keys: Set<T>, block: GetBatch<T>.() -> Unit): List<T> {
        val batchGetItemRequest = GetBatch(tableDefinition, keys).apply(block).build()

        return suspendCoroutine { continuation ->
            dynamoDbClient.batchGetItem(batchGetItemRequest).whenComplete { response, error ->
                if (error != null) continuation.resumeWithException(error)
                else continuation.resume(response.responses()[tableDefinition.tableName]
                    ?.mapNotNull {
                        it.takeIf { !it.isNullOrEmpty() }?.let(tableDefinition::fromItem)
                    } ?: emptyList())
            }
        }
    }

    override suspend fun delete(key: T, block: Delete<T>.() -> Unit) {
        val deleteItemRequest = Delete(tableDefinition, key).apply(block).build()

        return suspendCoroutine { continuation ->
            dynamoDbClient.deleteItem(deleteItemRequest).whenComplete { _, error ->
                if (error != null) continuation.resumeWithException(error)
                else continuation.resume(Unit)
            }
        }
    }
}