package ar.ferman.dynamodb.dsl.sync

import ar.ferman.dynamodb.dsl.Attributes
import ar.ferman.dynamodb.dsl.Query
import ar.ferman.dynamodb.dsl.Scan
import ar.ferman.dynamodb.dsl.TableDefinition
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.channels.produce
import kotlinx.coroutines.withContext
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest
import kotlin.coroutines.coroutineContext

class Table(
    private val dynamoDbClient: DynamoDbClient,
    private val tableDefinition: TableDefinition
) {
    suspend fun <T : Any> query(block: Query<T>.() -> Unit): List<T> = withContext(Dispatchers.IO) {
        val queryBuilder = Query<T>(tableDefinition)
        block.invoke(queryBuilder)
        val queryRequest = queryBuilder.build(emptyMap())

        dynamoDbClient.query(queryRequest)?.items()?.mapNotNull { queryBuilder.mapper.invoke(it) } ?: emptyList()
    }

    suspend fun <T : Any> put(value: T, toItem: (T) -> Attributes) = withContext(Dispatchers.IO) {
        val putItemRequest = PutItemRequest.builder().tableName(tableDefinition.name).item(toItem(value)).build()

        dynamoDbClient.putItem(putItemRequest)

        Unit
    }

    fun <T : Any> scanPaginated(block: Scan<T>.() -> Unit): PaginatedResult<T> {
        val scanBuilder = Scan<T>(tableDefinition)

        block.invoke(scanBuilder)

        return object : PaginatedResult<T>() {
            override suspend fun requestNextPage(lastEvaluatedKey: Attributes): Pair<List<T>, Attributes> =
                withContext(Dispatchers.IO) {
                    val queryRequest = scanBuilder.build(lastEvaluatedKey)

                    val result = dynamoDbClient.scan(queryRequest)
                    val pageContent = (result?.items()?.mapNotNull { scanBuilder.mapper.invoke(it) } ?: emptyList())
                    val currentLastEvaluatedKey = result?.lastEvaluatedKey() ?: emptyMap()

                    pageContent to currentLastEvaluatedKey
                }
        }
    }

    @ExperimentalCoroutinesApi
    suspend fun <T : Any> scan(block: Scan<T>.() -> Unit): ReceiveChannel<T> {
        val scanBuilder = Scan<T>(tableDefinition)

        block.invoke(scanBuilder)

        return CoroutineScope(coroutineContext).produce {
            var lastEvaluatedKey = emptyMap<String, AttributeValue>()

            do {
                val queryRequest = scanBuilder.build(lastEvaluatedKey)
                lateinit var pageContent: List<T>

                withContext(Dispatchers.IO) {
                    val result = dynamoDbClient.scan(queryRequest)
                    pageContent = (result?.items()?.mapNotNull { scanBuilder.mapper.invoke(it) } ?: emptyList())
                    lastEvaluatedKey = result?.lastEvaluatedKey() ?: emptyMap()
                }

                pageContent.forEach { send(it) }
            } while (lastEvaluatedKey.isNotEmpty())
        }
    }

    abstract class PaginatedResult<T> {
        var firstFetch = true
        var key: Attributes = emptyMap()

        fun hasNext(): Boolean {
            return firstFetch || key.isNotEmpty()
        }

        suspend fun next(): List<T> {
            firstFetch = false
            val (pageContent, lastEvaluatedKey) = requestNextPage(key)
            key = lastEvaluatedKey

            return pageContent
        }

        suspend fun awaitAll(): List<T> {
            val result = mutableListOf<T>()
            while (hasNext()) {
                result.addAll(next())
            }

            return result
        }


        protected abstract suspend fun requestNextPage(lastEvaluatedKey: Attributes): Pair<List<T>, Attributes>
    }
}