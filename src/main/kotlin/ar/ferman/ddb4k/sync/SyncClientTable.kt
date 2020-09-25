package ar.ferman.ddb4k.sync

import ar.ferman.ddb4k.Table
import ar.ferman.ddb4k.TableDefinition
import ar.ferman.ddb4k.builder.*
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.withContext
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model.AttributeValue

class SyncClientTable<T : Any>(
    private val dynamoDbClient: DynamoDbClient,
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

                    withContext(Dispatchers.IO) {
                        val result = dynamoDbClient.query(queryRequest)
                        pageContent = (result?.items()?.mapNotNull { queryBuilder.mapper.invoke(it) } ?: emptyList())
                        lastEvaluatedKey = result?.lastEvaluatedKey() ?: emptyMap()
                    }

                    pageContent.asSequence().take(limit - count).forEach {
                        count++
                        emit(it)
                    }
                } while (lastEvaluatedKey.isNotEmpty())
            }
        }
    }

    override suspend fun put(value: T, block: Put<T>.() -> Unit) =
        withContext(Dispatchers.IO) {
            val putItemRequest = Put(tableDefinition, value).apply(block).build()

            dynamoDbClient.putItem(putItemRequest)

            Unit
        }

    override fun scan(block: Scan<T>.() -> Unit): Flow<T> {
        val scanBuilder = Scan<T>(tableDefinition)

        block.invoke(scanBuilder)

        return flow {
            var lastEvaluatedKey = emptyMap<String, AttributeValue>()
            val limit = scanBuilder.currentLimit()
            if (limit > 0) {
                var count = 0
                do {
                    val queryRequest = scanBuilder.build(lastEvaluatedKey)
                    lateinit var pageContent: List<T>

                    withContext(Dispatchers.IO) {
                        val result = dynamoDbClient.scan(queryRequest)
                        pageContent = (result?.items()?.mapNotNull { scanBuilder.mapper.invoke(it) } ?: emptyList())
                        lastEvaluatedKey = result?.lastEvaluatedKey() ?: emptyMap()
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

        withContext(Dispatchers.IO) {
            dynamoDbClient.updateItem(updateItemRequest)
        }
    }

    override suspend fun get(key: T, block: Get<T>.() -> Unit): T? =
        withContext(Dispatchers.IO) {
            val getItemRequest = Get(tableDefinition, key).apply(block).build()

            dynamoDbClient.getItem(getItemRequest).item()
                ?.takeIf { !it.isNullOrEmpty() }
                ?.let(tableDefinition::fromItem)
        }

    override suspend fun get(keys: Set<T>, block: GetBatch<T>.() -> Unit): List<T> = withContext(Dispatchers.IO) {
        val batchGetItemRequest = GetBatch(tableDefinition, keys).apply(block).build()

        dynamoDbClient.batchGetItem(batchGetItemRequest).responses()[tableDefinition.tableName]
            ?.mapNotNull {
                it.takeIf { !it.isNullOrEmpty() }?.let(tableDefinition::fromItem)
            }
            ?: emptyList()
    }

    override suspend fun delete(key: T, block: Delete<T>.() -> Unit) =
        withContext(Dispatchers.IO) {
            val deleteItemRequest = Delete(tableDefinition, key).apply(block).build()

            dynamoDbClient.deleteItem(deleteItemRequest)

            Unit
        }
}
