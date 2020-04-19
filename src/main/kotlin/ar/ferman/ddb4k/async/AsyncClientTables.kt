package ar.ferman.ddb4k.async

import ar.ferman.ddb4k.TableDefinition
import ar.ferman.ddb4k.Tables
import ar.ferman.ddb4k.builder.ListTables
import ar.ferman.ddb4k.builder.TablesSupport
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.firstOrNull
import kotlinx.coroutines.flow.flow
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException
import kotlin.coroutines.suspendCoroutine

class AsyncClientTables(private val dynamoDbClient: DynamoDbAsyncClient) : Tables {

    private val tablesSupport = TablesSupport()

    override suspend fun <T : Any> create(tableDefinition: TableDefinition<T>, customize: CreateTableRequest.Builder.() -> Unit) {
        val createTableRequest = tablesSupport.buildCreateTableRequest(tableDefinition, customize)

        return suspendCoroutine { continuation ->
            dynamoDbClient.createTable(createTableRequest).whenComplete { _, error ->
                if (error != null) continuation.resumeWithException(error)
                else continuation.resume(Unit)
            }
        }
    }

    override suspend fun delete(tableName: String) {
        val deleteTableRequest = tablesSupport.buildDeleteTableRequest(tableName)

        return suspendCoroutine { continuation ->
            dynamoDbClient.deleteTable(deleteTableRequest).whenComplete { _, error ->
                if (error != null) continuation.resumeWithException(error)
                else continuation.resume(Unit)
            }
        }
    }

    override suspend fun exists(tableName: String): Boolean {
        return list().firstOrNull { it == tableName } != null
    }

    override fun list(): Flow<String> {
            val listTablesBuilder = ListTables()

            return flow {
                var lastTableName: String? = null

                do {
                    val queryRequest = listTablesBuilder.build(lastTableName)
                    var pageContent: List<String> = emptyList()
                    suspendCoroutine<Unit> { continuation ->
                        dynamoDbClient.listTables(queryRequest).whenComplete { result, _ ->
                            pageContent = (result?.tableNames() ?: emptyList())
                            lastTableName = result?.lastEvaluatedTableName()
                            continuation.resume(Unit)
                        }
                    }

                    pageContent.forEach { emit(it) }
                } while (lastTableName != null)
            }
        }


    suspend fun <T : Any> recreate(tableDefinition: TableDefinition<T>, customize: CreateTableRequest.Builder.() -> Unit = {}) {
        if (exists(tableDefinition.tableName)) kotlin.runCatching { delete(tableDefinition.tableName) }
        create(tableDefinition, customize)
    }
}