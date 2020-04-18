package ar.ferman.dynamodb.dsl.sync

import ar.ferman.dynamodb.dsl.TableDefinition
import ar.ferman.dynamodb.dsl.Tables
import ar.ferman.dynamodb.dsl.TablesSupport
import ar.ferman.dynamodb.dsl.builder.ListTables
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.firstOrNull
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.withContext
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest.Builder

class SyncClientTables(private val dynamoDbClient: DynamoDbClient) : Tables {

    private val tablesSupport = TablesSupport()

    override suspend fun <T : Any> create(
        tableDefinition: TableDefinition<T>, customize: Builder.() -> Unit
    ) {
        val createTableRequest = tablesSupport.buildCreateTableRequest(tableDefinition, customize)

        withContext(Dispatchers.IO) {
            dynamoDbClient.createTable(createTableRequest)
        }
    }

    override suspend fun delete(tableName: String) {
        val deleteTableRequest = tablesSupport.buildDeleteTableRequest(tableName)

        withContext(Dispatchers.IO) {
            dynamoDbClient.deleteTable(deleteTableRequest)
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
                lateinit var pageContent: List<String>

                withContext(Dispatchers.IO) {
                    val result = dynamoDbClient.listTables(queryRequest)
                    pageContent = (result?.tableNames() ?: emptyList())
                    lastTableName = result?.lastEvaluatedTableName()
                }

                pageContent.forEach { emit(it) }
            } while (lastTableName != null)
        }
    }


    suspend fun <T : Any> recreate(tableDefinition: TableDefinition<T>, customize: Builder.() -> Unit = {}) {
        if (exists(tableDefinition.tableName)) kotlin.runCatching { delete(tableDefinition.tableName) }
        create(tableDefinition, customize)
    }
}