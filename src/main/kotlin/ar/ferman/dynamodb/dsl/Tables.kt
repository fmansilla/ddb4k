package ar.ferman.dynamodb.dsl

import kotlinx.coroutines.flow.Flow
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest.Builder

interface Tables {
    suspend fun <T : Any> create(tableDefinition: TableDefinition<T>, customize: Builder.() -> Unit = {})
    suspend fun delete(tableName: String)
    suspend fun exists(tableName: String): Boolean
    fun list(): Flow<String>
}

