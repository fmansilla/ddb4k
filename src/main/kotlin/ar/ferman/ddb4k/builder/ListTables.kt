package ar.ferman.ddb4k.builder

import software.amazon.awssdk.services.dynamodb.model.ListTablesRequest

class ListTables {
    private val listTablesBuilder = ListTablesRequest.builder()

    fun build(lastTableName: String? = null): ListTablesRequest {
        return if (lastTableName == null) {
            listTablesBuilder.copy().build()
        } else {
            listTablesBuilder.copy().exclusiveStartTableName(lastTableName).build()
        }
    }
}