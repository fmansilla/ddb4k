package ar.ferman.dynamodb.dsl

data class TableDefinition(
    val name: String,
    val hashKey: String,
    val sortKey: String? = null
)