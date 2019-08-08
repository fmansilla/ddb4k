package ar.ferman.dynamodb.dsl

enum class AttributeType {
    STRING,
    NUMBER
}

data class TableKeyAttribute(val name: String, val type: AttributeType)

data class TableDefinition(
    val name: String,
    val hashKey: TableKeyAttribute,
    val sortKey: TableKeyAttribute? = null
)