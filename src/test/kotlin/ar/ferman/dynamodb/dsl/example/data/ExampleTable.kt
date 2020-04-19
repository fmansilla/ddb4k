package ar.ferman.dynamodb.dsl.example.data

import ar.ferman.dynamodb.dsl.TableDefinition
import ar.ferman.dynamodb.dsl.createTable

object ExampleTable {
    val TableName = "example_table"

    val UserId = "user_id"
    val Score = "score"
    val StringAttribute = "att_string"
    val BooleanAttribute = "att_boolean"
    val IntAttribute = "att_int"
    val LongAttribute = "att_long"
    val FloatAttribute = "att_float"
    val DoubleAttribute = "att_double"
    val StringListAttribute = "att_string_list"

    fun createTableDefinition(): TableDefinition<ExampleData> = createTable(TableName) {
        hashKey(UserId, String::class, ExampleData::userId)
        sortKey(Score, Int::class, ExampleData::score)

        attribute(StringAttribute, String::class, ExampleData::attString)
        attribute(BooleanAttribute, Boolean::class, ExampleData::attBoolean)
        attribute(IntAttribute, Int::class, ExampleData::attInt)
        attribute(LongAttribute, Long::class, ExampleData::attLong)
        attribute(FloatAttribute, Float::class, ExampleData::attFloat)
        attribute(DoubleAttribute, Double::class, ExampleData::attDouble)
        listAttribute(StringListAttribute, ExampleData::attStringList)
    }
}