package ar.ferman.dynamodb.dsl.builder

import ar.ferman.dynamodb.dsl.TableDefinition
import ar.ferman.dynamodb.dsl.toAttributeValue
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest

class Update<T: Any>(tableDefinition: TableDefinition<T>) {
    private val updateItemRequest = UpdateItemRequest.builder().tableName(tableDefinition.tableName)

    private val updateExpressions = mutableMapOf<String, MutableList<String>>()
    private val updateExpressionAttributes = mutableMapOf<String, AttributeValue>()

    fun where(block: KeyCondition.() -> Unit) {
        val keyCondition = KeyCondition()
        block.invoke(keyCondition)
        updateItemRequest.key(keyCondition.build())
    }

    fun set(attributeName: String, value: String) {
        getUpdateExpressionsList("SET") += "$attributeName = :$attributeName"
        updateExpressionAttributes[":$attributeName"] = value.toAttributeValue()
    }

    fun set(attributeName: String, value: Number) {
        getUpdateExpressionsList("SET") += "$attributeName = :$attributeName"
        updateExpressionAttributes[":$attributeName"] = value.toAttributeValue()
    }

    fun remove(vararg attributeNames: String) {
        getUpdateExpressionsList("REMOVE").addAll(attributeNames)
    }

//        fun add() {
//            updateExpressions += ""
//        }

    private fun getUpdateExpressionsList(operation: String) =
        updateExpressions.computeIfAbsent(operation) { mutableListOf() }

    fun build(): UpdateItemRequest {
        val updateExpresion = updateExpressions
            .map { (type, expressions) -> "$type ${expressions.joinToString(separator = ", ")}"}
            .joinToString(" ")

        return updateItemRequest
            .expressionAttributeValues(updateExpressionAttributes)
            .updateExpression(updateExpresion)
            .build()
    }
}