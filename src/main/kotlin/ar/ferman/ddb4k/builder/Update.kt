package ar.ferman.ddb4k.builder

import ar.ferman.ddb4k.TableDefinition
import ar.ferman.ddb4k.toAttributeValue
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import software.amazon.awssdk.services.dynamodb.model.ScanRequest
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

    fun add(attributeName: String, value: Number) {
        getUpdateExpressionsList("ADD") += "$attributeName :$attributeName"
        updateExpressionAttributes[":$attributeName"] = value.toAttributeValue()
    }

    fun add(attributeName: String, values: List<String>) {
        getUpdateExpressionsList("ADD") += "$attributeName :$attributeName"
        updateExpressionAttributes[":$attributeName"] = values.toAttributeValue()
    }

    fun delete(attributeName: String, values: List<String>) {
        getUpdateExpressionsList("DELETE") += "$attributeName :$attributeName"
        updateExpressionAttributes[":$attributeName"] = values.toAttributeValue()
    }

    fun custom(block: UpdateItemRequest.Builder.() -> Unit) {
        block(updateItemRequest)
    }

    private fun getUpdateExpressionsList(operation: String) =
        updateExpressions.computeIfAbsent(operation) { mutableListOf() }

    fun build(): UpdateItemRequest {
        val updateExpression = updateExpressions
            .map { (type, expressions) -> "$type ${expressions.joinToString(separator = ", ")}"}
            .joinToString(" ")

        return updateItemRequest
            .expressionAttributeValues(updateExpressionAttributes)
            .updateExpression(updateExpression)
            .build()
    }
}