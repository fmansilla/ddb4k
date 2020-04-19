package ar.ferman.ddb4k.builder

import ar.ferman.ddb4k.toAttributeValue
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import software.amazon.awssdk.services.dynamodb.model.ComparisonOperator
import software.amazon.awssdk.services.dynamodb.model.ComparisonOperator.*
import software.amazon.awssdk.services.dynamodb.model.Condition
import software.amazon.awssdk.services.dynamodb.model.QueryRequest

class QueryCondition(private val queryRequestBuilder: QueryRequest.Builder) {

    fun custom(builderBlock: QueryRequest.Builder.() -> Unit){
        builderBlock.invoke(queryRequestBuilder)
    }

    infix fun String.eq(value: String) = addCondition(this, EQ, value.toAttributeValue())
    infix fun String.eq(value: Number) = addCondition(this, EQ, value.toAttributeValue())

    infix fun String.le(value: String) = addCondition(this, LE, value.toAttributeValue())
    infix fun String.le(value: Number) = addCondition(this, LE, value.toAttributeValue())

    infix fun String.lt(value: String) = addCondition(this, LT, value.toAttributeValue())
    infix fun String.lt(value: Number) = addCondition(this, LT, value.toAttributeValue())

    infix fun String.ge(value: String) = addCondition(this, GE, value.toAttributeValue())
    infix fun String.ge(value: Number) = addCondition(this, GE, value.toAttributeValue())

    infix fun String.gt(value: String) = addCondition(this, GT, value.toAttributeValue())
    infix fun String.gt(value: Number) = addCondition(this, GT, value.toAttributeValue())

    infix fun String.ne(value: String) = addCondition(this, NE, value.toAttributeValue())
    infix fun String.ne(value: Number) = addCondition(this, NE, value.toAttributeValue())

    private fun addCondition(attribute: String, operator: ComparisonOperator, vararg values: AttributeValue) {
        queryRequestBuilder.keyConditions(
            mapOf(
                attribute to Condition.builder().comparisonOperator(
                    operator
                ).attributeValueList(*values).build()
            )
        )
    }
}