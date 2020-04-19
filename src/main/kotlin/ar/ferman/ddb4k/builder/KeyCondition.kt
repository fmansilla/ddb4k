package ar.ferman.ddb4k.builder

import ar.ferman.ddb4k.toAttributeValue
import software.amazon.awssdk.services.dynamodb.model.AttributeValue

class KeyCondition {

    private val attributes = mutableMapOf<String, AttributeValue>()

    infix fun String.eq(value: String) {
        attributes[this] = value.toAttributeValue()
    }

    infix fun String.eq(value: Number) {
        attributes[this] = value.toAttributeValue()
    }

    fun build(): Attributes {
        return attributes
    }
}