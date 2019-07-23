package ar.ferman.dynamodb.dsl

import software.amazon.awssdk.services.dynamodb.model.AttributeValue

typealias Attributes = Map<String, AttributeValue>

interface ItemMapper<T> {

    fun toItem(value: T): Attributes

    fun fromItem(item: Attributes): T
}