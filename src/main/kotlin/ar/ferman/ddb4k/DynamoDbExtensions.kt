package ar.ferman.ddb4k

import software.amazon.awssdk.services.dynamodb.model.AttributeValue


internal fun String.toAttributeValue() = AttributeValue.builder().s(this).build()
internal fun Collection<String>.toAttributeValue() = AttributeValue.builder().ss(this).build()
@JvmName("toNumberSetAttributeValue")
internal fun Collection<Number>.toAttributeValue(): AttributeValue = AttributeValue.builder().ns(map { toString() }).build()

internal fun Number.toAttributeValue() = AttributeValue.builder().n(toString()).build()
internal fun Boolean.toAttributeValue() = AttributeValue.builder().bool(this).build()