package ar.ferman.dynamodb.dsl

import ar.ferman.dynamodb.dsl.error.InvalidTypeException
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType
import kotlin.reflect.KClass
import kotlin.reflect.full.isSubclassOf

enum class AttributeType(
    private val builderType: (AttributeValue.Builder, Any) -> AttributeValue.Builder
) {
    STRING({ builder, value -> builder.s(value as String) }),
    INT({ builder, value -> builder.n(value as String) }),
    LONG({ builder, value -> builder.n(value as String) }),
    FLOAT({ builder, value -> builder.n(value as String) }),
    DOUBLE({ builder, value -> builder.n(value as String) }),
    BOOLEAN({ builder, value -> builder.bool(value as Boolean) }),
    @Suppress("UNCHECKED_CAST")
    STRING_LIST({ builder, value -> builder.ss(value as List<String>) });

    internal fun buildAttributeValue(value: Any): AttributeValue {
        val builder = AttributeValue.builder()
        builderType.invoke(builder, convertPrimitiveValue(value))
        return builder.build()
    }

    fun toScalarAttributeType(): ScalarAttributeType {
        return when (this) {
            STRING -> ScalarAttributeType.S
            INT, LONG, FLOAT, DOUBLE -> ScalarAttributeType.N
            else -> throw InvalidTypeException("Invalid key attribute type, only String or Number are allowed")
        }
    }

    private fun convertPrimitiveValue(value: Any): Any {
        return when (this) {
            INT, LONG, FLOAT, DOUBLE -> value.toString()
            else -> value
        }
    }

    fun primitiveValueFrom(attributeValue: AttributeValue?): Any? {
        if (attributeValue == null) return null
        return when (this) {
            STRING -> attributeValue.s()
            INT -> attributeValue.n().toInt()
            LONG -> attributeValue.n().toLong()
            FLOAT -> attributeValue.n().toFloat()
            DOUBLE -> attributeValue.n().toDouble()
            BOOLEAN -> attributeValue.bool()
            STRING_LIST -> attributeValue.ss()
        }
    }

    companion object {
        fun from(type: KClass<*>): AttributeType = when {
            type == String::class -> STRING
            type.isSubclassOf(Int::class) -> INT
            type.isSubclassOf(Long::class) -> LONG
            type.isSubclassOf(Float::class) -> FLOAT
            type.isSubclassOf(Number::class) -> DOUBLE
            type.isSubclassOf(Boolean::class) -> BOOLEAN
            else -> throw InvalidTypeException("Unsupported type: $type")
        }
    }
}