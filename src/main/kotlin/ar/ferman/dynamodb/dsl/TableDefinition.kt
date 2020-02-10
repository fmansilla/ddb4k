package ar.ferman.dynamodb.dsl

import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import kotlin.reflect.KClass
import kotlin.reflect.KFunction2
import kotlin.reflect.KMutableProperty
import kotlin.reflect.full.createInstance
import kotlin.reflect.full.isSubclassOf

enum class AttributeType(
    private val builderType: KFunction2<AttributeValue.Builder, String, AttributeValue.Builder>
) {
    STRING(AttributeValue.Builder::s),
    NUMBER(AttributeValue.Builder::n);

    fun buildAttributeValue(value: Any): AttributeValue {
        val builder = AttributeValue.builder()
        builderType.invoke(builder, convertPrimitiveValue(value))
        return builder.build()
    }

    private fun convertPrimitiveValue(value: Any): String {
        return when (this) {
            STRING -> value as String
            NUMBER -> (value as Number).toInt().toString() //TODO support all primitive types
        }
    }

    fun primitiveValueFrom(attributeValue: AttributeValue?): Any? {
        if (attributeValue == null) return null
        return when (this) {
            STRING -> attributeValue.s()
            NUMBER -> attributeValue.n().toInt()
        }
    }

    companion object {
        fun from(type: KClass<*>): AttributeType = when {
            type == String::class -> STRING
            type.isSubclassOf(Number::class) -> NUMBER
            else -> throw RuntimeException("Unsupported type")//TODO custom exception
        }
    }
}

data class AttributeDefinition(val name: String, val type: AttributeType, val property: KMutableProperty<*>)

inline fun <reified T : Any> createTable(
    tableName: String,
    noinline initializer: DefBuilder.() -> Unit
): TableDefinition<T> {
    return TableDefinition(tableName, T::class, initializer)
}

interface DefBuilder {
    fun <E : Any> hashKey(attributeName: String, type: KClass<E>, property: KMutableProperty<E?>)
    fun <E : Any> sortKey(attributeName: String, type: KClass<E>, property: KMutableProperty<E?>)
    fun <E : Any> attribute(attributeName: String, type: KClass<E>, property: KMutableProperty<E?>)
}

class TableDefinition<T : Any>(
    val tableName: String,
    private val type: KClass<T>,
    initialization: DefBuilder.() -> Unit
) : DefBuilder {
    var hashKey: AttributeDefinition? = null
        private set
    var sortKey: AttributeDefinition? = null
        private set
    val attributes = mutableListOf<AttributeDefinition>()

    init {
        initialization.invoke(this)
    }

    override fun <E : Any> hashKey(attributeName: String, type: KClass<E>, property: KMutableProperty<E?>) {
        val attributeType = AttributeType.from(type)
        hashKey = AttributeDefinition(attributeName, attributeType, property)
    }

    override fun <E : Any> sortKey(attributeName: String, type: KClass<E>, property: KMutableProperty<E?>) {
        val attributeType = AttributeType.from(type)
        sortKey = AttributeDefinition(attributeName, attributeType, property)
    }

    override fun <E : Any> attribute(attributeName: String, type: KClass<E>, property: KMutableProperty<E?>) {
        val attributeType = AttributeType.from(type)
        attributes.add(AttributeDefinition(attributeName, attributeType, property))
    }

    fun toItem(value: T): Map<String, AttributeValue> {
        return mutableMapOf<String, AttributeValue>().apply {
            putAttributeIfAvailable(hashKey!!, value)
            if (sortKey != null) putAttributeIfAvailable(sortKey!!, value)
            attributes.forEach { putAttributeIfAvailable(it, value) }
        }
    }

    fun fromItem(item: Map<String, AttributeValue>): T {
        val value = type.createInstance()
        hashKey?.setPropertyValueIfAvailable(item, value)
        sortKey?.setPropertyValueIfAvailable(item, value)
        attributes.forEach {
            it.setPropertyValueIfAvailable(item, value)

        }
        return value
    }

    private fun AttributeDefinition.setPropertyValueIfAvailable(
        item: Map<String, AttributeValue>,
        value: T
    ) {
        property.setter.call(value, type.primitiveValueFrom(item[name]))
    }

    private fun MutableMap<String, AttributeValue>.putAttributeIfAvailable(
        attributeDefinition: AttributeDefinition,
        value: Any
    ) {
        with(attributeDefinition) {
            val primitiveAttributeValue = property.getter.call(value)
            if (primitiveAttributeValue != null) {
                put(name, type.buildAttributeValue(primitiveAttributeValue))
            }
        }
    }

}
