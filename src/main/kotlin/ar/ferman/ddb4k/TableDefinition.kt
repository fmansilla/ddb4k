package ar.ferman.ddb4k

import ar.ferman.ddb4k.AttributeType.STRING_LIST
import ar.ferman.ddb4k.error.InvalidTypeException
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import kotlin.reflect.KClass
import kotlin.reflect.KMutableProperty
import kotlin.reflect.full.createInstance
import kotlin.reflect.full.isSubclassOf

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
    fun listAttribute(attributeName: String, property: KMutableProperty<List<String>?>)
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

    private val attributes = mutableListOf<AttributeDefinition>()

    val allAttributes: List<AttributeDefinition>
        get() = mutableListOf<AttributeDefinition>().apply {
            addAll(attributes)
            if (hashKey != null) add(hashKey!!)
            if (sortKey != null) add(sortKey!!)
        }

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
        if (type.isSubclassOf(Collection::class)) throw InvalidTypeException("Collection type not allowed, you can use listAttribute(...)")

        val attributeType = AttributeType.from(type)
        attributes.add(AttributeDefinition(attributeName, attributeType, property))
    }

    override fun listAttribute(attributeName: String, property: KMutableProperty<List<String>?>) {
        attributes.add(AttributeDefinition(attributeName, STRING_LIST, property))
    }

    internal fun toItem(value: T): Map<String, AttributeValue> {
        return mutableMapOf<String, AttributeValue>().apply {
            putAttributeIfAvailable(hashKey!!, value)
            if (sortKey != null) putAttributeIfAvailable(sortKey!!, value)
            attributes.forEach { putAttributeIfAvailable(it, value) }
        }
    }

    internal fun toItemKey(value: T): Map<String, AttributeValue> {
        return mutableMapOf<String, AttributeValue>().apply {
            putAttributeIfAvailable(hashKey!!, value)
            if (sortKey != null) putAttributeIfAvailable(sortKey!!, value)
        }
    }

    internal fun fromItem(item: Map<String, AttributeValue>): T {
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
