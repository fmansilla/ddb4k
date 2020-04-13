package ar.ferman.dynamodb.dsl.builder

import ar.ferman.dynamodb.dsl.Attributes
import ar.ferman.dynamodb.dsl.TableDefinition
import software.amazon.awssdk.services.dynamodb.model.QueryRequest

class Query<T : Any>(private val tableDefinition: TableDefinition<T>) {
    private val queryRequestBuilder = QueryRequest.builder().tableName(tableDefinition.tableName)

    internal var mapper: (Attributes) -> T = tableDefinition::fromItem

    fun withConsistentRead() {
        queryRequestBuilder.consistentRead(true)
    }

    fun limit(maxItems: Int) {
        queryRequestBuilder.limit(maxItems)
    }

    fun mappingItems(itemMapper: (Attributes) -> T) {
        this.mapper = itemMapper
    }

    fun where(block: QueryCondition.() -> Unit) {
        block.invoke(QueryCondition(queryRequestBuilder))
    }

    fun build(lastEvaluatedKey: Attributes): QueryRequest {
        val builder = if (lastEvaluatedKey.isNotEmpty()) {
            queryRequestBuilder.copy().exclusiveStartKey(lastEvaluatedKey)
        } else {
            queryRequestBuilder
        }
        return builder.build()
    }
}