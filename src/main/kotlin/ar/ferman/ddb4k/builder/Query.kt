package ar.ferman.ddb4k.builder

import ar.ferman.ddb4k.TableDefinition
import software.amazon.awssdk.services.dynamodb.model.QueryRequest

class Query<T : Any>(tableDefinition: TableDefinition<T>) {
    private val queryRequestBuilder = QueryRequest.builder().tableName(tableDefinition.tableName)

    internal var mapper: (Attributes) -> T = tableDefinition::fromItem
    var limit : Int? = null

    fun withConsistentRead() {
        queryRequestBuilder.consistentRead(true)
    }

    fun limit(maxItems: Int) {
        queryRequestBuilder.limit(maxItems)
        limit = maxItems
    }

    fun scanIndexForward(enabled : Boolean = true) {
        queryRequestBuilder.scanIndexForward(enabled)
    }

    fun mappingItems(itemMapper: (Attributes) -> T) {
        this.mapper = itemMapper
    }

    fun where(block: QueryCondition.() -> Unit) {
        block.invoke(QueryCondition(queryRequestBuilder))
    }

    fun custom(block: QueryRequest.Builder.() -> Unit) {
        block(queryRequestBuilder)
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