package ar.ferman.ddb4k.builder

import ar.ferman.ddb4k.TableDefinition
import software.amazon.awssdk.services.dynamodb.model.ScanRequest

class Scan<T : Any>(tableDefinition: TableDefinition<T>) {
    private val scanRequestBuilder = ScanRequest.builder().tableName(tableDefinition.tableName)
    internal var mapper: (Attributes) -> T = tableDefinition::fromItem
    private var limit : Int? = null

    fun currentLimit() = limit ?: Int.MAX_VALUE

    fun withConsistentRead() {
        scanRequestBuilder.consistentRead(true)
    }

    fun limit(maxItems: Int) {
        scanRequestBuilder.limit(maxItems)
        limit = maxItems
    }

    fun mappingItems(itemMapper: (Attributes) -> T) {
        this.mapper = itemMapper
    }

    fun custom(block: ScanRequest.Builder.() -> Unit) {
        block(scanRequestBuilder)
    }

    fun build(lastEvaluatedKey: Attributes): ScanRequest {
        val builder = if (lastEvaluatedKey.isNotEmpty()) {
            scanRequestBuilder.copy().exclusiveStartKey(lastEvaluatedKey)
        } else {
            scanRequestBuilder.copy()
        }
        return builder.build()
    }
}