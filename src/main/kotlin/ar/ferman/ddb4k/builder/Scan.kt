package ar.ferman.ddb4k.builder

import ar.ferman.ddb4k.TableDefinition
import software.amazon.awssdk.services.dynamodb.model.ScanRequest

class Scan<T : Any>(tableDefinition: TableDefinition<T>) {
    private val scanRequestBuilder = ScanRequest.builder().tableName(tableDefinition.tableName)
    internal var mapper: (Attributes) -> T = tableDefinition::fromItem

    fun withConsistentRead() {
        scanRequestBuilder.consistentRead(true)
    }

    fun limit(maxItems: Int) {
        scanRequestBuilder.limit(maxItems)
    }

    fun mappingItems(itemMapper: (Attributes) -> T) {
        this.mapper = itemMapper
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