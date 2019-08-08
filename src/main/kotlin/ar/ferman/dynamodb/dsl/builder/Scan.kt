package ar.ferman.dynamodb.dsl.builder

import ar.ferman.dynamodb.dsl.Attributes
import ar.ferman.dynamodb.dsl.TableDefinition
import software.amazon.awssdk.services.dynamodb.model.ScanRequest

class Scan<T>(tableDefinition: TableDefinition) {
    private val scanRequestBuilder = ScanRequest.builder().tableName(tableDefinition.name)
    internal lateinit var mapper: (Attributes) -> T

    fun attributes(vararg path: String) {
        //Puede utilizar cualquier nombre de atributo en una expresión de proyección, siempre y cuando el primer carácter sea a-z o A-Z y el segundo carácter (si lo hay) sea a-z, A-Z o 0-9
        //TODO validate path Description, RelatedItems[0], ProductReviews.FiveStar
        scanRequestBuilder.projectionExpression(path.joinToString(separator = ","))
    }

    fun limit(maxItems: Int){
        scanRequestBuilder.limit(maxItems)
    }

    fun mappingItems(itemMapper: (Attributes) -> T) {
        this.mapper = itemMapper
    }

    fun build(lastEvaluatedKey: Attributes): ScanRequest {
        val builder = if(lastEvaluatedKey.isNotEmpty()){
            scanRequestBuilder.copy().exclusiveStartKey(lastEvaluatedKey)
        }else {
            scanRequestBuilder
        }
        return builder.build()
    }
}