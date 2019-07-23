package ar.ferman.dynamodb.dsl

import software.amazon.awssdk.services.dynamodb.model.QueryRequest

class Query<T>(tableDefinition: TableDefinition) {
    private val queryRequestBuilder = QueryRequest.builder().tableName(tableDefinition.name)

    internal lateinit var mapper: (Attributes) -> T

    fun attributes(vararg path: String) {
        //Puede utilizar cualquier nombre de atributo en una expresión de proyección, siempre y cuando el primer carácter sea a-z o A-Z y el segundo carácter (si lo hay) sea a-z, A-Z o 0-9
        //TODO validate path Description, RelatedItems[0], ProductReviews.FiveStar
        queryRequestBuilder.projectionExpression(path.joinToString(separator = ","))
    }

    fun mappingItems(itemMapper: (Attributes) -> T) {
        this.mapper = itemMapper
    }

    fun where(block: QueryCondition.() -> Unit) {
        block.invoke(QueryCondition(queryRequestBuilder))
    }

    fun build(lastEvaluatedKey: Attributes): QueryRequest {
        val builder = if(lastEvaluatedKey.isNotEmpty()){
            queryRequestBuilder.copy().exclusiveStartKey(lastEvaluatedKey)
        }else {
            queryRequestBuilder
        }
        return builder.build()
    }
}