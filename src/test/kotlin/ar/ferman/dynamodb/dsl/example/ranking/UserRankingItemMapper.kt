package ar.ferman.dynamodb.dsl.example.ranking

import ar.ferman.dynamodb.dsl.ItemMapper
import software.amazon.awssdk.services.dynamodb.model.AttributeValue

class UserRankingItemMapper : ItemMapper<UserRanking> {
    override fun toItem(value: UserRanking): Map<String, AttributeValue> {
        return mapOf(
            "user_id" to AttributeValue.builder().s(value.userId).build(),
            "score" to AttributeValue.builder().n("${value.score}").build()
        )
    }

    override fun fromItem(item: Map<String, AttributeValue>): UserRanking {
        return UserRanking(item["user_id"]!!.s(), item["score"]!!.n().toInt())
    }

}