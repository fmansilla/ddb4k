package ar.ferman.dynamodb.dsl

import ar.ferman.dynamodb.dsl.example.ranking.UserRanking
import org.assertj.core.api.BDDAssertions.assertThat
import org.junit.jupiter.api.Test


internal class TableDefinitionTest {

    @Test
    internal fun `create item from value`() {
        val tableDef = createTable<UserRanking>("prueba") {
            hashKey("user_id", String::class, UserRanking::userId)
            sortKey("score", Int::class, UserRanking::score)
            attribute("att_int", Int::class, UserRanking::attInt)
            attribute("att_string", String::class, UserRanking::attString)
        }

        val userRanking = UserRanking("ferman", 100).apply {
            attInt = 25
            attString = "example"
        }
        val item = tableDef.toItem(userRanking)

        println(item)

        val userRankingFromItem = tableDef.fromItem(item)

        println(userRankingFromItem)

        assertThat(userRankingFromItem).isEqualTo(UserRanking("ferman", 100).apply {
            attInt = 25
            attString = "example"
        })
    }
}