package ar.ferman.dynamodb.dsl

import org.assertj.core.api.BDDAssertions.assertThat
import org.junit.jupiter.api.Test


internal class TableDefinitionTest {
    data class UserRanking2(var userId: String? = null, var score: Int? = null)

    @Test
    internal fun `create item from value`() {
        val tableDef = createTable<UserRanking2>("prueba") {
            hashKey("user_id", String::class, UserRanking2::userId)
            sortKey("score", Int::class, UserRanking2::score)
        }

        val item = tableDef.toItem(UserRanking2("ferman", 100))

        println(item)

        val orig = tableDef.fromItem(item)

        println(orig)

        assertThat(orig.userId).isEqualTo("ferman")
        assertThat(orig.score).isEqualTo(100)
    }
}