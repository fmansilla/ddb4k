package ar.ferman.ddb4k

import ar.ferman.ddb4k.example.data.ExampleData
import ar.ferman.ddb4k.example.data.ExampleTable
import org.assertj.core.api.BDDAssertions.assertThat
import org.junit.jupiter.api.Test


internal class TableDefinitionTest {

    @Test
    internal fun `create item from value`() {
        val tableDef = ExampleTable.createTableDefinition()

        val userRanking = ExampleData("ferman", 100).apply {
            attString = "example"
            attBoolean = true
            attInt = 25
            attLong = 50L
            attFloat = 51.4f
            attDouble = 5.999998
            attStringList = listOf("d", "a", "b", "c")
        }
        val item = tableDef.toItem(userRanking)

        println(item)

        val userRankingFromItem = tableDef.fromItem(item)

        println(userRankingFromItem)

        assertThat(userRankingFromItem).isEqualTo(ExampleData("ferman", 100).apply {
            attString = "example"
            attBoolean = true
            attInt = 25
            attLong = 50L
            attFloat = 51.4f
            attDouble = 5.999998
            attStringList = listOf("d", "a", "b", "c")
        })
    }
}