package ar.ferman.dynamodb.dsl

import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class FlowExtensionsKtTest {

    @Test
    fun groupBy() = runBlocking<Unit> {
        val groupedNumbers = (1..10).asFlow().groupBy { if (it % 2 == 0) "even" else "odd" }

        assertThat(groupedNumbers)
            .hasSize(2)
            .containsEntry("even", listOf(2, 4, 6, 8, 10))
            .containsEntry("odd", listOf(1, 3, 5, 7, 9))
    }

    @Test
    fun groupingBy() = runBlocking<Unit> {
        val groupedNumbers = (1..10).asFlow().groupingBy { if (it % 2 == 0) "even" else "odd" }

        assertThat(groupedNumbers.toList())
            .hasSize(2)
            .contains("even" to listOf(2, 4, 6, 8, 10))
            .contains("odd" to listOf(1, 3, 5, 7, 9))
    }

    @Test
    fun groupingFirstFourElementsBy() = runBlocking<Unit> {
        val groupedNumbers = (1..10).asFlow().take(4).groupingBy { if (it % 2 == 0) "even" else "odd" }

        assertThat(groupedNumbers.toList())
            .hasSize(2)
            .contains("even" to listOf(2, 4))
            .contains("odd" to listOf(1, 3))
    }
}