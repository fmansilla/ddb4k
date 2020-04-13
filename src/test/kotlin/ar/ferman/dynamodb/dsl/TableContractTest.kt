package ar.ferman.dynamodb.dsl

import ar.ferman.dynamodb.dsl.example.data.ExampleData
import ar.ferman.dynamodb.dsl.example.data.ExampleTable
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.BDDAssertions.then
import org.junit.jupiter.api.Test

abstract class TableContractTest {
    protected lateinit var table: Table<ExampleData>

    companion object {
        private const val USERNAME_1 = "username_1"
        private const val USERNAME_2 = "username_2"
        private const val USERNAME_3 = "username_3"
    }

    @Test
    fun `query for non existent elements returns empty`() = runBlocking<Unit> {
        val result = table.query {
            withConsistentRead()
            where {
                ExampleTable.UserId eq USERNAME_1
            }
        }.toList()

        then(result).isEmpty()
    }


    @Test
    fun `query for single existent element returns it`() = runBlocking<Unit> {
        table.put(ExampleData(USERNAME_1, 5))
        table.put(ExampleData(USERNAME_2, 10))
        table.put(ExampleData(USERNAME_3, 15))

        val result = table.query {
            where {
                ExampleTable.UserId eq USERNAME_1
            }
        }.toList()

        then(result).containsExactly(ExampleData(USERNAME_1, 5))
    }


    @Test
    fun `scan empty table does not return items`() = runBlocking<Unit> {
        val result = table.scan {
        }.toList()

        then(result).isEmpty()
    }

    @Test
    fun `scan non empty table return all items`() = runBlocking<Unit> {
        table.put(ExampleData(USERNAME_1, 5))
        table.put(ExampleData(USERNAME_2, 10))
        table.put(ExampleData(USERNAME_3, 15))

        val result = table.scan {
        }.toList()

        then(result).containsExactlyInAnyOrder(
            ExampleData(USERNAME_1, 5),
            ExampleData(USERNAME_2, 10),
            ExampleData(USERNAME_3, 15)
        )
    }

    @Test
    fun `update only some attributes`() = runBlocking<Unit> {
        table.put(ExampleData(USERNAME_1, 5))
        table.update {
            set(ExampleTable.Score, 10)
            where {
                ExampleTable.UserId eq USERNAME_1
            }
        }

        val result = table.scan {
        }.toList()

        then(result).containsExactlyInAnyOrder(ExampleData(USERNAME_1, 10))
    }
}