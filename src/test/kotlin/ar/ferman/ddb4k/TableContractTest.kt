package ar.ferman.ddb4k

import ar.ferman.ddb4k.example.data.ExampleData
import ar.ferman.ddb4k.example.data.ExampleTable
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.BDDAssertions.assertThat
import org.assertj.core.api.BDDAssertions.then
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.fail
import java.lang.RuntimeException

abstract class TableContractTest {
    protected lateinit var table: Table<ExampleData>

    companion object {
        private const val USERNAME_1 = "username_1"
        private const val USERNAME_2 = "username_2"
        private const val USERNAME_3 = "username_3"
    }

    @Test
    fun `get single item by key`() = runBlocking<Unit> {
        table.put(ExampleData(USERNAME_1, 5, attString = "expected value"))

        val result = table.get(ExampleData(USERNAME_1, 5))

        then(result).isEqualTo(ExampleData(USERNAME_1, 5, attString = "expected value"))
    }

    @Test
    fun `get non existing item by key`() = runBlocking<Unit> {
        val result = table.get(ExampleData(USERNAME_1, 5))

        then(result).isNull()
    }

    @Test
    fun `deny put on existing item using conditional expression`() = runBlocking<Unit> {
        table.put(ExampleData(USERNAME_1, 5, attString = "expected value"))

        kotlin.runCatching {
            table.put(ExampleData(USERNAME_1, 5, attString = "expected value")) {
                custom {
                    conditionExpression("attribute_not_exists(user_id) AND attribute_not_exists(score)")
                }
            }
        }
            .onSuccess { fail { "Should fail" } }
            .onFailure { assertThat(it).isInstanceOf(RuntimeException::class.java) }
    }

    @Test
    fun `delete existent item`() = runBlocking<Unit> {
        table.put(ExampleData(USERNAME_1, 5, attString = "expected value"))

        table.delete(ExampleData(USERNAME_1, 5)) {}

        then(table.scan().toList()).isEmpty()
    }

    @Test
    fun `delete non existent item does not affect other items`() = runBlocking<Unit> {
        table.put(ExampleData(USERNAME_1, 5, attString = "expected value"))

        table.delete(ExampleData(USERNAME_2, 10)) {}

        then(table.scan().toList()).containsExactly(ExampleData(USERNAME_1, 5, attString = "expected value"))
    }


    @Test
    fun `get multiple items by key returns only found`() = runBlocking<Unit> {
        table.put(ExampleData(USERNAME_1, 5, attString = "expected value"))
        table.put(ExampleData(USERNAME_2, 10, attString = "other value"))

        val result = table.get(
            setOf(
                ExampleData(USERNAME_1, 5),
                ExampleData(USERNAME_2, 10),
                ExampleData("missing", 50)
            )
        )

        then(result).containsExactlyInAnyOrder(
            ExampleData(USERNAME_1, 5, attString = "expected value"),
            ExampleData(USERNAME_2, 10, attString = "other value")
        )
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
    fun `query with complex key returns matching elements`() = runBlocking<Unit> {
        table.put(ExampleData(USERNAME_1, 5))
        table.put(ExampleData(USERNAME_1, 10))
        table.put(ExampleData(USERNAME_1, 15))
        table.put(ExampleData(USERNAME_1, 20))
        table.put(ExampleData(USERNAME_2, 20))

        val result = table.query {
            where {
                ExampleTable.UserId eq USERNAME_1
                ExampleTable.Score ge 15
            }
        }.toList()

        then(result).containsExactly(
            ExampleData(USERNAME_1, 15),
            ExampleData(USERNAME_1, 20)
        )
    }

    @Test
    fun `query for multiple existent elements in reverse order`() = runBlocking<Unit> {
        table.put(ExampleData(USERNAME_1, 5))
        table.put(ExampleData(USERNAME_1, 10))
        table.put(ExampleData(USERNAME_2, 15))

        val result = table.query {
            where {
                ExampleTable.UserId eq USERNAME_1
            }
            scanIndexForward(false)
        }.toList()

        then(result).containsExactly(
            ExampleData(USERNAME_1, 10),
            ExampleData(USERNAME_1, 5)
        )
    }

    @Test
    fun `query for multiple existent elements in reverse order with limit`() = runBlocking<Unit> {
        repeat(10) {
            table.put(ExampleData(USERNAME_1, it + 1))
            table.put(ExampleData(USERNAME_2, 100 * (it + 1)))
        }

        val result = table.query {
            where {
                ExampleTable.UserId eq USERNAME_1
            }
            limit(2)
            scanIndexForward(false)
        }.toList()

        then(result).containsExactly(
            ExampleData(USERNAME_1, 10),
            ExampleData(USERNAME_1, 9)
        )
    }

    @Test
    fun `scan empty table does not return items`() = runBlocking<Unit> {
        val result = table.scan().toList()

        then(result).isEmpty()
    }

    @Test
    fun `scan non empty table return all items`() = runBlocking<Unit> {
        table.put(ExampleData(USERNAME_1, 5))
        table.put(ExampleData(USERNAME_2, 10))
        table.put(ExampleData(USERNAME_3, 15))

        val result = table.scan().toList()

        then(result).containsExactlyInAnyOrder(
            ExampleData(USERNAME_1, 5),
            ExampleData(USERNAME_2, 10),
            ExampleData(USERNAME_3, 15)
        )
    }

    @Test
    fun `scan non empty table with limit return expected items`() = runBlocking<Unit> {
        table.put(ExampleData(USERNAME_1, 5))
        table.put(ExampleData(USERNAME_2, 10))
        table.put(ExampleData(USERNAME_3, 15))

        val result = table.scan {
            limit(2)
        }.toList()

        then(result).hasSize(2).containsAnyOf(
            ExampleData(USERNAME_1, 5),
            ExampleData(USERNAME_2, 10),
            ExampleData(USERNAME_3, 15)
        )
    }

    @Test
    fun `update only some attributes`() = runBlocking<Unit> {
        table.put(ExampleData(USERNAME_1, 5))
        table.update {
            set(ExampleTable.IntAttribute, 10)
            where {
                ExampleTable.UserId eq USERNAME_1
                ExampleTable.Score eq 5
            }
        }

        val result = table.scan().toList()

        then(result).containsExactlyInAnyOrder(ExampleData(USERNAME_1, 5, attInt = 10))
    }

    @Test
    fun `update set on empty attribute`() = runBlocking<Unit> {
        table.put(ExampleData(USERNAME_1, 5))
        table.update {
            add(ExampleTable.StringListAttribute, listOf("c", "d", "e"))
            where {
                ExampleTable.UserId eq USERNAME_1
                ExampleTable.Score eq 5
            }
        }

        val result = table.scan().toList()

        then(result).containsExactlyInAnyOrder(
            ExampleData(
                USERNAME_1,
                5,
                attStringList = listOf("c", "d", "e")
            )
        )
    }

    @Test
    fun `update set adding some elements`() = runBlocking<Unit> {
        table.put(ExampleData(USERNAME_1, 5, attStringList = listOf("a", "b")))
        table.update {
            add(ExampleTable.StringListAttribute, listOf("c", "d", "e"))
            where {
                ExampleTable.UserId eq USERNAME_1
                ExampleTable.Score eq 5
            }
        }

        val result = table.scan().toList()

        then(result).containsExactlyInAnyOrder(
            ExampleData(
                USERNAME_1,
                5,
                attStringList = listOf("a", "b", "c", "d", "e")
            )
        )
    }

    @Test
    fun `update set removing some elements`() = runBlocking<Unit> {
        table.put(ExampleData(USERNAME_1, 5, attStringList = listOf("a", "b", "c", "d")))
        table.update {
            delete(ExampleTable.StringListAttribute, listOf("b", "c"))
            where {
                ExampleTable.UserId eq USERNAME_1
                ExampleTable.Score eq 5
            }
        }

        val result = table.scan().toList()

        then(result).containsExactlyInAnyOrder(ExampleData(USERNAME_1, 5, attStringList = listOf("a", "d")))
    }

    @Test
    fun `increment numeric attribute`() = runBlocking<Unit> {
        table.put(ExampleData(USERNAME_1, 5, attInt = 10))
        table.update {
            add(ExampleTable.IntAttribute, 5)
            where {
                ExampleTable.UserId eq USERNAME_1
                ExampleTable.Score eq 5
            }
        }

        val result = table.scan().toList()

        then(result).containsExactlyInAnyOrder(ExampleData(USERNAME_1, 5, attInt = 15))
    }

    @Test
    fun `decrement numeric attribute`() = runBlocking<Unit> {
        table.put(ExampleData(USERNAME_1, 5, attInt = 10))
        table.update {
            add(ExampleTable.IntAttribute, -3)
            where {
                ExampleTable.UserId eq USERNAME_1
                ExampleTable.Score eq 5
            }
        }

        val result = table.scan().toList()

        then(result).containsExactlyInAnyOrder(ExampleData(USERNAME_1, 5, attInt = 7))
    }
}