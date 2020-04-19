package ar.ferman.ddb4k

import ar.ferman.ddb4k.example.data.ExampleTable
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.BDDAssertions.then
import org.junit.jupiter.api.Test

abstract class TablesContractTest {

    protected lateinit var tables: Tables

    @Test
    fun `check table existence when empty`() = runBlocking<Unit> {
        val exists = tables.exists(ExampleTable.TableName)

        then(exists).isFalse()
    }

    @Test
    fun `check table existence when already created`() = runBlocking<Unit> {
        tables.create(ExampleTable.createTableDefinition())

        val exists = tables.exists(ExampleTable.TableName)

        then(exists).isTrue()
    }

    @Test
    fun `create table and then delete it`() = runBlocking<Unit> {
        assertThat(tables.exists(ExampleTable.TableName)).isFalse()
        tables.create(ExampleTable.createTableDefinition())
        assertThat(tables.exists(ExampleTable.TableName)).isTrue()

        tables.delete(ExampleTable.TableName)
        assertThat(tables.exists(ExampleTable.TableName)).isFalse()
    }

    @Test
    fun `list tables when empty db`() = runBlocking<Unit> {
        val allTableNames = tables.list().toList()

        then(allTableNames).isEmpty()
    }

    @Test
    fun `create table and list existent tables`() = runBlocking<Unit> {
        tables.create(ExampleTable.createTableDefinition())

        val allTableNames = tables.list().toList()

        then(allTableNames).containsExactly(ExampleTable.TableName)
    }
}