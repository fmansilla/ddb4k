package ar.ferman.dynamodb.dsl.async

import ar.ferman.dynamodb.dsl.DynamoDbForTests
import ar.ferman.dynamodb.dsl.TablesContractTest
import ar.ferman.dynamodb.dsl.utils.KGenericContainer
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.BeforeEach
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient

@Testcontainers
class AsyncClientTablesTest : TablesContractTest() {

    companion object {
        @Container
        @JvmField
        val dynamoDbContainer: KGenericContainer = DynamoDbForTests.createContainer()
    }

    private lateinit var dynamoDbClient: DynamoDbAsyncClient

    @BeforeEach
    internal fun setUp() = runBlocking<Unit> {
        dynamoDbClient = DynamoDbForTests.createAsyncClient(dynamoDbContainer)
        deleteAllTables()

        tables = AsyncClientTables(dynamoDbClient)
    }

    private fun deleteAllTables() {
        dynamoDbClient.listTablesPaginator().tableNames().subscribe { tableName ->
            dynamoDbClient.deleteTable { it.tableName(tableName) }.join()
        }.join()
    }
}
