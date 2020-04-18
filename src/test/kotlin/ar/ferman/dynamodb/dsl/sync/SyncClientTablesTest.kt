package ar.ferman.dynamodb.dsl.sync

import ar.ferman.dynamodb.dsl.DynamoDbForTests
import ar.ferman.dynamodb.dsl.TablesContractTest
import ar.ferman.dynamodb.dsl.utils.KGenericContainer
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.BeforeEach
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import software.amazon.awssdk.services.dynamodb.DynamoDbClient

@Testcontainers
class SyncClientTablesTest : TablesContractTest() {

    companion object {
        @Container
        @JvmField
        val dynamoDbContainer: KGenericContainer = DynamoDbForTests.createContainer()
    }

    private lateinit var dynamoDbClient: DynamoDbClient

    @BeforeEach
    internal fun setUp() = runBlocking<Unit> {
        dynamoDbClient = DynamoDbForTests.createSyncClient(dynamoDbContainer)
        deleteAllTables()
        tables = SyncClientTables(dynamoDbClient)
    }

    private fun deleteAllTables() {
        dynamoDbClient.listTablesPaginator().tableNames().forEach { tableName ->
            dynamoDbClient.deleteTable { it.tableName(tableName) }
        }
    }
}
