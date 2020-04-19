package ar.ferman.ddb4k.sync

import ar.ferman.ddb4k.DynamoDbForTests
import ar.ferman.ddb4k.TablesContractTest
import ar.ferman.ddb4k.utils.KGenericContainer
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
