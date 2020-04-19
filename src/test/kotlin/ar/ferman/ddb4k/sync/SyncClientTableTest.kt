package ar.ferman.ddb4k.sync

import ar.ferman.ddb4k.DynamoDbForTests
import ar.ferman.ddb4k.TableContractTest
import ar.ferman.ddb4k.example.data.ExampleTable
import ar.ferman.ddb4k.utils.KGenericContainer
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.BeforeEach
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import software.amazon.awssdk.services.dynamodb.DynamoDbClient

@Testcontainers
class SyncClientTableTest : TableContractTest() {

    companion object {
        @Container
        @JvmField
        val dynamoDbContainer: KGenericContainer = DynamoDbForTests.createContainer()
    }

    private lateinit var dynamoDbClient: DynamoDbClient

    @BeforeEach
    internal fun setUp() = runBlocking<Unit> {
        dynamoDbClient = DynamoDbForTests.createSyncClient(dynamoDbContainer)
        val tableDefinition = ExampleTable.createTableDefinition()
        SyncClientTables(dynamoDbClient).recreate(tableDefinition)
        table = SyncClientTable(dynamoDbClient, tableDefinition)
    }
}
