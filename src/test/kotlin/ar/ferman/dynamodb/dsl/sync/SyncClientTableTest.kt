package ar.ferman.dynamodb.dsl.sync

import ar.ferman.dynamodb.dsl.DynamoDbForTests
import ar.ferman.dynamodb.dsl.TableContractTest
import ar.ferman.dynamodb.dsl.createTable
import ar.ferman.dynamodb.dsl.example.ranking.UserRanking
import ar.ferman.dynamodb.dsl.example.ranking.UserRankingTable
import ar.ferman.dynamodb.dsl.recreate
import ar.ferman.dynamodb.dsl.utils.KGenericContainer
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
        table = SyncClientTable(
            dynamoDbClient,
            createTable(UserRankingTable.TableName) {
                hashKey(UserRankingTable.UserId, String::class, UserRanking::userId)
                attribute(UserRankingTable.Score, Int::class, UserRanking::score)
            }
        )
        table.recreate()
    }
}
