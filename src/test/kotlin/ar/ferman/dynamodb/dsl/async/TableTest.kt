package ar.ferman.dynamodb.dsl.async

import ar.ferman.dynamodb.dsl.DynamoDbForTests
import ar.ferman.dynamodb.dsl.TableDefinition
import ar.ferman.dynamodb.dsl.example.ranking.*
import ar.ferman.dynamodb.dsl.utils.KGenericContainer
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.BDDAssertions.then
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient

@Testcontainers
class TableTest {

    companion object {
        @Container
        @JvmField
        val dynamoDb: KGenericContainer = DynamoDbForTests.createContainer()
    }

    private lateinit var dynamoDbClient: DynamoDbAsyncClient
    private lateinit var table: Table
    private lateinit var itemMapper: UserRankingItemMapper

    @BeforeEach
    internal fun setUp() = runBlocking {
        dynamoDbClient = DynamoDbForTests.createAsyncClient()
        table = Table(
            dynamoDbClient,
            TableDefinition(
                UserRankingTable.TableName,
                UserRankingTable.UserId
            )
        )
        itemMapper = UserRankingItemMapper()

        dynamoDbClient.deleteUserRankingTable()
        dynamoDbClient.createUserRankingTable()
    }

    @Test
    fun `query for non existent elements returns empty`() = runBlocking<Unit> {
        val result = table.query<UserRanking> {
            attributes(
                UserRankingTable.UserId,
                UserRankingTable.Score
            )
            mappingItems(itemMapper::fromItem)
            where {
                UserRankingTable.UserId eq "a"
            }
        }.toList()

        then(result).isEmpty()
    }

    @Test
    fun `query for single existent element returns it`() = runBlocking<Unit> {
        table.put(UserRanking("a", 5), itemMapper::toItem)
        table.put(UserRanking("b", 10), itemMapper::toItem)
        table.put(UserRanking("c", 15), itemMapper::toItem)

        val result = table.query<UserRanking> {
            attributes(
                UserRankingTable.UserId,
                UserRankingTable.Score
            )
            mappingItems(itemMapper::fromItem)
            where {
                UserRankingTable.UserId eq "a"
            }
        }.toList()

        then(result).containsExactly(UserRanking("a", 5))
    }


    @Test
    fun `scan empty table does not return items`() = runBlocking<Unit> {
        val result = table.scan<UserRanking> {
            attributes(
                UserRankingTable.UserId,
                UserRankingTable.Score
            )
            mappingItems(itemMapper::fromItem)
        }.toList()

        then(result).isEmpty()
    }

    @Test
    fun `scan non empty table return all items`() = runBlocking<Unit> {
        table.put(UserRanking("a", 5), itemMapper::toItem)
        table.put(UserRanking("b", 10), itemMapper::toItem)
        table.put(UserRanking("c", 15), itemMapper::toItem)

        val result = table.scan<UserRanking> {
            attributes(
                UserRankingTable.UserId,
                UserRankingTable.Score
            )
            mappingItems(itemMapper::fromItem)
        }.toList()

        then(result).containsExactlyInAnyOrder(UserRanking("a", 5), UserRanking("b", 10), UserRanking("c", 15))

//        Assertions.assertThat(repository.find("a")).isNull()
//
//        repository.put(UserRanking("a", 5))
//
//        Assertions.assertThat(repository.find("a")).isEqualTo(UserRanking("a", 5))
//
//        Assertions.assertThat(repository.findAll()).containsExactly(UserRanking("a", 5))
    }


}
