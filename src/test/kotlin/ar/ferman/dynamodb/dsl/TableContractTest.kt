package ar.ferman.dynamodb.dsl

import ar.ferman.dynamodb.dsl.example.ranking.UserRanking
import ar.ferman.dynamodb.dsl.example.ranking.UserRankingTable
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.BDDAssertions
import org.junit.jupiter.api.Test

abstract class TableContractTest {
    protected lateinit var table: Table<UserRanking>
    protected lateinit var itemMapper: ItemMapper<UserRanking>

    companion object {
        private const val USERNAME_1 = "username_1"
        private const val USERNAME_2 = "username_2"
        private const val USERNAME_3 = "username_3"
    }

    @Test
    fun `query for non existent elements returns empty`() = runBlocking<Unit> {
        val result = table.query {
            attributes(
                UserRankingTable.UserId,
                UserRankingTable.Score
            )
            mappingItems(itemMapper::fromItem)
            where {
                UserRankingTable.UserId eq USERNAME_1
            }
        }.toList()

        BDDAssertions.then(result).isEmpty()
    }


    @Test
    fun `query for single existent element returns it`() = runBlocking<Unit> {
        table.put(UserRanking(USERNAME_1, 5), itemMapper::toItem)
        table.put(UserRanking(USERNAME_2, 10), itemMapper::toItem)
        table.put(UserRanking(USERNAME_3, 15), itemMapper::toItem)

        val result = table.query {
            attributes(
                UserRankingTable.UserId,
                UserRankingTable.Score
            )
            mappingItems(itemMapper::fromItem)
            where {
                UserRankingTable.UserId eq USERNAME_1
            }
        }.toList()

        BDDAssertions.then(result).containsExactly(UserRanking(USERNAME_1, 5))
    }


    @Test
    fun `scan empty table does not return items`() = runBlocking<Unit> {
        val result = table.scan {
            attributes(
                UserRankingTable.UserId,
                UserRankingTable.Score
            )
            mappingItems(itemMapper::fromItem)
        }.toList()

        BDDAssertions.then(result).isEmpty()
    }

    @Test
    fun `scan non empty table return all items`() = runBlocking<Unit> {
        table.put(UserRanking(USERNAME_1, 5), itemMapper::toItem)
        table.put(UserRanking(USERNAME_2, 10), itemMapper::toItem)
        table.put(UserRanking(USERNAME_3, 15), itemMapper::toItem)

        val result = table.scan {
            attributes(
                UserRankingTable.UserId,
                UserRankingTable.Score
            )
            mappingItems(itemMapper::fromItem)
        }.toList()

        BDDAssertions.then(result).containsExactlyInAnyOrder(
            UserRanking(USERNAME_1, 5),
            UserRanking(USERNAME_2, 10),
            UserRanking(USERNAME_3, 15)
        )
    }

    @Test
    fun `update only some attributes`() = runBlocking<Unit> {
        table.put(UserRanking(USERNAME_1, 5), itemMapper::toItem)
        table.update {
            set(UserRankingTable.Score, 10)
            where {
                UserRankingTable.UserId eq USERNAME_1
            }
        }

        val result = table.scan {
            attributes(
                UserRankingTable.UserId,
                UserRankingTable.Score
            )
            mappingItems(itemMapper::fromItem)
        }.toList()

        BDDAssertions.then(result).containsExactlyInAnyOrder(UserRanking(USERNAME_1, 10))
    }
}