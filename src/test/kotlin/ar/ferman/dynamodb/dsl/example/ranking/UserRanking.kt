package ar.ferman.dynamodb.dsl.example.ranking


data class UserRanking(var userId: String?, var score: Int?) {
    constructor() : this(null, null)
}
