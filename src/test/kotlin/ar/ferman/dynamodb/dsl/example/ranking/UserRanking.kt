package ar.ferman.dynamodb.dsl.example.ranking


data class UserRanking(
    var userId: String?,
    var score: Int?,
    var attInt: Int?,
    var attString: String?
) {
    constructor(userId: String?, score: Int?) : this(userId, score, null, null)
    constructor() : this(null, null)
}
