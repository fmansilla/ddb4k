package ar.ferman.dynamodb.dsl.example.data


data class ExampleData(
    var userId: String? = null,
    var score: Int? = null,
    var attString: String? = null,
    var attBoolean: Boolean? = null,
    var attInt: Int? = null,
    var attLong: Long? = null,
    var attFloat: Float? = null,
    var attDouble: Double? = null,
    var attStringList: List<String>? = null
)
