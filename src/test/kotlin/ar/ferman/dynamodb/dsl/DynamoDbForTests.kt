package ar.ferman.dynamodb.dsl

import ar.ferman.dynamodb.dsl.utils.KGenericContainer
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import java.net.URI

object DynamoDbForTests {
    const val DYNAMO_PORT = 8000

    fun createAsyncClient() = DynamoDbAsyncClient.builder()
        .region(Region.US_EAST_1)
        .endpointOverride(URI("http://localhost:$DYNAMO_PORT"))
        .credentialsProvider { AwsBasicCredentials.create("access", "secret") }.build()

    fun createSyncClient() = DynamoDbClient.builder()
        .region(Region.US_EAST_1)
        .endpointOverride(URI("http://localhost:$DYNAMO_PORT"))
        .credentialsProvider { AwsBasicCredentials.create("access", "secret") }.build()

    fun createContainer(): KGenericContainer {
        return KGenericContainer("amazon/dynamodb-local:1.11.477").withExposedPorts(DYNAMO_PORT)
    }
}

