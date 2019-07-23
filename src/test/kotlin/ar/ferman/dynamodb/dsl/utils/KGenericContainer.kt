package ar.ferman.dynamodb.dsl.utils

import org.testcontainers.containers.GenericContainer

//Workaround for Kotlin type inference issue
class KGenericContainer(imageName: String) : GenericContainer<KGenericContainer>(imageName)