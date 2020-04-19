package ar.ferman.ddb4k.utils

import org.testcontainers.containers.GenericContainer

//Workaround for Kotlin type inference issue
class KGenericContainer(imageName: String) : GenericContainer<KGenericContainer>(imageName)