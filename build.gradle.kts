@file:Suppress("PropertyName")

import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    kotlin("jvm") version "1.3.21"
    `maven-publish`
}

object Versions {
    const val KOTLIN = "1.3.21"
    const val JUNIT = "5.4.2"
}

repositories {
    jcenter()
    mavenCentral()
}

dependencies {
    implementation(kotlin("stdlib-jdk8", Versions.KOTLIN))
    implementation(kotlin("reflect", Versions.KOTLIN))
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.3.0-RC")

    implementation("software.amazon.awssdk:dynamodb:2.7.2")

    loggingDependencies()
}

val compileKotlin by tasks.getting(KotlinCompile::class) {
    kotlinOptions.jvmTarget = "1.8"
}

tasks.withType<KotlinCompile> {
    kotlinOptions.jvmTarget = "1.8"
}

dependencies {
    testImplementation("org.assertj:assertj-core:3.11.1")
    testImplementation("org.junit.jupiter:junit-jupiter-api:${Versions.JUNIT}")
    testImplementation("org.junit.jupiter:junit-jupiter-engine:${Versions.JUNIT}")

    testImplementation("com.nhaarman.mockitokotlin2:mockito-kotlin:2.1.0")

    testImplementation("org.testcontainers:testcontainers:1.11.3")
    testImplementation("org.testcontainers:junit-jupiter:1.11.3")
    //testImplementation("org.jetbrains.kotlinx:kotlinx-coroutines-test:1.3.0-M1")
}

val compileTestKotlin by tasks.getting(KotlinCompile::class) {
    kotlinOptions.jvmTarget = "1.8"
}

tasks.withType<Test> {
    useJUnitPlatform()
}


val sourcesJar by tasks.registering(Jar::class) {
    classifier = "sources"
    from(sourceSets["main"].allSource)
}

publishing {
    repositories {
        maven {
            // change to point to your repo, e.g. http://my.org/repo
            url = uri("${projectDir.parentFile}/repo")
        }
    }
    publications {
        register("mavenJava", MavenPublication::class) {
            from(components["java"])
            artifact(sourcesJar.get())
        }
    }
}


fun DependencyHandlerScope.loggingDependencies() {
    implementation("org.slf4j:slf4j-api:1.7.26")
//    implementation("ch.qos.logback:logback-classic:1.1.3")

    implementation("org.apache.logging.log4j:log4j-api:2.12.0")
    implementation("org.apache.logging.log4j:log4j-core:2.12.0")
    implementation("org.apache.logging.log4j:log4j-slf4j-impl:2.12.0")
}