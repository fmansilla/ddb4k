@file:Suppress("PropertyName")

import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    kotlin("jvm") version "1.3.70"
    `maven-publish`
}

object Versions {
    const val JUNIT = "5.5.2"
}

repositories {
    jcenter()
    mavenCentral()
}

dependencies {
    implementation(kotlin("stdlib", org.jetbrains.kotlin.config.KotlinCompilerVersion.VERSION))
    implementation(kotlin("reflect", org.jetbrains.kotlin.config.KotlinCompilerVersion.VERSION))
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.3.5")

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
    testImplementation("org.junit.jupiter:junit-jupiter-api:${Versions.JUNIT}")
    testImplementation("org.junit.jupiter:junit-jupiter-engine:${Versions.JUNIT}")
    testImplementation("org.junit.jupiter:junit-jupiter-params:${Versions.JUNIT}")
    testImplementation("org.assertj:assertj-core:3.15.0")
    testImplementation("org.jetbrains.kotlinx:kotlinx-coroutines-test:1.3.5")

    testImplementation("com.nhaarman.mockitokotlin2:mockito-kotlin:2.1.0")

    testImplementation("org.testcontainers:testcontainers:1.12.5")
    testImplementation("org.testcontainers:junit-jupiter:1.12.5")
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

    runtime("org.apache.logging.log4j:log4j-api:2.12.0")
    runtime("org.apache.logging.log4j:log4j-core:2.12.0")
    runtime("org.apache.logging.log4j:log4j-slf4j-impl:2.12.0")
}