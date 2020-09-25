# DDB4K

DDB4K (or Dynamo DB for Kotlin) is a set of abstractions intended to reduce the complexity of working with AWS Dynamo DB java API.

Latest stable version is `1.2.0`

## Changelog
#### 1.2.0
* Improve scan and query limit
* Allow multiple conditions in query where statement
#### 1.1.0
* Support request customization for Put, Get, GetBatch and Delete
* Allow query, scan and update request customization and add support for scan ordering in query
#### 1.0.0
* Initial release

## Download
You can download the dependencies using https://jitpack.io/

### Gradle
Add dependencies replacing `<version>` with the desired tag name or commit hash:
```groovy
dependencies {
    implementation 'com.github.fmansilla:ddb4k:<version>'
}
```
And make sure that you use the latest Kotlin version:
```groovy
buildscript {
    ext.kotlin_version = '1.3.70'
}
```
Make sure that you have either jitpack.io in the list of repositories:
```groovy
repository {
	maven { url 'https://jitpack.io' }
}
```
### Gradle Kotlin DSL
Add dependencies replacing `<version>` with the desired tag name or commit hash:
```kotlin
dependencies {
    implementation("com.github.fmansilla:ddb4k:<version>")
}
```

Make sure that you use the latest Kotlin version:
```kotlin
plugins {
    kotlin("jvm") version "1.3.70"
}
```
Make sure that you have either jitpack.io in the list of repositories:
```kotlin
repository {
    maven { url = uri("https://jitpack.io") }
}
```