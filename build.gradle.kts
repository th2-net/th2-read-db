plugins {
    kotlin("jvm") version "1.8.22" apply false
    id("com.google.protobuf") version "0.9.3" apply false
    id("com.exactpro.th2.gradle.grpc") version "0.0.3" apply false
    id("com.exactpro.th2.gradle.base") version "0.0.3"
    id("com.exactpro.th2.gradle.publish") version "0.0.3"
    id("com.exactpro.th2.gradle.component") version "0.0.3" apply false
}

allprojects {
    group = "com.exactpro.th2"
    version = project.findProperty("release_version") as String
    val suffix = project.findProperty("version_suffix") as String
    if (suffix.isNotEmpty()) {
        version = "$version-$suffix"
    }
}