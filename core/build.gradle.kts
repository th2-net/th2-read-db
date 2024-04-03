plugins {
    alias(libs.plugins.kotlin)
    id("java-library")
    id("maven-publish")
}

kotlin {
    jvmToolchain(11)
}

configurations.all {
    resolutionStrategy.cacheChangingModulesFor(0, "seconds")
    resolutionStrategy.cacheDynamicVersionsFor(0, "seconds")
}

dependencies {
    implementation(project(":grpc-read-db"))

    implementation(libs.th2.common)
    implementation(libs.th2.common.utils)
    implementation(libs.th2.lw.data.provider.utils)

    implementation("org.slf4j:slf4j-api")

    implementation(libs.commons.dbcp2) {
        because("connection pool")
    }
    implementation("org.apache.commons:commons-text")
    implementation(libs.opencsv) {
        because("publishes raw messages in csv format")
    }

    implementation(libs.kotlinx.coroutines.core)
    implementation(libs.kotlin.logging)
    implementation(libs.kotlin.logging)
    implementation("com.fasterxml.jackson.core:jackson-databind")

    testImplementation(libs.junit.jupiter)
    testImplementation("org.jetbrains.kotlin:kotlin-test-junit")
    testImplementation(libs.kotlinx.coroutines.test)
    testImplementation(libs.mockito.kotlin)
    testImplementation(libs.strikt.core)

    testImplementation(platform(libs.testcontainers.bom))
    testImplementation("org.testcontainers:testcontainers")
    testImplementation("org.testcontainers:mysql")
    testImplementation("org.testcontainers:oracle-xe")
    testImplementation("io.grpc:grpc-testing")
    testImplementation("io.grpc:grpc-inprocess")

    testImplementation(libs.th2.junit.jupiter.integration)

    testRuntimeOnly(libs.mysql.connector.j) {
        because("mysql support")
    }
    testRuntimeOnly(libs.ojdbc11) {
        because("oracle support")
    }
}

tasks {
    test { useJUnitPlatform() }

    register<Test>("integrationTest") {
        group = "verification"
        useJUnitPlatform {
            includeTags("integration-test")
        }
        testLogging {
            showStandardStreams = true
        }
    }

    register<Test>("unitTest") {
        group = "verification"
        useJUnitPlatform {
            excludeTags("integration-test")
        }
        testLogging {
            showStandardStreams = true
        }
    }
}