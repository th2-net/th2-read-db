plugins {
    alias(libs.plugins.kotlin)
    alias(libs.plugins.kapt)
    `java-library`
    `maven-publish`
}

kotlin {
    jvmToolchain(11)
}

configurations.all {
    resolutionStrategy.cacheChangingModulesFor(0, "seconds")
    resolutionStrategy.cacheDynamicVersionsFor(0, "seconds")
}

dependencies {
    implementation(project(":read-db-core"))

    compileOnly(libs.ojdbc11) {
        because("oracle support")
    }

    // Auto service
    compileOnly(libs.auto.service.annotations)
    kapt(libs.auto.service)

    testImplementation(libs.junit.jupiter)
    testImplementation("org.jetbrains.kotlin:kotlin-test-junit")
    testImplementation(libs.mockito.kotlin)

    testImplementation(platform(libs.testcontainers.bom))
    testImplementation("org.testcontainers:testcontainers")
    testImplementation("org.testcontainers:oracle-xe")
    testImplementation("io.grpc:grpc-testing")
    testImplementation("io.grpc:grpc-inprocess")

    testImplementation(libs.th2.junit.jupiter.integration)
    testImplementation(libs.awaitility)
    testImplementation(project(":grpc-read-db"))

    testImplementation(libs.kotlin.logging)
    testImplementation(libs.th2.common.utils)
    testImplementation(libs.th2.lw.data.provider.utils)

    testImplementation(libs.ojdbc11) {
        because("oracle support")
    }
}

tasks {
    test {
        useJUnitPlatform {
            excludeTags("integration-test")
        }
    }

    register<Test>("integrationTest") {
        group = "verification"
        useJUnitPlatform {
            includeTags("integration-test")
        }
        testLogging {
            showStandardStreams = true
        }
    }
}