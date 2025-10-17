plugins {
    alias(libs.plugins.kotlin)
    alias(libs.plugins.kapt)
    `java-library`
    `maven-publish`
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

    testImplementation(platform(libs.junit.bom))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testImplementation("org.jetbrains.kotlin:kotlin-test-junit")
    testImplementation(libs.mockito.kotlin)

    testImplementation(platform(libs.testcontainers.bom))
    testImplementation("org.testcontainers:testcontainers")
    testImplementation("org.testcontainers:testcontainers-oracle-xe")
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
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
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

        if (Env.isPodmanInstalled) {
            environment("DOCKER_HOST", "unix:///run/user/${Env.uid}/podman/podman.sock")
            environment("TESTCONTAINERS_RYUK_DISABLED", "true")
        }
    }
}