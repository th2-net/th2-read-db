plugins {
    kotlin("jvm")
    kotlin("kapt")
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
    implementation(project(":read-db-core"))

    compileOnly("com.oracle.database.jdbc:ojdbc11:23.3.0.23.09") {
        because("oracle support")
    }

    // Auto service
    compileOnly("com.google.auto.service:auto-service-annotations:1.1.1")
    kapt("com.google.auto.service:auto-service:1.1.1")

    testImplementation("org.junit.jupiter:junit-jupiter:5.10.0")
    testImplementation("org.jetbrains.kotlin:kotlin-test-junit")
    testImplementation("org.mockito.kotlin:mockito-kotlin:5.1.0")

    testImplementation(platform("org.testcontainers:testcontainers-bom:1.19.7"))
    testImplementation("org.testcontainers:testcontainers")
    testImplementation("org.testcontainers:oracle-xe")
    testImplementation("io.grpc:grpc-testing")
    testImplementation("io.grpc:grpc-inprocess")

    testImplementation("com.exactpro.th2:junit-jupiter-integration:0.0.1")
    testImplementation("org.awaitility:awaitility:4.2.1")
    testImplementation(testFixtures(project(":read-db-core")))
    testImplementation(project(":grpc-read-db"))

    testImplementation("io.github.microutils:kotlin-logging:3.0.5")
    testImplementation("com.exactpro.th2:common-utils:2.2.2-dev")
    testImplementation("com.exactpro.th2:lw-data-provider-utils:0.0.1-dev")

    testImplementation("com.oracle.database.jdbc:ojdbc11:23.3.0.23.09") {
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