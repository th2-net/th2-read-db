plugins {
    kotlin("jvm")
    id("java-library")
    id("maven-publish")
}

val coroutinesVersion by extra("1.7.3")

java {
    withJavadocJar()
    withSourcesJar()
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

    implementation("com.exactpro.th2:common:5.7.1-dev")
    implementation("com.exactpro.th2:common-utils:2.2.0-dev")
    implementation("com.exactpro.th2:lw-data-provider-utils:0.0.1-dev")

    implementation("org.slf4j:slf4j-api")

    implementation("org.apache.commons:commons-dbcp2:2.9.0") {
        because("connection pool")
    }
    implementation("org.apache.commons:commons-text")
    implementation("com.opencsv:opencsv:5.8") {
        because("publishes raw messages in csv format")
    }

    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:$coroutinesVersion")
    implementation("io.github.microutils:kotlin-logging:3.0.5")
    implementation("com.fasterxml.jackson.core:jackson-databind")

    testImplementation("org.junit.jupiter:junit-jupiter:5.10.0")
    testImplementation("org.jetbrains.kotlin:kotlin-test-junit")
    testImplementation("org.jetbrains.kotlinx:kotlinx-coroutines-test:$coroutinesVersion")
    testImplementation("org.mockito.kotlin:mockito-kotlin:5.1.0")
    testImplementation("io.strikt:strikt-core:0.34.1")

    testImplementation(platform("org.testcontainers:testcontainers-bom:1.19.0"))
    testImplementation("org.testcontainers:testcontainers")
    testImplementation("org.testcontainers:mysql")
    testImplementation("org.testcontainers:oracle-xe")
    testImplementation("io.grpc:grpc-testing")
    testImplementation("io.grpc:grpc-inprocess")

    testImplementation("com.exactpro.th2:junit-jupiter-integration:0.0.1")

    testRuntimeOnly("com.mysql:mysql-connector-j:8.1.0") {
        because("mysql support")
    }
    testRuntimeOnly("com.oracle.database.jdbc:ojdbc11:23.2.0.0") {
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