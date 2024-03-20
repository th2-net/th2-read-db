plugins {
    id("java-library")
    id("maven-publish")
    id("com.exactpro.th2.gradle.grpc")
}

configurations.all {
    resolutionStrategy.cacheChangingModulesFor(0, "seconds")
    resolutionStrategy.cacheDynamicVersionsFor(0, "seconds")
}

dependencies {
    api("com.exactpro.th2:grpc-common:4.4.0-dev")
}

th2Grpc {
    service.set(true)
}