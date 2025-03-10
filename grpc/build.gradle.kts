plugins {
    `java-library`
    `maven-publish`
    alias(libs.plugins.th2.grpc)
}

configurations.all {
    resolutionStrategy.cacheChangingModulesFor(0, "seconds")
    resolutionStrategy.cacheDynamicVersionsFor(0, "seconds")
}

dependencies {
    api(libs.th2.grpc.common)
}

th2Grpc {
    service.set(true)
}