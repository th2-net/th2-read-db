plugins {
    alias(libs.plugins.th2.base)
    alias(libs.plugins.th2.publish)

    alias(libs.plugins.kotlin) apply false
    alias(libs.plugins.kapt) apply false
    alias(libs.plugins.th2.grpc) apply false
    alias(libs.plugins.th2.component) apply false
}

//dependencyCheck {
//    suppressionFile = "suppressions.xml"
//}

allprojects {
    group = "com.exactpro.th2"
    version = project.findProperty("release_version") as String
    val suffix = project.findProperty("version_suffix") as String
    if (suffix.isNotEmpty()) {
        version = "$version-$suffix"
    }
}