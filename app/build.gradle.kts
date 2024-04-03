plugins {
    id("application")
    alias(libs.plugins.th2.component)
}

configurations.all {
    resolutionStrategy.cacheChangingModulesFor(0, "seconds")
    resolutionStrategy.cacheDynamicVersionsFor(0, "seconds")
}

dependencies {
    implementation(project(":read-db-core"))

    //region postgresql
    runtimeOnly(libs.postgresql) {
        because("prostresql support")
    }
    //endregion

    //region mysql
    runtimeOnly(libs.mysql.connector.j) {
        because("mysql support")
    }
    //endregion

    //region oracle
    runtimeOnly(libs.ojdbc11) {
        because("oracle support")
    }
    runtimeOnly(project(":read-db-oracle-extension")) {
        because("oracle support")
    }
    //endregion

    //region mssql
    runtimeOnly(libs.mssql.jdbc) {
        because("mssql support")
    }
    //endregion
}

application {
    applicationName = "service"
    mainClass.set("com.exactpro.th2.read.db.bootstrap.Main")
}
