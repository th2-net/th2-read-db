plugins {
    id("application")
    id("com.exactpro.th2.gradle.component")
}

configurations.all {
    resolutionStrategy.cacheChangingModulesFor(0, "seconds")
    resolutionStrategy.cacheDynamicVersionsFor(0, "seconds")
}

dependencies {
    implementation(project(":read-db-core"))

    //region postgresql
    runtimeOnly("org.postgresql:postgresql:42.7.3") {
        because("prostresql support")
    }
    //endregion

    //region mysql
    runtimeOnly("com.mysql:mysql-connector-j:8.3.0") {
        because("mysql support")
    }
    //endregion

    //region oracle
    runtimeOnly("com.oracle.database.jdbc:ojdbc11:23.3.0.23.09") {
        because("oracle support")
    }
    runtimeOnly(project(":read-db-oracle-extension")) {
        because("oracle support")
    }
    //endregion

    //region mssql
    runtimeOnly("com.microsoft.sqlserver:mssql-jdbc:12.4.0.jre11") {
        because("mssql support")
    }
    //endregion
}

application {
    applicationName = "service"
    mainClass.set("com.exactpro.th2.read.db.bootstrap.Main")
}
