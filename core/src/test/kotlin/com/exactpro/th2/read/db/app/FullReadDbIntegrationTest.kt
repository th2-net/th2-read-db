/*
 * Copyright 2024 Exactpro (Exactpro Systems Limited)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.exactpro.th2.read.db.app

import com.exactpro.th2.common.message.toJson
import com.exactpro.th2.common.schema.factory.CommonFactory
import com.exactpro.th2.read.db.MYSQL_DOCKER_IMAGE
import com.exactpro.th2.read.db.annotations.IntegrationTest
import com.exactpro.th2.read.db.bootstrap.setupApp
import com.exactpro.th2.read.db.grpc.QueryId
import com.exactpro.th2.read.db.grpc.QueryRequest
import com.exactpro.th2.read.db.grpc.ReadDbService
import com.exactpro.th2.read.db.grpc.SourceId
import com.exactpro.th2.test.annotations.CustomConfigProvider
import com.exactpro.th2.test.annotations.Th2AppFactory
import com.exactpro.th2.test.annotations.Th2IntegrationTest
import com.exactpro.th2.test.annotations.Th2TestFactory
import com.exactpro.th2.test.extension.CleanupExtension
import com.exactpro.th2.test.spec.CradleSpec
import com.exactpro.th2.test.spec.CustomConfigSpec
import com.exactpro.th2.test.spec.GrpcSpec
import com.exactpro.th2.test.spec.RabbitMqSpec
import com.exactpro.th2.test.spec.pin
import com.exactpro.th2.test.spec.pins
import com.exactpro.th2.test.spec.publishers
import com.exactpro.th2.test.spec.server
import mu.KotlinLogging
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertDoesNotThrow
import org.junit.jupiter.api.assertThrows
import org.testcontainers.containers.MySQLContainer
import org.testcontainers.utility.DockerImageName
import java.sql.Connection


@Th2IntegrationTest
@IntegrationTest
internal class FullReadDbIntegrationTest {
    private val mysql = MySQLContainer(DockerImageName.parse(MYSQL_DOCKER_IMAGE))

    @Suppress("unused")
    val grpc = GrpcSpec.create()
        .server<ReadDbService>()

    @Suppress("unused")
    val mq = RabbitMqSpec.create()
        .pins {
            publishers {
                pin("out") {
                    attributes("transport-group")
                }
            }
        }

    @Suppress("unused")
    val cradle = CradleSpec.create()
        .disableAutoPages()
        .reuseKeyspace()

    @BeforeAll
    fun init() {
        mysql.withDatabaseName("test_data")
            .start()
    }

    @AfterAll
    fun cleanup() {
        mysql.stop()
    }

    @BeforeEach
    fun setup() {
        execute {
            dropTable()
            initTestData()
        }
    }

    @Test
    @CustomConfigProvider("config")
    fun `exception during query execution does not block grpc`(
        @Th2AppFactory factory: CommonFactory,
        @Th2TestFactory test: CommonFactory,
        resourceCleaner: CleanupExtension.Registry,
    ) {
        setupApp(factory) { name, resource -> resourceCleaner.add(name, resource) }
        val readDb = test.grpcRouter.getService(ReadDbService::class.java)

        assertThrows<Exception> {
            readDb.execute(
                QueryRequest.newBuilder()
                    .setQueryId(QueryId.newBuilder().setId("wrong_db"))
                    .setSourceId(SourceId.newBuilder().setId("persons"))
                    .build()
            ).asSequence().toList()
        }
    }

    @Test
    @CustomConfigProvider("config")
    fun `query execution using grpc works`(
        @Th2AppFactory factory: CommonFactory,
        @Th2TestFactory test: CommonFactory,
        resourceCleaner: CleanupExtension.Registry,
    ) {
        setupApp(factory) { name, resource -> resourceCleaner.add(name, resource) }
        val readDb = test.grpcRouter.getService(ReadDbService::class.java)

        val results = assertDoesNotThrow {
            readDb.execute(
                QueryRequest.newBuilder()
                    .setQueryId(QueryId.newBuilder().setId("all"))
                    .setSourceId(SourceId.newBuilder().setId("persons"))
                    .build()
            ).asSequence().toList()
        }

        Assertions.assertEquals(0, results.size) {
            "unexpected results: ${results.map { it.toJson() }}"
        }
    }

    @Suppress("unused")
    fun config(): CustomConfigSpec {
        return CustomConfigSpec.fromString(
            """
            {
                "dataSources": {
                    "persons": {
                        "url": "${mysql.jdbcUrl}",
                        "username": "${mysql.username}",
                        "password": "${mysql.password}"
                    }
                },
                "queries": {
                    "all": {
                        "query": "SELECT * FROM test_data.person;"
                    },
                    "wrong_db": {
                        "query": "SELECT * FROM test_dat.person;"
                    }
                },
                "useTransport": true
            }
            """.trimIndent()
        )
    }

    private fun execute(action: Connection.() -> Unit) {
        mysql.createConnection("").use { it.action() }
    }

    private fun Connection.initTestData() {
        createStatement()
            .execute(
                """
                    CREATE TABLE `test_data`.`person` (
                      `id` INT NOT NULL AUTO_INCREMENT,
                      `name` VARCHAR(45) NOT NULL,
                      `birthday` DATE NOT NULL,
                      `data` BLOB NOT NULL,
                      PRIMARY KEY (`id`));
                """.trimIndent()
            )
        LOGGER.info { "table created" }
    }

    private fun Connection.dropTable() {
        createStatement()
            .execute(
                """
                DROP TABLE IF EXISTS `test_data`.`person`;
            """.trimIndent()
            )
        LOGGER.info { "table dropped" }
    }

    companion object {
        private val LOGGER = KotlinLogging.logger { }
    }
}