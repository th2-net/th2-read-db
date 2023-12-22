/*
 * Copyright 2023 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.th2.read.db.impl.grpc

import com.exactpro.th2.common.grpc.Event
import com.exactpro.th2.common.grpc.EventID
import com.exactpro.th2.common.grpc.EventStatus
import com.exactpro.th2.common.message.toTimestamp
import com.exactpro.th2.common.schema.factory.AbstractCommonFactory.MAPPER
import com.exactpro.th2.read.db.annotations.IntegrationTest
import com.exactpro.th2.read.db.app.DataBaseReader
import com.exactpro.th2.read.db.app.DataBaseReaderConfiguration
import com.exactpro.th2.read.db.containers.MySqlContainer
import com.exactpro.th2.read.db.core.DataSourceConfiguration
import com.exactpro.th2.read.db.core.DataSourceId
import com.exactpro.th2.read.db.core.MessageLoader
import com.exactpro.th2.read.db.core.QueryConfiguration
import com.exactpro.th2.read.db.core.QueryId
import com.exactpro.th2.read.db.core.RowListener
import com.exactpro.th2.read.db.core.TableRow
import com.exactpro.th2.read.db.core.UpdateListener
import com.exactpro.th2.read.db.grpc.QueryRequest
import com.exactpro.th2.read.db.grpc.QueryResponse
import com.exactpro.th2.read.db.grpc.ReadDbGrpc
import com.exactpro.th2.read.db.impl.grpc.util.toModel
import com.google.protobuf.ByteString
import io.grpc.BindableService
import io.grpc.Context
import io.grpc.ManagedChannel
import io.grpc.Server
import io.grpc.inprocess.InProcessChannelBuilder
import io.grpc.inprocess.InProcessServerBuilder
import io.netty.buffer.ByteBufUtil.decodeHexDump
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.runBlocking
import mu.KotlinLogging
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.mockito.kotlin.any
import org.mockito.kotlin.argumentCaptor
import org.mockito.kotlin.mock
import org.mockito.kotlin.timeout
import org.mockito.kotlin.times
import org.mockito.kotlin.verify
import org.mockito.kotlin.verifyNoInteractions
import org.mockito.kotlin.verifyNoMoreInteractions
import strikt.api.expectThat
import strikt.assertions.contains
import strikt.assertions.containsExactly
import strikt.assertions.isEqualTo
import strikt.assertions.isSameInstanceAs
import java.io.ByteArrayInputStream
import java.sql.Connection
import java.sql.Date
import java.time.Instant
import java.time.LocalDate
import java.time.ZoneOffset
import java.time.temporal.ChronoUnit
import java.util.concurrent.TimeUnit
import kotlin.test.assertNotNull


@IntegrationTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class DataBaseReaderGrpcServerIntegrationTest {
    private val rootEventId = EventID.newBuilder().apply {
        bookName = "test-book"
        scope = "test-scope"
        id = "test-id"
        startTimestamp = Instant.now().toTimestamp()
    }.build()
    private val mysql = MySqlContainer()
    private val persons = (1..30).map {
        Person("person$it", Instant.now().truncatedTo(ChronoUnit.DAYS), "test-data-$it".toByteArray())
    }

    private data class Person(val name: String, val birthday: Instant, val data: ByteArray) {
        override fun equals(other: Any?): Boolean {
            if (this === other) return true
            if (javaClass != other?.javaClass) return false

            other as Person

            if (name != other.name) return false
            if (birthday != other.birthday) return false
            if (!data.contentEquals(other.data)) return false

            return true
        }

        override fun hashCode(): Int {
            var result = name.hashCode()
            result = 31 * result + birthday.hashCode()
            result = 31 * result + data.contentHashCode()
            return result
        }
    }

    @BeforeAll
    fun init() {
        mysql.withDatabaseName("test_data").start()
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
    fun `test execute gRPC request`() {
        val genericUpdateListener = mock<UpdateListener> { }
        val genericRowListener = mock<RowListener> { }
        val messageLoader = mock<MessageLoader> { }
        val onEvent = mock<OnEvent> { }

        val sourceId = "persons"
        val queryId = "select"
        val cfg = DataBaseReaderConfiguration(
            dataSources = mapOf(
                DataSourceId(sourceId) to DataSourceConfiguration(
                    mysql.jdbcUrl,
                    mysql.username,
                    mysql.password,
                )
            ),
            queries = mapOf(
                QueryId(queryId) to QueryConfiguration(
                    "SELECT * FROM test_data.person WHERE \${fromId:integer} < id AND id < \${toId:integer}",
                    mapOf(
                        "fromId" to listOf(Int.MIN_VALUE.toString()),
                        "toId" to listOf(Int.MAX_VALUE.toString())
                    )
                ),
            )
        )
        val request = QueryRequest.newBuilder().apply {
            sourceIdBuilder.setId(sourceId)
            queryIdBuilder.setId(queryId)
            parametersBuilder
                .putValues("fromId", "10")
                .putValues("toId", "20")
        }.build()

        runBlocking(Dispatchers.IO) {
            val reader = DataBaseReader.createDataBaseReader(
                cfg,
                this,
                genericUpdateListener,
                genericRowListener,
                messageLoader,
            )

            val service = DataBaseReaderGrpcServer(
                reader,
                rootEventId,
                { cfg.dataSources[it] ?: error("'$it' data source isn't found in custom config") },
                { cfg.queries[it] ?: error("'$it' query isn't found in custom config") },
                onEvent,
            )

            val responses: List<QueryResponse> = GrpcTestHolder(service).use { (stub) ->
                withCancellation {
                    stub.execute(request).asSequence().toList()
                }
            }

            val expectedData = persons.asSequence().drop(10).take(9).toList()

            verifyNoInteractions(genericUpdateListener)
            verifyNoInteractions(messageLoader)

            val executionIds = hashSetOf<Long>()
            genericRowListener.assertCaptured(expectedData).also(executionIds::add)
            responses.assert(expectedData).also(executionIds::add)
            onEvent.assertCaptured(cfg, request, 1_000).also(executionIds::add)
            assertEquals(1, executionIds.size, "execution ids mismatch $executionIds")
        }
    }

    private fun RowListener.assertCaptured(persons: List<Person>): Long {
        val captor = argumentCaptor<TableRow>()
        verify(this, times(persons.size)).onRow(any(), captor.capture())
        verifyNoMoreInteractions(this)

        captor.allValues.map {
            Person(
                checkNotNull(it.columns["name"]).toString(),
                (checkNotNull(it.columns["birthday"]) as LocalDate).atStartOfDay().toInstant(ZoneOffset.UTC),
                (checkNotNull(it.columns["data"]) as ByteArray),
            )
        }.also {
            expectThat(it).containsExactly(persons)
        }

        val executionIds = captor.allValues.asSequence()
            .map(TableRow::executionId)
            .toSet()
        assertEquals(1, executionIds.size)
        return assertNotNull(executionIds.single())
    }

    private fun List<QueryResponse>.assert(persons: List<Person>): Long {
        map { response ->
            Person(
                response.getRowOrThrow("name").toString(),
                LocalDate.parse(response.getRowOrThrow("birthday")).atStartOfDay().toInstant(ZoneOffset.UTC),
                decodeHexDump(response.getRowOrThrow("data")),
            )
        }.also {
            expectThat(it).containsExactly(persons)
        }
        val executionIds = asSequence()
            .map(QueryResponse::getExecutionId)
            .toSet()
        assertEquals(1, executionIds.size)

        return executionIds.single()
    }

    private fun OnEvent.assertCaptured(cfg: DataBaseReaderConfiguration, request: QueryRequest, timeout: Long): Long {
        val captor = argumentCaptor<Event> { }
        verify(this, timeout(timeout)).accept(captor.capture())

        return captor.firstValue.let { event ->
            expectThat(event) {
                get { name }.contains(Regex("Execute '${request.queryId.id}' query \\(.*\\)"))
                get { type }.isEqualTo("read-db.execute")
                get { status }.isEqualTo(EventStatus.SUCCESS)
                get { parentId }.isSameInstanceAs(rootEventId)
                get { body.parseSingle<ExecuteBodyData>() }.apply {
                    get { dataSource }.isEqualTo(cfg.dataSources[request.sourceId.toModel()]?.copy(password = null))
                    get { beforeQueries }.isEqualTo(request.beforeQueryIdsList.map { requireNotNull(cfg.queries[it.toModel()]) })
                    get { query }.isEqualTo(cfg.queries[request.queryId.toModel()])
                    get { afterQueries }.isEqualTo(request.afterQueryIdsList.map { requireNotNull(cfg.queries[it.toModel()]) })
                }
            }

            event.body.parseSingle<ExecuteBodyData>().executionId
        }
    }

    private inline fun <reified T> ByteString.parseSingle(): T = MAPPER.readValue<List<T>>(
        toString(Charsets.UTF_8),
        MAPPER.typeFactory.constructCollectionType(List::class.java, T::class.java)
    ).single()

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
        insertData(persons)
        LOGGER.info { "Initial data inserted" }
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

    private fun Connection.insertData(persons: List<Person>) {
        val prepareStatement = prepareStatement(
            """
                        INSERT INTO `test_data`.`person` (`name`, `birthday`, `data`)
                        VALUES
                        (?, ?, ?);
                    """.trimIndent()
        )
        for (person in persons) {
            prepareStatement.setString(1, person.name)
            prepareStatement.setDate(2, Date(person.birthday.toEpochMilli()))
            prepareStatement.setBlob(3, ByteArrayInputStream(person.data))
            prepareStatement.addBatch()
        }
        prepareStatement.executeBatch()
    }

    private class GrpcTestHolder(
        service: BindableService
    ) : AutoCloseable {
        private val inProcessServer: Server

        private val inProcessChannel: ManagedChannel

        val stub: ReadDbGrpc.ReadDbBlockingStub

        init {
            inProcessServer = InProcessServerBuilder
                .forName(SERVER_NAME)
                .addService(service)
                .directExecutor()
                .build()
                .also(Server::start)

            inProcessChannel = InProcessChannelBuilder
                .forName(SERVER_NAME)
                .directExecutor()
                .build()

            stub = ReadDbGrpc.newBlockingStub(inProcessChannel)
        }

        operator fun component1(): ReadDbGrpc.ReadDbBlockingStub = stub

        override fun close() {
            LOGGER.info { "Shutdown process channel" }
            inProcessChannel.shutdown()
            if (!inProcessChannel.awaitTermination(1, TimeUnit.MINUTES)) {
                LOGGER.warn { "Process channel couldn't stop during 1 min" }
                inProcessChannel.shutdownNow()
                LOGGER.warn { "Process channel shutdown now, is terminated: ${inProcessChannel.isTerminated}" }
            }
            LOGGER.info { "Shutdown process server" }
            inProcessServer.shutdown()
            if (!inProcessServer.awaitTermination(1, TimeUnit.MINUTES)) {
                LOGGER.warn { "Process server couldn't stop during 1 min" }
                inProcessServer.shutdownNow()
                LOGGER.warn { "Process server shutdown now, is terminated: ${inProcessChannel.isTerminated}" }
            }
        }
    }

    companion object {
        private val LOGGER = KotlinLogging.logger { }

        private const val SERVER_NAME = "server"

        private fun <T> withCancellation(code: () -> T): T {
            return Context.current().withCancellation().use { context ->
                context.call { code() }
            }
        }
    }
}