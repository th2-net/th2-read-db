/*
 * Copyright 2023-2024 Exactpro (Exactpro Systems Limited)
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

import com.exactpro.th2.common.event.Event
import com.exactpro.th2.common.grpc.EventID
import com.exactpro.th2.common.grpc.EventStatus
import com.exactpro.th2.common.message.toTimestamp
import com.exactpro.th2.common.schema.factory.AbstractCommonFactory.MAPPER
import com.exactpro.th2.common.utils.toInstant
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
import com.exactpro.th2.read.db.grpc.QueryReport
import com.exactpro.th2.read.db.grpc.QueryRequest
import com.exactpro.th2.read.db.grpc.QueryResponse
import com.exactpro.th2.read.db.grpc.ReadDbGrpc
import com.exactpro.th2.read.db.impl.grpc.util.toModel
import com.google.protobuf.ByteString
import com.google.protobuf.util.Timestamps
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
import org.mockito.kotlin.anyOrNull
import org.mockito.kotlin.argumentCaptor
import org.mockito.kotlin.doAnswer
import org.mockito.kotlin.isNull
import org.mockito.kotlin.mock
import org.mockito.kotlin.never
import org.mockito.kotlin.timeout
import org.mockito.kotlin.times
import org.mockito.kotlin.verify
import org.mockito.kotlin.verifyBlocking
import org.mockito.kotlin.verifyNoInteractions
import org.mockito.kotlin.verifyNoMoreInteractions
import strikt.api.expectThat
import strikt.assertions.contains
import strikt.assertions.containsExactly
import strikt.assertions.isEqualTo
import strikt.assertions.isGreaterThan
import strikt.assertions.isLessThan
import strikt.assertions.isSameInstanceAs
import java.io.ByteArrayInputStream
import java.sql.Connection
import java.sql.Date
import java.time.Instant
import java.time.LocalDate
import java.time.ZoneOffset
import java.time.temporal.ChronoUnit
import java.util.concurrent.TimeUnit
import kotlin.math.max
import kotlin.math.min
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertTrue


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

    private data class Person(val name: String, val birthday: Instant, val data: ByteArray?) {
        override fun equals(other: Any?): Boolean {
            if (this === other) return true
            if (javaClass != other?.javaClass) return false

            other as Person

            if (name != other.name) return false
            if (birthday != other.birthday) return false
            if (data != null) {
                if (other.data == null) return false
                if (!data.contentEquals(other.data)) return false
            } else if (other.data != null) return false

            return true
        }

        override fun hashCode(): Int {
            var result = name.hashCode()
            result = 31 * result + birthday.hashCode()
            result = 31 * result + (data?.contentHashCode() ?: 0)
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
    fun `execute gRPC request test`() {
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

            val executionId = onEvent.assertCaptured(cfg, request, 1_000)
            genericRowListener.assertCaptured(expectedData, executionId)
            responses.assert(expectedData, executionId)
        }
    }

    @Test
    fun `cancel execute gRPC request test`() {
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
                { cfg.dataSources[it] ?: error("'$it' data source isn't found in custom config") },
                { cfg.queries[it] ?: error("'$it' query isn't found in custom config") },
                onEvent,
            )

            GrpcTestHolder(service).use { (stub) ->
                val responses: List<QueryResponse> = withCancellation {
                    stub.execute(request).asSequence().take(3).toList()
                }

                val expectedData = persons.asSequence().drop(10).take(9).toList()

                verifyNoInteractions(genericUpdateListener)
                verifyNoInteractions(messageLoader)

                val executionId = onEvent.assertCaptured(cfg, request, 1_000)
                genericRowListener.assertCaptured(expectedData, executionId)
                responses.assert(expectedData.subList(0, 3), executionId)
            }
        }
    }

    @Test
    fun `execute gRPC request (slow consumer) test`() {
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
                { cfg.dataSources[it] ?: error("'$it' data source isn't found in custom config") },
                { cfg.queries[it] ?: error("'$it' query isn't found in custom config") },
                onEvent,
            )

            val expectedData = persons.asSequence().drop(10).take(9).toList()
            val responses = mutableListOf<QueryResponse>()
            GrpcTestHolder(service).use { (stub) ->
                withCancellation {
                    val iterator: Iterator<QueryResponse> = stub.execute(request)
                    // Grpc listener executes the checks sequentially if previous is false
                    //  1) is isReady flag on gRPC observer true
                    //  2) is OnReady call back called on gRPC observer. This check is blocking
                    // In the initial state OnReady is called and isReady is set true
                    // Iterations:
                    //  1) check isReady, send message
                    //  2) check isReady and OnReady, send message
                    //  3) check isReady and OnReady, block
                    // Operation sequence: send, (send, receive)
                    verifyBlocking(genericRowListener,  timeout(1_000).times(2)) {
                        onRow(any(), any())
                    }
                    repeat(expectedData.size) { index ->
                        // minimal value is 2 because gRPC Listener allow to process first two messages without blocking.
                        var expectRows = max(2, index + 1)
                        verifyBlocking(
                            genericRowListener,
                            timeout(100).times(expectRows)
                                .description("Check listener before iterator hasNext call for $index person")
                        ) { onRow(any(), any()) }
                        assertTrue(iterator.hasNext(), "Check next gRPC response for $index person")
                        responses.add(iterator.next())

                        // Client requests the next message after getting current.
                        // minimal value is 2 because gRPC Listener allow to process first two messages without blocking.
                        expectRows = max(2, index + 2)
                        verifyBlocking(
                            genericRowListener,

                            timeout(100).times(min(expectRows, expectedData.size))
                                .description("Check listener after iterator hasNext call for $index person")
                        ) { onRow(any(), any()) }

                        if (expectRows < expectedData.size) { // some messages will be published
                            verify(onEvent, never()).accept(any(), anyOrNull())
                        } else if (expectRows > expectedData.size) { // all messages are already published
                            verify(onEvent).accept(any(), anyOrNull())
                        } else { // the last message is published on this iteration
                            verify(onEvent, timeout(100)).accept(any(), anyOrNull())
                        }
                    }
                    assertFalse(iterator.hasNext())
                }
            }

            verifyNoInteractions(genericUpdateListener)
            verifyNoInteractions(messageLoader)

            val executionId = onEvent.assertCaptured(cfg, request)
            genericRowListener.assertCaptured(expectedData, executionId)
            responses.assert(expectedData, executionId)
        }
    }

    @Test
    fun `load gRPC request test`() {
        var firstTimestamp: Instant? = null
        var lastTimestamp: Instant? = null
        val genericUpdateListener = mock<UpdateListener> { }
        val genericRowListener = mock<RowListener> {
            onBlocking { onRow(any(), any()) }.doAnswer {
                if (firstTimestamp == null) {
                    Instant.now().let {
                        firstTimestamp = it
                        lastTimestamp = it
                    }
                } else {
                    lastTimestamp = Instant.now()
                }
            }
        }
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
                { cfg.dataSources[it] ?: error("'$it' data source isn't found in custom config") },
                { cfg.queries[it] ?: error("'$it' query isn't found in custom config") },
                onEvent,
            )

            val start = Instant.now()
            val report: QueryReport = GrpcTestHolder(service).use { (stub) ->
                withCancellation { stub.load(request) }
            }
            val end = Instant.now()

            val expectedData = persons.asSequence().drop(10).take(9).toList()

            verifyNoInteractions(genericUpdateListener)
            verifyNoInteractions(messageLoader)

            assertNotNull(lastTimestamp)

            expectThat(report) {
                get { executionId }.isGreaterThan(0)
                get { rowsReceived }.isEqualTo(expectedData.size.toLong())
                get { this.start.toInstant() }.isGreaterThan(start).isLessThan(assertNotNull(firstTimestamp))
                get { this.end.toInstant() }.isGreaterThan(assertNotNull(lastTimestamp)).isLessThan(end)
            }
            assertTrue(Timestamps.comparator().compare(report.start, report.end) < 0)

            onEvent.assertCaptured(cfg, request, 1_000).also { executionId ->
                assertEquals(report.executionId, executionId)
            }
            genericRowListener.assertCaptured(expectedData, report.executionId)
        }
    }

    @Test
    fun `null value in response test`() {
        val genericUpdateListener = mock<UpdateListener> { }
        val genericRowListener = mock<RowListener> { }
        val messageLoader = mock<MessageLoader> { }
        val onEvent = mock<OnEvent> { }

        val person = Person("null-test", Instant.now().truncatedTo(ChronoUnit.DAYS), null)
        execute {
            insertData(listOf(person))
        }

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
                    "SELECT * FROM test_data.person WHERE name = \${name}",
                    mapOf(
                        "name" to listOf(person.name),
                    )
                ),
            )
        )


        val request = QueryRequest.newBuilder().apply {
            sourceIdBuilder.setId(sourceId)
            queryIdBuilder.setId(queryId)
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
                { cfg.dataSources[it] ?: error("'$it' data source isn't found in custom config") },
                { cfg.queries[it] ?: error("'$it' query isn't found in custom config") },
                onEvent,
            )

            val responses: List<QueryResponse> = GrpcTestHolder(service).use { (stub) ->
                withCancellation {
                    stub.execute(request).asSequence().toList()
                }
            }

            val expectedData = listOf(person)

            verifyNoInteractions(genericUpdateListener)
            verifyNoInteractions(messageLoader)

            val executionId = onEvent.assertCaptured(cfg, request, 1_000)
            genericRowListener.assertCaptured(expectedData, executionId)
            responses.assert(expectedData, executionId)
        }
    }

    private fun RowListener.assertCaptured(persons: List<Person>, executionId: Long) {
        val captor = argumentCaptor<TableRow>()
        verifyBlocking(this, times(persons.size)) { onRow(any(), captor.capture()) }
        verifyNoMoreInteractions(this)

        captor.allValues.map {
            Person(
                checkNotNull(it.columns["name"]).toString(),
                (checkNotNull(it.columns["birthday"]) as LocalDate).atStartOfDay().toInstant(ZoneOffset.UTC),
                it.columns["data"] as ByteArray?,
            )
        }.also {
            expectThat(it).containsExactly(persons)
        }

        captor.allValues.forEachIndexed { index, row ->
            assertEquals(executionId, row.executionId, "execution id mismatch in $index element")
        }
    }

    private fun List<QueryResponse>.assert(persons: List<Person>, executionId: Long) {
        map { response ->
            Person(
                response.getRowOrThrow("name").toString(),
                LocalDate.parse(response.getRowOrThrow("birthday")).atStartOfDay().toInstant(ZoneOffset.UTC),
                if (response.containsRow("data")) decodeHexDump(response.getRowOrThrow("data")) else null,
            )
        }.also {
            expectThat(it).containsExactly(persons)
        }
        forEachIndexed { index, response ->
            assertEquals(executionId, response.executionId, "execution id mismatch in $index element")
        }
    }

    private fun OnEvent.assertCaptured(cfg: DataBaseReaderConfiguration, request: QueryRequest, timeout: Long = 0): Long {
        val captor = argumentCaptor<Event> { }
        verify(this, timeout(timeout)).accept(captor.capture(), isNull())

        return captor.firstValue.toProto(rootEventId).let { event ->
            expectThat(event) {
                get { name }.contains(Regex("Execute '${request.queryId.id}' query \\(.*\\)"))
                get { type }.isEqualTo("read-db.execute")
                get { status }.isEqualTo(EventStatus.SUCCESS)
                get { parentId }.isSameInstanceAs(rootEventId)
                get { body.parseSingle<ExecuteBodyData>() }.and {
                    get { dataSource }.isEqualTo(cfg.dataSources[request.sourceId.toModel()]?.copy(password = null))
                    get { beforeQueries }.isEqualTo(request.beforeQueryIdsList.map { requireNotNull(cfg.queries[it.toModel()]) })
                    get { query }.isEqualTo(cfg.queries[request.queryId.toModel()])
                    get { afterQueries }.isEqualTo(request.afterQueryIdsList.map { requireNotNull(cfg.queries[it.toModel()]) })
                }
            }

            event.body.parseSingle<ExecuteBodyData>().executionId
                .also { assertTrue(it > 0) }
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
                      `data` BLOB,
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
            prepareStatement.setBlob(3, person.data?.let { ByteArrayInputStream(it) })
            prepareStatement.addBatch()
        }
        prepareStatement.executeBatch()
    }

    private class GrpcTestHolder(
        service: BindableService
    ) : AutoCloseable {
        private val inProcessServer: Server = InProcessServerBuilder
            .forName(SERVER_NAME)
            .addService(service)
            .directExecutor()
            .build()
            .also(Server::start)

        private val inProcessChannel: ManagedChannel = InProcessChannelBuilder
            .forName(SERVER_NAME)
            .directExecutor()
            .build()

        val stub: ReadDbGrpc.ReadDbBlockingStub = ReadDbGrpc.newBlockingStub(inProcessChannel)

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