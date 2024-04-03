/*
 * Copyright 2022-2024 Exactpro (Exactpro Systems Limited)
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

import com.exactpro.th2.read.db.annotations.IntegrationTest
import com.exactpro.th2.read.db.containers.MySqlContainer
import com.exactpro.th2.read.db.core.DataBaseMonitorService.Companion.TH2_PULL_TASK_UPDATE_HASH_PROPERTY
import com.exactpro.th2.read.db.core.DataSourceConfiguration
import com.exactpro.th2.read.db.core.DataSourceId
import com.exactpro.th2.read.db.core.HashService
import com.exactpro.th2.read.db.core.HashService.Companion.calculateHash
import com.exactpro.th2.read.db.core.MessageLoader
import com.exactpro.th2.read.db.core.QueryConfiguration
import com.exactpro.th2.read.db.core.QueryId
import com.exactpro.th2.read.db.core.ResultListener
import com.exactpro.th2.read.db.core.RowListener
import com.exactpro.th2.read.db.core.TableRow
import com.exactpro.th2.read.db.core.UpdateListener
import com.exactpro.th2.read.db.core.ValueTransformProvider.Companion.DEFAULT_TRANSFORM
import com.exactpro.th2.read.db.core.impl.BaseDataSourceProvider
import com.exactpro.th2.read.db.core.impl.BaseHashServiceImpl
import com.exactpro.th2.read.db.core.impl.BaseQueryProvider
import io.netty.buffer.ByteBufUtil.decodeHexDump
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.test.advanceTimeBy
import kotlinx.coroutines.test.advanceUntilIdle
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import mu.KotlinLogging
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource
import org.junit.jupiter.params.provider.ValueSource
import org.mockito.Mockito.timeout
import org.mockito.kotlin.any
import org.mockito.kotlin.argumentCaptor
import org.mockito.kotlin.clearInvocations
import org.mockito.kotlin.eq
import org.mockito.kotlin.isNull
import org.mockito.kotlin.mock
import org.mockito.kotlin.never
import org.mockito.kotlin.same
import org.mockito.kotlin.spy
import org.mockito.kotlin.verify
import org.mockito.kotlin.verifyNoInteractions
import org.mockito.kotlin.whenever
import strikt.api.expectThat
import strikt.assertions.containsExactly
import java.io.ByteArrayInputStream
import java.sql.Connection
import java.sql.Date
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.time.LocalDate
import java.time.LocalTime
import java.time.ZoneOffset
import java.time.temporal.ChronoUnit

@OptIn(ExperimentalCoroutinesApi::class)
@IntegrationTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
internal class DataBaseReaderIntegrationTest {
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

    @ParameterizedTest
    @CsvSource(
        "'',''",
        "'count',''",
        "'','count'",
        "'count','count'",
        "'count,count-all',''",
        "'','count,count-all'",
        "'count,count-all','count,count-all'",
    )
    fun `receives data from database`(beforeList: String, afterList: String) {
        val genericUpdateListener = mock<UpdateListener> { }
        val genericRowListener = mock<RowListener> { }
        val messageLoader = mock<MessageLoader> { }
        runTest {
            val reader = DataBaseReader.createDataBaseReader(
                DataBaseReaderConfiguration(
                    mapOf(
                        DataSourceId("persons") to DataSourceConfiguration(
                            mysql.jdbcUrl,
                            mysql.username,
                            mysql.password,
                        )
                    ),
                    mapOf(
                        QueryId("all") to QueryConfiguration(
                            "SELECT * FROM test_data.person WHERE birthday > \${birthday:date}",
                            mapOf(
                                "birthday" to listOf("1996-10-31")
                            )
                        ),
                        QueryId("count") to QueryConfiguration(
                            "SELECT COUNT(*) FROM test_data.person WHERE birthday > \${birthday:date}",
                            mapOf(
                                "birthday" to listOf("1996-10-31")
                            )
                        ),
                        QueryId("count-all") to QueryConfiguration(
                            "SELECT COUNT(*) FROM test_data.person"
                        )
                    )
                ),
                this,
                genericUpdateListener,
                genericRowListener,
                messageLoader,
            )
            val listener = mock<ResultListener> { }
            reader.executeQuery(
                ExecuteQueryRequest(
                    DataSourceId("persons"),
                    beforeList.split(',').filter(String::isNotEmpty).map(::QueryId),
                    QueryId("all"),
                    afterList.split(',').filter(String::isNotEmpty).map(::QueryId),
                    emptyMap()
                ),
                listener
            )
            advanceUntilIdle()

            genericRowListener.assertCaptured(persons)
            listener.assertCaptured(persons)
            verifyNoInteractions(genericUpdateListener)
        }
    }

    @Test
    fun `receives update from table (with init query)`() {
        val genericUpdateListener = mock<UpdateListener> { }
        val genericRowListener = mock<RowListener> { }
        val messageLoader = mock<MessageLoader> { }
        val interval = Duration.ofMillis(100)
        runTest {
            val reader = DataBaseReader.createDataBaseReader(
                DataBaseReaderConfiguration(
                    mapOf(
                        DataSourceId("persons") to DataSourceConfiguration(
                            mysql.jdbcUrl,
                            mysql.username,
                            mysql.password,
                        )
                    ),
                    mapOf(
                        QueryId("current_state") to QueryConfiguration(
                            "SELECT * FROM test_data.person ORDER BY id DESC LIMIT 1;"
                        ),
                        QueryId("updates") to QueryConfiguration(
                            "SELECT * FROM test_data.person WHERE id > \${id:integer}"
                        )
                    )
                ),
                this,
                genericUpdateListener,
                genericRowListener,
                messageLoader
            )
            val listener = mock<UpdateListener> { }
            val taskId = reader.submitPullTask(
                PullTableRequest(
                    DataSourceId("persons"),
                    startFromLastReadRow = false,
                    resetStateParameters = ResetState(),
                    beforeInitQueryIds = emptyList(),
                    initQueryId = QueryId("current_state"),
                    initParameters = emptyMap(),
                    afterInitQueryIds = emptyList(),
                    useColumns = setOf("id"),
                    beforeUpdateQueryIds = emptyList(),
                    updateQueryId = QueryId("updates"),
                    updateParameters = emptyMap(),
                    afterUpdateQueryIds = emptyList(),
                    interval = interval,
                ),
                listener,
            )

            advanceTimeBy(interval.toMillis())

            val newData: List<Person> = (1..10).map { Person("new$it", Instant.now().truncatedTo(ChronoUnit.DAYS), "test-new-data-$it".toByteArray()) }
            insertData(newData)

            advanceTimeBy(interval.toMillis() * 2)

            genericUpdateListener.assertCaptured(newData, interval.toMillis() * 2)
            listener.assertCaptured(newData)
            verify(messageLoader, never()).load(any(), isNull(), any())
            verifyNoInteractions(genericRowListener)
            reader.stopPullTask(taskId)

            advanceUntilIdle()
        }
    }

    @ParameterizedTest
    @ValueSource(ints = [0, 10, 30, 35, 50])
    fun `receives update from table (without init query)`(startId: Int) {
        val genericUpdateListener = mock<UpdateListener> { }
        val genericRowListener = mock<RowListener> { }
        val messageLoader = mock<MessageLoader> { }
        val interval = Duration.ofMillis(100)
        runTest {
            val reader = DataBaseReader.createDataBaseReader(
                DataBaseReaderConfiguration(
                    mapOf(
                        DataSourceId("persons") to DataSourceConfiguration(
                            mysql.jdbcUrl,
                            mysql.username,
                            mysql.password,
                        )
                    ),
                    mapOf(
                        QueryId("updates") to QueryConfiguration(
                            "SELECT * FROM test_data.person WHERE id > \${id:integer}"
                        )
                    )
                ),
                this,
                genericUpdateListener,
                genericRowListener,
                messageLoader
            )
            val listener = mock<UpdateListener> { }
            val taskId = reader.submitPullTask(
                PullTableRequest(
                    DataSourceId("persons"),
                    startFromLastReadRow = false,
                    resetStateParameters = ResetState(),
                    beforeInitQueryIds = emptyList(),
                    initQueryId = null,
                    initParameters = mapOf("id" to listOf(startId.toString())),
                    afterInitQueryIds = emptyList(),
                    useColumns = setOf("id"),
                    beforeUpdateQueryIds = emptyList(),
                    updateQueryId = QueryId("updates"),
                    updateParameters = emptyMap(),
                    afterUpdateQueryIds = emptyList(),
                    interval = interval,
                ),
                listener,
            )

            advanceTimeBy(interval.toMillis())

            val newData: List<Person> = (1..10).map { Person("new$it", Instant.now().truncatedTo(ChronoUnit.DAYS), "test-new-data-$it".toByteArray()) }
            insertData(newData)

            val pulledData = (persons + newData).drop(startId)

            advanceTimeBy(interval.toMillis() * 2)

            genericUpdateListener.assertCaptured(pulledData, interval.toMillis() * 2)
            listener.assertCaptured(pulledData)
            verify(messageLoader, never()).load(any(), isNull(), any())
            verifyNoInteractions(genericRowListener)
            reader.stopPullTask(taskId)

            advanceUntilIdle()
        }
    }

    @ParameterizedTest
    @ValueSource(ints = [1, 15, 30])
    fun `receives update from table (message loader request)`(startId: Int) {
        val genericUpdateListener = mock<UpdateListener> { }
        val genericRowListener = mock<RowListener> { }
        val messageLoader = mock<MessageLoader> {
            on { load(any(), isNull(), any()) }.thenReturn(persons[startId - 1].toTableRow(startId))
        }
        val interval = Duration.ofMillis(100)

        val dataSourceId = DataSourceId("persons")
        val queryId = QueryId("updates")
        val configuration = DataBaseReaderConfiguration(
            mapOf(
                dataSourceId to DataSourceConfiguration(
                    mysql.jdbcUrl,
                    mysql.username,
                    mysql.password,
                )
            ),
            mapOf(
                queryId to QueryConfiguration(
                    "SELECT * FROM test_data.person WHERE id > \${id:integer}"
                )
            )
        )
        val hashService: HashService = BaseHashServiceImpl(
            BaseDataSourceProvider(configuration.dataSources),
            BaseQueryProvider(configuration.queries)
        )

        runTest {
            val reader = spy(DataBaseReader.createDataBaseReader(
                configuration,
                this,
                genericUpdateListener,
                genericRowListener,
                messageLoader
            ))
            val listener = mock<UpdateListener> { }
            val taskId = reader.submitPullTask(
                PullTableRequest(
                    dataSourceId,
                    startFromLastReadRow = true,
                    resetStateParameters = ResetState(),
                    beforeInitQueryIds = emptyList(),
                    initQueryId = null,
                    initParameters = emptyMap(),
                    afterInitQueryIds = emptyList(),
                    useColumns = setOf("id"),
                    beforeUpdateQueryIds = emptyList(),
                    updateQueryId = queryId,
                    updateParameters = emptyMap(),
                    afterUpdateQueryIds = emptyList(),
                    interval = interval,
                ),
                listener,
            )

            advanceTimeBy(interval.toMillis())

            val newData: List<Person> = (1..10).map { Person("new$it", Instant.now().truncatedTo(ChronoUnit.DAYS), "test-new-data-$it".toByteArray()) }
            insertData(newData)

            val pulledData = (persons + newData).drop(startId)

            advanceTimeBy(interval.toMillis() * 2)

            genericUpdateListener.assertCaptured(pulledData, interval.toMillis() * 2)
            listener.assertCaptured(pulledData)
            verify(messageLoader).load(
                same(dataSourceId),
                isNull(),
                eq(
                    mapOf(
                        TH2_PULL_TASK_UPDATE_HASH_PROPERTY to hashService.calculateHash(dataSourceId, queryId).toString()
                    )
                )
            )
            verifyNoInteractions(genericRowListener)
            reader.stopPullTask(taskId)

            advanceUntilIdle()
        }
    }

    @Test
    fun `reset internal state by afterDate`() {
        val instant = Instant.now()
        val resetDate = instant.plus(1, ChronoUnit.MINUTES)
        val interval = Duration.ofMillis(100)

        val genericUpdateListener = mock<UpdateListener> { }
        val genericRowListener = mock<RowListener> { }
        val messageLoader = mock<MessageLoader> { }
        val clock = mock<Clock> {
            on { instant() }.thenReturn(instant)
        }

        runTest {
            val reader = DataBaseReader.createDataBaseReader(
                DataBaseReaderConfiguration(
                    mapOf(
                        DataSourceId("persons") to DataSourceConfiguration(
                            mysql.jdbcUrl,
                            mysql.username,
                            mysql.password,
                        )
                    ),
                    mapOf(
                        QueryId("updates") to QueryConfiguration(
                            "SELECT * FROM test_data.person WHERE id > \${id:integer}"
                        )
                    )
                ),
                this,
                genericUpdateListener,
                genericRowListener,
                messageLoader,
                clock
            )
            val listener = mock<UpdateListener> { }
            val taskId = reader.submitPullTask(
                PullTableRequest(
                    DataSourceId("persons"),
                    startFromLastReadRow = false,
                    resetStateParameters = ResetState(
                        afterDate = resetDate
                    ),
                    beforeInitQueryIds = emptyList(),
                    initQueryId = null,
                    initParameters = mapOf("id" to listOf(persons.size.toString())),
                    afterInitQueryIds = emptyList(),
                    useColumns = setOf("id"),
                    beforeUpdateQueryIds = emptyList(),
                    updateQueryId = QueryId("updates"),
                    updateParameters = emptyMap(),
                    afterUpdateQueryIds = emptyList(),
                    interval = interval,
                ),
                listener,
            )

            runCurrent()

            val newData: List<Person> = (1..10).map { Person("new$it", Instant.now().truncatedTo(ChronoUnit.DAYS), "test-new-data-$it".toByteArray()) }
            insertData(newData)

            advanceTimeBy(interval.toMillis() * 2)

            genericUpdateListener.assertCaptured(newData, interval.toMillis())
            listener.assertCaptured(newData)
            verify(messageLoader, never()).load(any(), isNull(), any())
            verifyNoInteractions(genericRowListener)

            clearInvocations(genericUpdateListener)
            clearInvocations(listener)
            whenever(clock.instant()).thenReturn(resetDate)
            advanceTimeBy(interval.toMillis() * 2)

            genericUpdateListener.assertCaptured(newData, interval.toMillis())
            listener.assertCaptured(newData)
            verify(messageLoader, never()).load(any(), isNull(), any())
            verifyNoInteractions(genericRowListener)

            reader.stopPullTask(taskId)
            advanceUntilIdle()
        }
    }

    @Test
    fun `reset internal state by afterTime`() {
        val instant = Instant.now()
        val resetDate = instant.plus(1, ChronoUnit.MINUTES)
        val resetTime = LocalTime.ofInstant(resetDate, ZoneOffset.UTC)
        val interval = Duration.ofMillis(100)

        val genericUpdateListener = mock<UpdateListener> { }
        val genericRowListener = mock<RowListener> { }
        val messageLoader = mock<MessageLoader> { }
        val clock = mock<Clock> {
            on { instant() }.thenReturn(instant)
        }

        runTest {
            val reader = DataBaseReader.createDataBaseReader(
                DataBaseReaderConfiguration(
                    mapOf(
                        DataSourceId("persons") to DataSourceConfiguration(
                            mysql.jdbcUrl,
                            mysql.username,
                            mysql.password,
                        )
                    ),
                    mapOf(
                        QueryId("updates") to QueryConfiguration(
                            "SELECT * FROM test_data.person WHERE id > \${id:integer}"
                        )
                    )
                ),
                this,
                genericUpdateListener,
                genericRowListener,
                messageLoader,
                clock
            )
            val listener = mock<UpdateListener> { }
            val taskId = reader.submitPullTask(
                PullTableRequest(
                    DataSourceId("persons"),
                    startFromLastReadRow = false,
                    resetStateParameters = ResetState(
                        afterTime = resetTime
                    ),
                    beforeInitQueryIds = emptyList(),
                    initQueryId = null,
                    initParameters = mapOf("id" to listOf(persons.size.toString())),
                    afterInitQueryIds = emptyList(),
                    useColumns = setOf("id"),
                    beforeUpdateQueryIds = emptyList(),
                    updateQueryId = QueryId("updates"),
                    updateParameters = emptyMap(),
                    afterUpdateQueryIds = emptyList(),
                    interval = interval,
                ),
                listener,
            )

            runCurrent()

            val newData: List<Person> = (1..10).map { Person("new$it", Instant.now().truncatedTo(ChronoUnit.DAYS), "test-new-data-$it".toByteArray()) }
            insertData(newData)

            advanceTimeBy(interval.toMillis() * 2)

            genericUpdateListener.assertCaptured(newData, interval.toMillis())
            listener.assertCaptured(newData)
            verify(messageLoader, never()).load(any(), isNull(), any())
            verifyNoInteractions(genericRowListener)

            clearInvocations(genericUpdateListener)
            clearInvocations(listener)
            whenever(clock.instant()).thenReturn(resetDate)
            advanceTimeBy(interval.toMillis() * 2)

            genericUpdateListener.assertCaptured(newData, interval.toMillis())
            listener.assertCaptured(newData)
            verify(messageLoader, never()).load(any(), isNull(), any())
            verifyNoInteractions(genericRowListener)

            reader.stopPullTask(taskId)
            advanceUntilIdle()
        }
    }

    private fun UpdateListener.assertCaptured(persons: List<Person>, timeout: Long = 0) {
        val tableRawCaptor = argumentCaptor<TableRow>()
        val propertiesCaptor = argumentCaptor<Map<String, String>>()
        verify(this, timeout(timeout).times(persons.size)).onUpdate(any(), tableRawCaptor.capture(), propertiesCaptor.capture())
        tableRawCaptor.allValues.map {
            Person(
                checkNotNull(it.columns["name"]),
                LocalDate.parse(checkNotNull(it.columns["birthday"])).atStartOfDay().toInstant(ZoneOffset.UTC),
                decodeHexDump(checkNotNull(it.columns["data"])),
            )
        }.also {
            expectThat(it).containsExactly(persons)
        }
        propertiesCaptor.allValues.forEach {
            assertEquals(1, it.size)
            assertNotNull(it[TH2_PULL_TASK_UPDATE_HASH_PROPERTY])
        }
    }
    private suspend fun RowListener.assertCaptured(persons: List<Person>, timeout: Long = 0) {
        val captor = argumentCaptor<TableRow>()
        verify(this, timeout(timeout).times(persons.size)).onRow(any(), captor.capture())
        captor.allValues.map {
            Person(
                checkNotNull(it.columns["name"]),
                LocalDate.parse(checkNotNull(it.columns["birthday"])).atStartOfDay().toInstant(ZoneOffset.UTC),
                decodeHexDump(checkNotNull(it.columns["data"])),
            )
        }.also {
            expectThat(it).containsExactly(persons)
        }
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
        insertData(persons)
        LOGGER.info { "Initial data inserted" }
    }

    private fun insertData(data: List<Person>) {
        execute {
            insertData(data)
        }
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

    private fun Person.toTableRow(id: Int): TableRow = TableRow(
        mapOf(
            "id" to DEFAULT_TRANSFORM(id),
            "name" to name,
            "birthday" to DEFAULT_TRANSFORM(birthday)
        )
    )

    companion object {
        private val LOGGER = KotlinLogging.logger { }
    }
}