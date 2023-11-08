/*
 * Copyright 2022-2023 Exactpro (Exactpro Systems Limited)
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
import com.exactpro.th2.read.db.core.impl.BaseDataSourceProvider
import com.exactpro.th2.read.db.core.impl.BaseHashServiceImpl
import com.exactpro.th2.read.db.core.impl.BaseQueryProvider
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.delay
import kotlinx.coroutines.test.advanceTimeBy
import kotlinx.coroutines.test.advanceUntilIdle
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
import org.junit.jupiter.params.provider.ValueSource
import org.mockito.kotlin.any
import org.mockito.kotlin.argumentCaptor
import org.mockito.kotlin.eq
import org.mockito.kotlin.mock
import org.mockito.kotlin.never
import org.mockito.kotlin.same
import org.mockito.kotlin.spy
import org.mockito.kotlin.times
import org.mockito.kotlin.verify
import org.mockito.kotlin.verifyNoInteractions
import strikt.api.expectThat
import strikt.assertions.containsExactly
import java.sql.Connection
import java.sql.Date
import java.time.Duration
import java.time.Instant
import java.time.LocalDate
import java.time.ZoneOffset
import java.time.temporal.ChronoUnit

@OptIn(ExperimentalCoroutinesApi::class)
@IntegrationTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
internal class DataBaseReaderIntegrationTest {
    private val mysql = MySqlContainer()
    private val persons = (1..30).map {
        Person("person$it", Instant.now().truncatedTo(ChronoUnit.DAYS))
    }

    private data class Person(val name: String, val birthday: Instant)

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
    fun `receives data from database`() {
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
                    QueryId("all"),
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
                    false,
                    QueryId("current_state"),
                    emptyMap(),
                    setOf("id"),
                    QueryId("updates"),
                    emptyMap(),
                    interval,
                ),
                listener,
            )

            advanceTimeBy(interval.toMillis())

            val newData: List<Person> = (1..10).map { Person("new$it", Instant.now().truncatedTo(ChronoUnit.DAYS)) }
            insertData(newData)

            advanceTimeBy(interval.toMillis() * 2)
            delay(interval.toMillis() * 2)

            genericUpdateListener.assertCaptured(newData)
            listener.assertCaptured(newData)
            verify(messageLoader, never()).load(any(), any())
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
                    false,
                    null,
                    mapOf("id" to listOf(startId.toString())),
                    setOf("id"),
                    QueryId("updates"),
                    emptyMap(),
                    interval,
                ),
                listener,
            )

            advanceTimeBy(interval.toMillis())

            val newData: List<Person> = (1..10).map { Person("new$it", Instant.now().truncatedTo(ChronoUnit.DAYS)) }
            insertData(newData)

            val pulledData = (persons + newData).drop(startId)

            advanceTimeBy(interval.toMillis() * 2)
            delay(interval.toMillis() * 2)

            genericUpdateListener.assertCaptured(pulledData)
            listener.assertCaptured(pulledData)
            verify(messageLoader, never()).load(any(), any())
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
            on { load(any(), any()) }.thenReturn(persons[startId - 1].toTableRow(startId))
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
                    true,
                    null,
                    emptyMap(),
                    setOf("id"),
                    queryId,
                    emptyMap(),
                    interval,
                ),
                listener,
            )

            advanceTimeBy(interval.toMillis())

            val newData: List<Person> = (1..10).map { Person("new$it", Instant.now().truncatedTo(ChronoUnit.DAYS)) }
            insertData(newData)

            val pulledData = (persons + newData).drop(startId)

            advanceTimeBy(interval.toMillis() * 2)
            delay(interval.toMillis() * 2)

            genericUpdateListener.assertCaptured(pulledData)
            listener.assertCaptured(pulledData)
            verify(messageLoader).load(
                same(dataSourceId),
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

    private fun UpdateListener.assertCaptured(persons: List<Person>) {
        val tableRawCaptor = argumentCaptor<TableRow>()
        val propertiesCaptor = argumentCaptor<Map<String, String>>()
        verify(this, times(persons.size)).onUpdate(any(), tableRawCaptor.capture(), propertiesCaptor.capture())
        tableRawCaptor.allValues.map {
            Person(
                checkNotNull(it.columns["name"]).toString(),
                (checkNotNull(it.columns["birthday"]) as LocalDate).atStartOfDay().toInstant(ZoneOffset.UTC),
            )
        }.also {
            expectThat(it).containsExactly(persons)
        }
        propertiesCaptor.allValues.forEach {
            assertEquals(1, it.size)
            assertNotNull(it[TH2_PULL_TASK_UPDATE_HASH_PROPERTY])
        }
    }
    private fun RowListener.assertCaptured(persons: List<Person>) {
        val captor = argumentCaptor<TableRow>()
        verify(this, times(persons.size)).onRow(any(), captor.capture())
        captor.allValues.map {
            Person(
                checkNotNull(it.columns["name"]).toString(),
                (checkNotNull(it.columns["birthday"]) as LocalDate).atStartOfDay().toInstant(ZoneOffset.UTC),
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
                        INSERT INTO `test_data`.`person` (`name`, `birthday`)
                        VALUES
                        (?, ?);
                    """.trimIndent()
        )
        for (person in persons) {
            prepareStatement.setString(1, person.name)
            prepareStatement.setDate(2, Date(person.birthday.toEpochMilli()))
            prepareStatement.addBatch()
        }
        prepareStatement.executeBatch()
    }

    private fun Person.toTableRow(id: Int): TableRow = TableRow(
        mapOf(
            "id" to id,
            "name" to name,
            "birthday" to birthday.toString()
        )
    )

    companion object {
        private val LOGGER = KotlinLogging.logger { }
    }
}