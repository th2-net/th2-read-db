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
import com.exactpro.th2.read.db.core.DataSourceConfiguration
import com.exactpro.th2.read.db.core.DataSourceId
import com.exactpro.th2.read.db.core.QueryConfiguration
import com.exactpro.th2.read.db.core.QueryId
import com.exactpro.th2.read.db.core.ResultListener
import com.exactpro.th2.read.db.core.RowListener
import com.exactpro.th2.read.db.core.TableRow
import com.exactpro.th2.read.db.core.UpdateListener
import com.nhaarman.mockitokotlin2.any
import com.nhaarman.mockitokotlin2.argumentCaptor
import com.nhaarman.mockitokotlin2.mock
import com.nhaarman.mockitokotlin2.times
import com.nhaarman.mockitokotlin2.verify
import com.nhaarman.mockitokotlin2.verifyZeroInteractions
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.delay
import kotlinx.coroutines.test.advanceTimeBy
import kotlinx.coroutines.test.advanceUntilIdle
import kotlinx.coroutines.test.runTest
import mu.KotlinLogging
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
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
        runTest {
            val reader = DataBaseReader.createDataBaseReader(
                DataBaseReaderConfiguration(
                    mapOf(
                        DataSourceId("persons") to DataSourceConfiguration(
                            mysql.jdbcUrl,
                            mysql.username,
                            mysql.password,
                        ).wrap()
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
                genericRowListener
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
            verifyZeroInteractions(genericUpdateListener)
        }
    }

    @Test
    fun `receives update from table`() {
        val genericUpdateListener = mock<UpdateListener> { }
        val genericRowListener = mock<RowListener> { }
        val interval = Duration.ofMillis(100)
        runTest {
            val reader = DataBaseReader.createDataBaseReader(
                DataBaseReaderConfiguration(
                    mapOf(
                        DataSourceId("persons") to DataSourceConfiguration(
                            mysql.jdbcUrl,
                            mysql.username,
                            mysql.password,
                        ).wrap()
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
                genericRowListener
            )
            val listener = mock<UpdateListener> { }
            val taskId = reader.submitPullTask(
                PullTableRequest(
                    DataSourceId("persons"),
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
            verifyZeroInteractions(genericRowListener)
            reader.stopPullTask(taskId)

            advanceUntilIdle()
        }
    }

    private fun UpdateListener.assertCaptured(persons: List<Person>) {
        val captor = argumentCaptor<TableRow>()
        verify(this, times(persons.size)).onUpdate(any(), captor.capture())
        captor.allValues.map {
            Person(
                checkNotNull(it.columns["name"]).toString(),
                (checkNotNull(it.columns["birthday"]) as LocalDate).atStartOfDay().toInstant(ZoneOffset.UTC),
            )
        }.also {
            expectThat(it).containsExactly(persons)
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

    private fun DataSourceConfiguration.wrap(): DataSourceParameters = DataSourceParameters().apply {
        sourceConfiguration = this@wrap
    }

    companion object {
        private val LOGGER = KotlinLogging.logger { }
    }
}