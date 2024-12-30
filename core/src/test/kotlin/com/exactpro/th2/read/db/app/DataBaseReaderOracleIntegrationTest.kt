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

package com.exactpro.th2.read.db.app

import com.exactpro.th2.read.db.ORACLE_DOCKER_IMAGE
import com.exactpro.th2.read.db.annotations.IntegrationTest
import com.exactpro.th2.read.db.app.DataBaseReaderOracleIntegrationTest.Operation.DELETE
import com.exactpro.th2.read.db.app.DataBaseReaderOracleIntegrationTest.Operation.INSERT
import com.exactpro.th2.read.db.app.DataBaseReaderOracleIntegrationTest.Operation.UPDATE
import com.exactpro.th2.read.db.core.DataBaseMonitorService.Companion.TH2_PULL_TASK_UPDATE_HASH_PROPERTY
import com.exactpro.th2.read.db.core.DataSourceConfiguration
import com.exactpro.th2.read.db.core.DataSourceId
import com.exactpro.th2.read.db.core.MessageLoader
import com.exactpro.th2.read.db.core.QueryConfiguration
import com.exactpro.th2.read.db.core.QueryId
import com.exactpro.th2.read.db.core.RowListener
import com.exactpro.th2.read.db.core.TableRow
import com.exactpro.th2.read.db.core.UpdateListener
import io.github.oshai.kotlinlogging.KotlinLogging
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.test.advanceTimeBy
import kotlinx.coroutines.test.advanceUntilIdle
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.mockito.Mockito.timeout
import org.mockito.kotlin.any
import org.mockito.kotlin.argumentCaptor
import org.mockito.kotlin.doAnswer
import org.mockito.kotlin.isNull
import org.mockito.kotlin.mock
import org.mockito.kotlin.never
import org.mockito.kotlin.verify
import org.mockito.kotlin.verifyNoInteractions
import org.testcontainers.containers.OracleContainer
import org.testcontainers.utility.DockerImageName
import java.io.ByteArrayInputStream
import java.sql.Connection
import java.sql.Date
import java.time.Duration
import java.time.Instant
import java.time.ZoneId
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit

@OptIn(ExperimentalCoroutinesApi::class)
@IntegrationTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
internal class DataBaseReaderOracleIntegrationTest {
    private val oracle = OracleContainer(DockerImageName.parse(ORACLE_DOCKER_IMAGE))
    private lateinit var redoFiles: List<String>
    private val persons = (1..15).map {
        Person(it, "$TABLE_NAME$it", Instant.now().truncatedTo(ChronoUnit.DAYS), "test-init-data-$it".toByteArray())
    }

    private enum class Operation {
        INSERT,
        UPDATE,
        DELETE
    }

    private class Person(val id: Int, val name: String, val birthday: Instant, val data: ByteArray)
    private class HistoryEntity(val operation: Operation, val data: Person) {
        companion object {
            fun insert(data: Person) = HistoryEntity(INSERT, data)
            fun update(data: Person) = HistoryEntity(UPDATE, data)
            fun delete(data: Person) = HistoryEntity(DELETE, data)
        }
    }

    private class DataBaseEntity(val rowId: String, val data: Person)

    @BeforeAll
    fun init() {
        oracle.usingSid() // OracleContainer use `system` user for access
            .start()
    }

    @AfterAll
    fun cleanup() {
        oracle.stop()
    }

    @BeforeEach
    fun setup() {
        execute {
            dropTable()
            initTestData()
            selectRedoLogFiles()
        }
    }

    @Test
    fun `receives update from oracle log miner`() {
        val genericUpdateListener = mock<UpdateListener> {
            on { onError(any(), any()) }.doAnswer {
                LOGGER.error(it.getArgument(1, Throwable::class.java)) { "Update failure" }
            }
        }
        val genericRowListener = mock<RowListener> { }
        val messageLoader = mock<MessageLoader> { }
        val interval = Duration.ofMillis(100)
        runTest {
            val addLogFileQueries: Map<QueryId, QueryConfiguration> = redoFiles.asSequence()
                .mapIndexed { index, file ->
                    QueryId("add-logfile-$index") to QueryConfiguration(
                        "{CALL DBMS_LOGMNR.ADD_LOGFILE (LOGFILENAME => '$file', OPTIONS => DBMS_LOGMNR.ADDFILE)}"
                    )
                }.toMap()
            val reader = DataBaseReader.createDataBaseReader(
                DataBaseReaderConfiguration(
                    mapOf(
                        DataSourceId("persons") to DataSourceConfiguration(
                            oracle.jdbcUrl,
                            oracle.username,
                            oracle.password,
                        )
                    ),
                    mapOf(
                        QueryId("add-supplemental-log-for-table") to QueryConfiguration(
                            // add-supplemental-log for all tables: 'ALTER DATABASE ADD SUPPLEMENTAL LOG DATA'
                            "ALTER TABLE $TABLE_SPACE.$TABLE_NAME ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS"
                        ),
                        QueryId("start-log-miner") to QueryConfiguration(
                            "{CALL DBMS_LOGMNR.START_LOGMNR (OPTIONS => DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG + DBMS_LOGMNR.COMMITTED_DATA_ONLY)}"
                        ),
                        QueryId("stop-log-miner") to QueryConfiguration(
                            "{CALL DBMS_LOGMNR.END_LOGMNR}"
                        ),
                        QueryId("updates") to QueryConfiguration(
                            "SELECT SCN, ROW_ID, OPERATION, SQL_REDO FROM V\$LOGMNR_CONTENTS WHERE TABLE_NAME = '$TABLE_NAME' AND SCN > \${SCN:integer} AND OPERATION in ('INSERT', 'DELETE', 'UPDATE')"
                        )
                    ) + addLogFileQueries
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
                    beforeInitQueryIds = listOf(QueryId("add-supplemental-log-for-table")),
                    initQueryId = null,
                    initParameters = mapOf("SCN" to listOf("-1")),
                    afterInitQueryIds = emptyList(),
                    useColumns = setOf("SCN"),
                    beforeUpdateQueryIds = addLogFileQueries.keys.toList() + listOf(QueryId("start-log-miner")),
                    updateQueryId = QueryId("updates"),
                    updateParameters = emptyMap(),
                    afterUpdateQueryIds = listOf(QueryId("stop-log-miner")),
                    interval = interval,
                ),
                listener,
            )

            advanceTimeBy(interval.toMillis())

            val history: MutableList<HistoryEntity> = persons.map(HistoryEntity::insert).toMutableList()

            val newData: List<Person> = (16..30).map {
                Person(
                    it, "new$it",
                    Instant.now().truncatedTo(ChronoUnit.DAYS),
                    "test-new-data-$it".toByteArray()
                )
            }.also { history.addAll(it.map(HistoryEntity::insert)) }
            insertData(newData)

            val updateData: List<Person> = history.asSequence()
                .filter { it.operation == INSERT }
                .map {
                    with(it.data) {
                        Person(
                            id, "update$id",
                            Instant.now().truncatedTo(ChronoUnit.DAYS),
                            "test-update-data-$id".toByteArray()
                        )
                    }
                }.toList()
                .also { history.addAll(it.map(HistoryEntity::update)) }
            updateData(updateData)

            val deleteData: List<Person> = updateData
                .also { history.addAll(it.map(HistoryEntity::delete)) }
            deleteData(deleteData)

            advanceTimeBy(interval.toMillis() * 2)

            genericUpdateListener.assertCaptured(history, interval.toMillis() * 2)
            listener.assertCaptured(history)
            verify(messageLoader, never()).load(any(), isNull(), any())
            verifyNoInteractions(genericRowListener)
            reader.stopPullTask(taskId)

            advanceUntilIdle()
        }
    }

    private fun UpdateListener.assertCaptured(history: List<HistoryEntity>, timeout: Long = 0) {
        val tableRawCaptor = argumentCaptor<TableRow>()
        val propertiesCaptor = argumentCaptor<Map<String, String>>()
        verify(this, timeout(timeout).times(history.size)).onUpdate(
            any(),
            tableRawCaptor.capture(),
            propertiesCaptor.capture()
        )
        val dataBaseEntries = mutableMapOf<Int, DataBaseEntity>()
        tableRawCaptor.allValues.forEachIndexed { index, tableRow ->
            val historyEntity = history[index]

            checkNotNull(tableRow.columns["SCN"]) {
                "'SCN' column in $index row"
            }
            val rowId = checkNotNull(tableRow.columns["ROW_ID"]) {
                "'ROW_ID' column in $index row"
            }
            assertEquals(historyEntity.operation.name, tableRow.columns["OPERATION"]) {
                "'OPERATION' column in $index row"
            }
            val sql = checkNotNull(tableRow.columns["SQL_REDO"]) {
                "'SQL_REDO' column in $index row"
            }

            with(historyEntity.data) {
                when (historyEntity.operation) {
                    INSERT -> {
                        val regex = Regex("insert into \"$TABLE_SPACE\".\"$TABLE_NAME\"" +
                                "\\(\"ID\",\"NAME\",\"BIRTHDAY\",\"DATA\"\\) " +
                                "values \\('$id','$name',TO_DATE\\('${
                                    FORMATTER.format(birthday).uppercase()
                                }', 'DD-MON-RR'\\),HEXTORAW\\('${data.toHex()}'\\)\\);")
                        assertTrue(regex matches sql) {
                            "${historyEntity.operation}: query check in $index row, source '$sql', regex: $regex"
                        }
                        dataBaseEntries[id] = DataBaseEntity(rowId, this)
                    }

                    UPDATE -> {
                        val dataBaseEntity = checkNotNull(dataBaseEntries[id]) {
                            "${historyEntity.operation}: source database entry doesn't found for $id id in $index row"
                        }
                        assertEquals(
                            dataBaseEntity.rowId,
                            rowId,
                            "${historyEntity.operation}: ROW_ID check in $index row"
                        )
                        val regex = Regex("update \"$TABLE_SPACE\".\"$TABLE_NAME\" " +
                                "set \"NAME\" = '$name', \"DATA\" = HEXTORAW\\('${data.toHex()}'\\) " +
                                "where \"NAME\" = '${dataBaseEntity.data.name}' " +
                                "and \"DATA\" = HEXTORAW\\('${dataBaseEntity.data.data.toHex()}'\\) " +
                                "and ROWID = '${dataBaseEntity.rowId}';")
                        assertTrue(regex matches sql) {
                            "${historyEntity.operation}: query check in $index row, source '$sql', regex: $regex"
                        }
                        dataBaseEntries[id] = DataBaseEntity(rowId, this)
                    }
                    DELETE -> {
                        val dataBaseEntity = checkNotNull(dataBaseEntries[id]) {
                            "${historyEntity.operation}: source database entry doesn't found for $id id in $index row"
                        }
                        assertEquals(
                            dataBaseEntity.rowId,
                            rowId,
                            "${historyEntity.operation}: ROW_ID check in $index row"
                        )
                        val regex = Regex("delete from \"$TABLE_SPACE\".\"$TABLE_NAME\" " +
                                "where \"ID\" = '${dataBaseEntity.data.id}' " +
                                "and \"NAME\" = '${dataBaseEntity.data.name}' " +
                                "and \"BIRTHDAY\" = TO_DATE\\('${
                                    FORMATTER.format(dataBaseEntity.data.birthday).uppercase()
                                }', 'DD-MON-RR'\\) " +
                                "and ROWID = '${dataBaseEntity.rowId}';")
                        assertTrue(regex matches sql) {
                            "${historyEntity.operation}: query check in $index row, source '$sql', regex: $regex"
                        }
                        dataBaseEntries.remove(id)
                    }
                }
            }
        }

        propertiesCaptor.allValues.forEach {
            assertEquals(1, it.size)
            assertNotNull(it[TH2_PULL_TASK_UPDATE_HASH_PROPERTY])
        }
    }

    private inline fun execute(action: Connection.() -> Unit) {
        oracle.createConnection("").use { it.action() }
    }

    private fun Connection.dropTable() {
        createStatement()
            .execute(
                """
                BEGIN
                   EXECUTE IMMEDIATE 'DROP TABLE $TABLE_NAME';
                EXCEPTION
                   WHEN OTHERS THEN
                      IF SQLCODE != -942 THEN
                         RAISE;
                      END IF;
                END;
            """.trimIndent()
            )
        LOGGER.info { "table dropped" }
    }

    private fun Connection.initTestData() {
        createStatement().apply {
            execute(
                """
                    CREATE TABLE $TABLE_NAME 
                     (
                      ID INT NOT NULL ,
                      NAME VARCHAR(45) NOT NULL ,
                      BIRTHDAY DATE NOT NULL ,
                      DATA BLOB NOT NULL
                     )
                """.trimIndent()
            )
        }
        LOGGER.info { "table created" }
        insertData(persons)
        LOGGER.info { "Initial data inserted" }
    }

    private fun insertData(data: List<Person>) {
        execute {
            insertData(data)
        }
    }

    private fun updateData(data: List<Person>) {
        execute {
            updateData(data)
        }
    }

    private fun deleteData(data: List<Person>) {
        execute {
            deleteData(data)
        }
    }

    private fun Connection.selectRedoLogFiles() {
        redoFiles = createStatement()
            .executeQuery("SELECT DISTINCT MEMBER LOGFILENAME FROM V\$LOGFILE")
            .run {
                generateSequence {
                    if (next()) {
                        getObject(1)?.toString()
                    } else {
                        null
                    }
                }.toList()
            }.also {
                LOGGER.info { "selected redo log files $it" }
            }
    }

    private fun Connection.insertData(persons: List<Person>) {
        val prepareStatement = prepareStatement(
            """
                INSERT INTO $TABLE_NAME (ID, NAME, BIRTHDAY, DATA)
                VALUES (?, ?, ?, ?)
            """.trimIndent()
        )
        for (person in persons) {
            prepareStatement.setInt(1, person.id)
            prepareStatement.setString(2, person.name)
            prepareStatement.setDate(3, Date(person.birthday.toEpochMilli()))
            prepareStatement.setBlob(4, ByteArrayInputStream(person.data))
            prepareStatement.addBatch()
        }
        prepareStatement.executeBatch()
    }

    private fun Connection.updateData(persons: List<Person>) {
        val prepareStatement = prepareStatement(
            """
                UPDATE $TABLE_NAME SET NAME = ?, BIRTHDAY = ?, DATA = ?
                WHERE ID = ?
            """.trimIndent()
        )
        for (person in persons) {
            prepareStatement.setString(1, person.name)
            prepareStatement.setDate(2, Date(person.birthday.toEpochMilli()))
            prepareStatement.setBlob(3, ByteArrayInputStream(person.data))
            prepareStatement.setInt(4, person.id)
            prepareStatement.addBatch()
        }
        prepareStatement.executeBatch()
    }

    private fun Connection.deleteData(persons: List<Person>) {
        val prepareStatement = prepareStatement(
            """
                DELETE FROM $TABLE_NAME
                WHERE ID = ?
            """.trimIndent()
        )
        for (person in persons) {
            prepareStatement.setInt(1, person.id)
            prepareStatement.addBatch()
        }
        prepareStatement.executeBatch()
    }

    companion object {
        private val LOGGER = KotlinLogging.logger { }
        private val FORMATTER = DateTimeFormatter.ofPattern("dd-MMM-uu")
            .withZone(ZoneId.systemDefault())

        private const val TABLE_SPACE = "SYSTEM"
        private const val TABLE_NAME = "PERSON"

        @OptIn(ExperimentalUnsignedTypes::class)
        fun ByteArray.toHex(): String = asUByteArray().joinToString("") { it.toString(radix = 16).padStart(2, '0') }
    }
}