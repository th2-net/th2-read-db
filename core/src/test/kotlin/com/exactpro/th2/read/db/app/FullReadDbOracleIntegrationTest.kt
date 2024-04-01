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

import com.exactpro.th2.common.grpc.MessageID
import com.exactpro.th2.common.message.toJson
import com.exactpro.th2.common.schema.factory.CommonFactory
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.Message
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.RawMessage
import com.exactpro.th2.common.utils.message.transport.logId
import com.exactpro.th2.common.utils.message.transport.toProto
import com.exactpro.th2.dataprovider.lw.grpc.DataProviderGrpc
import com.exactpro.th2.dataprovider.lw.grpc.DataProviderService
import com.exactpro.th2.dataprovider.lw.grpc.MessageGroupResponse
import com.exactpro.th2.dataprovider.lw.grpc.MessageGroupsSearchRequest
import com.exactpro.th2.dataprovider.lw.grpc.MessageSearchResponse
import com.exactpro.th2.read.db.annotations.IntegrationTest
import com.exactpro.th2.read.db.bootstrap.setupApp
import com.exactpro.th2.read.db.containers.OracleContainer
import com.exactpro.th2.read.db.grpc.DbPullRequest
import com.exactpro.th2.read.db.grpc.ReadDbService
import com.exactpro.th2.read.db.grpc.StopPullingRequest
import com.exactpro.th2.test.annotations.CustomConfigProvider
import com.exactpro.th2.test.annotations.Th2AppFactory
import com.exactpro.th2.test.annotations.Th2IntegrationTest
import com.exactpro.th2.test.annotations.Th2TestFactory
import com.exactpro.th2.test.extension.CleanupExtension
import com.exactpro.th2.test.spec.CradleSpec
import com.exactpro.th2.test.spec.CustomConfigSpec
import com.exactpro.th2.test.spec.GrpcSpec
import com.exactpro.th2.test.spec.RabbitMqSpec
import com.exactpro.th2.test.spec.client
import com.exactpro.th2.test.spec.pin
import com.exactpro.th2.test.spec.pins
import com.exactpro.th2.test.spec.publishers
import com.exactpro.th2.test.spec.server
import com.google.protobuf.UnsafeByteOperations
import com.google.protobuf.util.Durations
import com.google.protobuf.util.Timestamps
import io.grpc.stub.StreamObserver
import mu.KotlinLogging
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource
import org.testcontainers.shaded.org.awaitility.Awaitility.await
import java.sql.Connection
import java.sql.Timestamp
import java.time.Instant
import java.util.concurrent.TimeUnit

private const val MESSAGE_OUT_PIN = "out"
private const val TABLE_NAME = "RECORD"
private const val RECORDS_DATA_SOURCE_ID = "records"
private const val PULL_BY_DATE_QUERY_ID = "query-pull-by-date"
private const val PULL_BY_TIMESTAMP_QUERY_ID = "query-pull-by-timestamp"
private const val PULL_BY_TIMESTAMP_WITH_TIME_ZONE_QUERY_ID = "query-pull-by-timestamp-with-time-zone"
private const val PULL_BY_TIMESTAMP_WITH_LOCAL_TIME_ZONE_QUERY_ID = "query-pull-by-timestamp-with-local-time-zone"
private const val DATE_COLUMN = "DATE_COLUMN"
private const val TIMESTAMP_COLUMN = "TIMESTAMP_COLUMN"
private const val TIMESTAMP_WITH_TIME_ZONE_COLUMN = "TIMESTAMP_WITH_TIME_ZONE_COLUMN"
private const val TIMESTAMP_WITH_LOCAL_TIME_ZONE_COLUMN = "TIMESTAMP_WITH_LOCAL_TIME_ZONE_COLUMN"

@Th2IntegrationTest
@IntegrationTest
internal class FullReadDbOracleIntegrationTest {
    private val oracle = OracleContainer()

    @Suppress("unused")
    val grpc = GrpcSpec.create()
        .server<ReadDbService>()
        .client<DataProviderService>("dp")

    @Suppress("unused")
    val mq = RabbitMqSpec.create()
        .pins {
            publishers {
                pin(MESSAGE_OUT_PIN) {
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
        oracle.withDatabaseName("test_data")
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
        }
    }

    @ParameterizedTest
    @CsvSource(
        "$PULL_BY_DATE_QUERY_ID,$DATE_COLUMN",
        "$PULL_BY_TIMESTAMP_QUERY_ID,$TIMESTAMP_COLUMN",
//        "$PULL_BY_TIMESTAMP_WITH_TIME_ZONE_QUERY_ID,$TIMESTAMP_WITH_TIME_ZONE_COLUMN",
//        "$PULL_BY_TIMESTAMP_WITH_LOCAL_TIME_ZONE_QUERY_ID,$TIMESTAMP_WITH_LOCAL_TIME_ZONE_COLUMN",
    )
    @CustomConfigProvider("config")
    fun `pull by date filed type test`(
        queryId: String,
        column: String,
        @Th2AppFactory factory: CommonFactory,
        @Th2TestFactory test: CommonFactory,
        resourceCleaner: CleanupExtension.Registry,
    ) {
        val publishedMessages = mutableListOf<Message<*>>()
        val dbPullRequest = DbPullRequest.newBuilder().apply {
            sourceIdBuilder.setId(RECORDS_DATA_SOURCE_ID)
            updateQueryIdBuilder.setId(queryId)
            initParametersBuilder
                .putValues(column, "1970-01-01 00:00:00.000")
            addUseColumns(column)
            startFromLastReadRow = true
            pullInterval = Durations.fromMillis(200)
            resetStateParametersBuilder.setAfterDate(Timestamps.now())
        }.build()

        val dp = DataProviderGrpcServer().apply { test.grpcRouter.startServer(this).start() }
        test.transportGroupBatchRouter.subscribe({ _, batch ->
            batch.groups.asSequence()
                .flatMap { it.messages.asSequence() }
                .forEach {
                    check(it is RawMessage) {
                        "Unexpected message type ${it::class.java}, id: ${it.id.logId}"
                    }
                    dp.set(it.id.toProto(batch.book, batch.sessionGroup), it.body.array(), it.metadata)
                    publishedMessages.add(it)
                }
        }, MESSAGE_OUT_PIN)

        setupApp(factory) { name, resource -> resourceCleaner.add(name, resource) }
        val readDb = test.grpcRouter.getService(ReadDbService::class.java)

        execute { insertData(listOf(Record(1, Instant.now(), Instant.now(), Instant.now(), Instant.now()))) }
        var dbPullResponse = readDb.startPulling(dbPullRequest)
        await("first message").atMost(5, TimeUnit.SECONDS).until { publishedMessages.size == 1 }
        readDb.stopPulling(StopPullingRequest.newBuilder().setId(dbPullResponse.id).build())

        Thread.sleep(1)

        execute { insertData(listOf(Record(2, Instant.now(), Instant.now(), Instant.now(), Instant.now()))) }
        dbPullResponse = readDb.startPulling(dbPullRequest)
        await("second message").atMost(5, TimeUnit.SECONDS).until { publishedMessages.size == 2 }
        readDb.stopPulling(StopPullingRequest.newBuilder().setId(dbPullResponse.id).build())

    }

    @Suppress("unused")
    fun config() = CustomConfigSpec.fromString(
        """
            {
                "dataSources": {
                    "$RECORDS_DATA_SOURCE_ID": {
                        "url": "${oracle.jdbcUrl}",
                        "username": "${oracle.username}",
                        "password": "${oracle.password}"
                    }
                },
                "queries": {
                    "$PULL_BY_DATE_QUERY_ID": {
                        "query": "SELECT * FROM $TABLE_NAME WHERE $DATE_COLUMN > ${"$"}{$DATE_COLUMN:timestamp}"
                    },
                    "$PULL_BY_TIMESTAMP_QUERY_ID": {
                        "query": "SELECT * FROM $TABLE_NAME WHERE $TIMESTAMP_COLUMN > ${"$"}{$TIMESTAMP_COLUMN:timestamp}"
                    },
                    "$PULL_BY_TIMESTAMP_WITH_TIME_ZONE_QUERY_ID": {
                        "query": "SELECT * FROM $TABLE_NAME WHERE $TIMESTAMP_WITH_TIME_ZONE_COLUMN > ${"$"}{$TIMESTAMP_WITH_TIME_ZONE_COLUMN:timestamp}"
                    },
                    "$PULL_BY_TIMESTAMP_WITH_LOCAL_TIME_ZONE_QUERY_ID": {
                        "query": "SELECT * FROM $TABLE_NAME WHERE $TIMESTAMP_WITH_LOCAL_TIME_ZONE_COLUMN > ${"$"}{$TIMESTAMP_WITH_LOCAL_TIME_ZONE_COLUMN:timestamp}"
                    }
                },
                "publication": {
                    "maxDelayMillis": 1,
                    "maxBatchSize": 1
                },
                "useTransport": true
            }
            """.trimIndent()
    )

    private fun execute(action: Connection.() -> Unit) {
        oracle.createConnection("").use { it.action() }
    }

    private fun Connection.insertData(records: List<Record>) {
        val prepareStatement = prepareStatement(
            """
                INSERT INTO $TABLE_NAME (ID, $DATE_COLUMN, $TIMESTAMP_COLUMN, $TIMESTAMP_WITH_TIME_ZONE_COLUMN, $TIMESTAMP_WITH_LOCAL_TIME_ZONE_COLUMN)
                VALUES (?, ?, ?, ?, ?)
            """.trimIndent()
        )
        for (record in records) {
            prepareStatement.setInt(1, record.id)
            prepareStatement.setTimestamp(2, Timestamp(record.date.toEpochMilli()))
            prepareStatement.setTimestamp(3, Timestamp(record.timestamp.toEpochMilli()))
            prepareStatement.setTimestamp(4, Timestamp(record.timestampWithTimeZone.toEpochMilli()))
            prepareStatement.setTimestamp(5, Timestamp(record.timestampWithLocalTimeZone.toEpochMilli()))
            prepareStatement.addBatch()
        }
        prepareStatement.executeBatch()
        LOGGER.info { "Inserted $records" }
    }

    private fun Connection.initTestData() {
        createStatement()
            .execute(
                """
                    CREATE TABLE $TABLE_NAME   
                     (
                      ID INT NOT NULL ,
                      $DATE_COLUMN DATE NOT NULL ,
                      $TIMESTAMP_COLUMN TIMESTAMP NOT NULL,
                      $TIMESTAMP_WITH_TIME_ZONE_COLUMN TIMESTAMP WITH TIME ZONE NOT NULL, 
                      $TIMESTAMP_WITH_LOCAL_TIME_ZONE_COLUMN TIMESTAMP WITH LOCAL TIME ZONE NOT NULL
                     )
                """.trimIndent()
            )
        LOGGER.info { "table created" }
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

    private data class Record(
        val id: Int,
        val date: Instant,
        val timestamp: Instant,
        val timestampWithTimeZone: Instant,
        val timestampWithLocalTimeZone: Instant,
    )

    private class DataProviderGrpcServer: DataProviderGrpc.DataProviderImplBase() {
        @Volatile
        private var response: MessageGroupResponse? = null

        override fun searchMessageGroups(
            request: MessageGroupsSearchRequest,
            responseObserver: StreamObserver<MessageSearchResponse>
        ) {
            response?.let { message ->
                LOGGER.info { "Return ${message.toJson()}" }
                responseObserver.onNext(
                    MessageSearchResponse.newBuilder().setMessage(message).build()
                )
            }
            responseObserver.onCompleted()
        }

        fun set(messageID: MessageID, body: ByteArray, properties: Map<String, String>) {
            response = MessageGroupResponse.newBuilder()
                .setMessageId(messageID)
                .setBodyRaw(UnsafeByteOperations.unsafeWrap(body))
                .putAllMessageProperties(properties)
                .build()
        }
    }

    companion object {
        private val LOGGER = KotlinLogging.logger { }
    }
}