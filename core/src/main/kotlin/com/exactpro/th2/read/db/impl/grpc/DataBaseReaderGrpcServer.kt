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

package com.exactpro.th2.read.db.impl.grpc

import com.exactpro.th2.common.event.Event
import com.exactpro.th2.common.grpc.EventID
import com.exactpro.th2.common.message.toJavaDuration
import com.exactpro.th2.common.message.toJson
import com.exactpro.th2.common.utils.message.toTimestamp
import com.exactpro.th2.read.db.app.DataBaseReader
import com.exactpro.th2.read.db.app.ExecuteQueryRequest
import com.exactpro.th2.read.db.app.PullTableRequest
import com.exactpro.th2.read.db.bootstrap.toStringValue
import com.exactpro.th2.read.db.core.DataSourceConfiguration
import com.exactpro.th2.read.db.core.DataSourceId
import com.exactpro.th2.read.db.core.QueryConfiguration
import com.exactpro.th2.read.db.core.QueryId
import com.exactpro.th2.read.db.core.ResultListener
import com.exactpro.th2.read.db.core.TableRow
import com.exactpro.th2.read.db.core.UpdateListener
import com.exactpro.th2.read.db.grpc.DbPullRequest
import com.exactpro.th2.read.db.grpc.DbPullResponse
import com.exactpro.th2.read.db.grpc.QueryReport
import com.exactpro.th2.read.db.grpc.QueryRequest
import com.exactpro.th2.read.db.grpc.QueryResponse
import com.exactpro.th2.read.db.grpc.ReadDbGrpc
import com.exactpro.th2.read.db.grpc.StopPullingRequest
import com.exactpro.th2.read.db.impl.grpc.util.toGrpc
import com.exactpro.th2.read.db.impl.grpc.util.toModel
import com.google.protobuf.Empty
import com.google.protobuf.Message
import io.grpc.Status
import io.grpc.StatusRuntimeException
import io.grpc.stub.ServerCallStreamObserver
import io.grpc.stub.StreamObserver
import mu.KotlinLogging
import java.time.Instant
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong
import com.exactpro.th2.read.db.grpc.QueryId as ProtoQueryId

class DataBaseReaderGrpcServer(
    private val app: DataBaseReader,
    private val getSourceCfg: (DataSourceId) -> DataSourceConfiguration,
    private val getQueryCfg: (QueryId) -> QueryConfiguration,
    private val onEvent: OnEvent,
) : ReadDbGrpc.ReadDbImplBase() {
    override fun execute(request: QueryRequest, responseObserver: StreamObserver<QueryResponse>) {
        execute("execute", request, responseObserver) { event, parentEventId, executionId ->
            GrpcExecuteListener(responseObserver, executionId, event) {
                onEvent.accept(it, parentEventId)
            }
        }
    }

    override fun load(request: QueryRequest, responseObserver: StreamObserver<QueryReport>) {
        execute("load", request, responseObserver) { event, parentEventId, executionId ->
            val report = QueryReport.newBuilder()
                .setStart(event.startTimestamp.toTimestamp())
                .setExecutionId(executionId)
            GrpcLoadListener(responseObserver, report, event) {
                onEvent.accept(it, parentEventId)
            }
        }
    }

    override fun startPulling(request: DbPullRequest, responseObserver: StreamObserver<DbPullResponse>) {
        LOGGER.info { "Executing 'start pulling' grpc request ${request.toJson()}" }
        try {
            val associatedMessageType: String? = if (request.hasAssociatedMessageType()) request.associatedMessageType.name else null
            val id = app.submitPullTask(
                request.run {
                    PullTableRequest(
                        sourceId.toModel(),
                        startFromLastReadRow,
                        resetStateParameters.toModel(),
                        beforeInitQueryIdsList.map(ProtoQueryId::toModel),
                        if (hasInitQueryId()) initQueryId.toModel() else null,
                        initParameters.toModel(),
                        afterInitQueryIdsList.map(ProtoQueryId::toModel),
                        useColumnsList.toSet(),
                        beforeUpdateQueryIdsList.map(ProtoQueryId::toModel),
                        updateQueryId.toModel(),
                        updateParameters.toModel(),
                        afterUpdateQueryIdsList.map(ProtoQueryId::toModel),
                        pullInterval.toJavaDuration(),
                    )
                },
                DummyUpdateListener,
            ) { row ->
                associatedMessageType?.let { row.copy(associatedMessageType = it) } ?: row
            }
            with(responseObserver) {
                onNext(
                    DbPullResponse.newBuilder()
                        .setId(id.toGrpc())
                        .build()
                )
                onCompleted()
            }
        } catch (ex: Exception) {
            LOGGER.error(ex) { "cannot submit pulling task for request ${request.toJson()}" }
            responseObserver.onError(Status.INTERNAL.withDescription(ex.message).asRuntimeException())
        }
    }

    override fun stopPulling(request: StopPullingRequest, responseObserver: StreamObserver<Empty>) {
        LOGGER.info { "Executing 'stop pulling' grpc request ${request.toJson()}" }
        try {
            require(request.hasId()) { "missing id in the request" }
            app.stopPullTask(request.id.toModel())
            responseObserver.onNext(Empty.getDefaultInstance())
            responseObserver.onCompleted()
        } catch (ex: Exception) {
            LOGGER.error(ex) { "cannot execute stop pulling request ${request.toJson()}" }
            responseObserver.onError(Status.INTERNAL.withDescription(ex.message).asRuntimeException())
        }
    }

    private fun <T: Message> execute(
        name: String,
        request: QueryRequest,
        responseObserver: StreamObserver<T>,
        createListener: (Event, EventID?, Long) -> ResultListener
    ) {
        val executionId = EXECUTION_COUNTER.incrementAndGet()
        LOGGER.info { "Executing '$name' grpc request ${request.toJson()}, execution id: $executionId" }
        val event = Event.start()
            .name("Execute '${request.queryId.id}' query ($executionId)")
            .type("read-db.execute")
        val parentEventId: EventID? = if (request.hasParentEventId()) request.parentEventId else null
        try {
            val associatedMessageType: String? = if (request.hasAssociatedMessageType()) request.associatedMessageType.name else null
            val executeQueryRequest = request.run {
                ExecuteQueryRequest(
                    sourceId.toModel(),
                    beforeQueryIdsList.map(ProtoQueryId::toModel),
                    queryId.toModel(),
                    afterQueryIdsList.map(ProtoQueryId::toModel),
                    parameters.toModel(),
                )
            }
            event.bodyData(executeQueryRequest.toBody(executionId))
            app.executeQuery(
                executeQueryRequest,
                createListener(event, parentEventId, executionId)
            ) { row ->
                row.copy(associatedMessageType = associatedMessageType, executionId = executionId)
            }
        } catch (ex: Exception) {
            LOGGER.error(ex) { "cannot execute request ${request.toJson()}" }
            responseObserver.onError(Status.INTERNAL.withDescription(ex.message).asRuntimeException())
            event.exception(ex, true)
                .also { onEvent.accept(it, parentEventId) }
        }
    }

    private class GrpcExecuteListener(
        streamObserver: StreamObserver<QueryResponse>,
        private val executionId: Long,
        private val event: Event,
        private val onEvent: (Event) -> Unit,
    ) : ResultListener {
        private val skipped = AtomicLong()
        private val observer = streamObserver as ServerCallStreamObserver<QueryResponse>

        override fun onRow(sourceId: DataSourceId, row: TableRow) {
            check(executionId == row.executionId) {
                "'Execution id' isn't equal to '$executionId', row: $row"
            }

            while (!observer.isReady) {
                if (observer.isCancelled) {
                    skipped.incrementAndGet()
                    return
                }
                Thread.yield()
            }

            try {
                observer.onNext(
                    QueryResponse.newBuilder()
                        .putRows(row)
                        .setExecutionId(row.executionId)
                        .build()
                )
            } catch (e: StatusRuntimeException) {
                LOGGER.error(e) { "Couldn't send next gRPC message for '$executionId' execution" }
                skipped.incrementAndGet()
            }
        }

        override fun onError(error: Throwable) {
            observer.onError(error)
            event.endTimestamp()
                .exception(error, true)
                .also(onEvent)
            logSkipped()
        }

        override fun onComplete() {
            observer.onCompleted()
            event.endTimestamp()
                .also(onEvent)
            logSkipped()
        }

        private fun logSkipped() {
            if (skipped.get() > 0) {
                LOGGER.warn { "gRPC consumer doesn't receives ${skipped.get()} rows for '$executionId' execution" }
            }
        }
    }

    private class GrpcLoadListener(
        private val observer: StreamObserver<QueryReport>,
        private val report: QueryReport.Builder,
        private val event: Event,
        private val onEvent: (Event) -> Unit,
    ) : ResultListener {
        private val counter = AtomicLong()

        override fun onRow(sourceId: DataSourceId, row: TableRow) {
            check(report.executionId == row.executionId) {
                "'Execution id' isn't equal to '${report.executionId}', row: $row"
            }
            counter.incrementAndGet()
        }

        override fun onError(error: Throwable) {
            observer.onError(error)
            event.endTimestamp()
                .exception(error, true)
                .also(onEvent)
        }

        override fun onComplete() {
            event.endTimestamp()
            report.setRowsReceived(counter.get())
                .setEnd(event.endTimestamp.toTimestamp())
            observer.onNext(report.build())
            observer.onCompleted()
            onEvent(event)
        }
    }

    private fun ExecuteQueryRequest.toBody(executionId: Long) = ExecuteBodyData(
        executionId,
        getSourceCfg(sourceId),
        before.map(getQueryCfg),
        getQueryCfg(queryId),
        after.map(getQueryCfg),
        this
    )

    private object DummyUpdateListener : UpdateListener {
        override fun onUpdate(dataSourceId: DataSourceId, row: TableRow, properties: Map<String, String>) = Unit

        override fun onError(dataSourceId: DataSourceId, reason: Throwable) = Unit

        override fun onComplete(dataSourceId: DataSourceId) = Unit
    }

    companion object {
        private val LOGGER = KotlinLogging.logger { }

        private val EXECUTION_COUNTER = AtomicLong(
            Instant.now().run { epochSecond * TimeUnit.SECONDS.toNanos(1) + nano }
        )

        private fun QueryResponse.Builder.putRows(tableRow: TableRow) = apply {
            tableRow.columns.forEach { (key, value) ->
                // null values should be skipped because they aren't supported by Protobuf
                if (value != null) {
                    putRow(key, value.toStringValue())
                }
            }
        }
    }
}

fun interface OnEvent {
    fun accept(event: Event, parentEventID: EventID?)
}