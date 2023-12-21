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

package com.exactpro.th2.read.db.impl.grpc

import com.exactpro.th2.common.event.Event
import com.exactpro.th2.common.message.toJavaDuration
import com.exactpro.th2.common.message.toJson
import com.exactpro.th2.read.db.app.DataBaseReader
import com.exactpro.th2.read.db.app.ExecuteQueryRequest
import com.exactpro.th2.read.db.app.PullTableRequest
import com.exactpro.th2.read.db.bootstrap.toMap
import com.exactpro.th2.read.db.core.DataSourceConfiguration
import com.exactpro.th2.read.db.core.DataSourceId
import com.exactpro.th2.read.db.core.QueryConfiguration
import com.exactpro.th2.read.db.core.QueryId
import com.exactpro.th2.read.db.core.ResultListener
import com.exactpro.th2.read.db.core.TableRow
import com.exactpro.th2.read.db.core.UpdateListener
import com.exactpro.th2.read.db.grpc.DbPullRequest
import com.exactpro.th2.read.db.grpc.DbPullResponse
import com.exactpro.th2.read.db.grpc.QueryRequest
import com.exactpro.th2.read.db.grpc.QueryResponse
import com.exactpro.th2.read.db.grpc.ReadDbGrpc
import com.exactpro.th2.read.db.grpc.StopPullingRequest
import com.exactpro.th2.read.db.impl.grpc.util.toGrpc
import com.exactpro.th2.read.db.impl.grpc.util.toModel
import com.google.protobuf.Empty
import io.grpc.Status
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
        val executionId = EXECUTION_COUNTER.incrementAndGet()
        LOGGER.info { "Executing 'execute' grpc request ${request.toJson()}, execution id: $executionId" }
        val event = Event.start()
            .name("Execute '${request.queryId.id}' query ($executionId)")
            .type("read-db.execute")
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
            app.executeQuery(executeQueryRequest, GrpcResultListener(responseObserver, event)) { row ->
                row.copy(associatedMessageType = associatedMessageType, executionId = executionId)
            }
        } catch (ex: Exception) {
            LOGGER.error(ex) { "cannot execute request ${request.toJson()}" }
            responseObserver.onError(Status.INTERNAL.withDescription(ex.message).asRuntimeException())
            event.exception(ex, true).also(onEvent::accept)
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

    inner class GrpcResultListener(
        private val observer: StreamObserver<QueryResponse>,
        private val event: Event,
    ) : ResultListener {
        override fun onRow(sourceId: DataSourceId, row: TableRow) {
            requireNotNull(row.executionId) {
                "'Execution id' is null for row: $row"
            }
            observer.onNext(
                QueryResponse.newBuilder()
                    .putAllRow(row.toMap())
                    .setExecutionId(row.executionId)
                    .build()
            )
        }

        override fun onError(error: Throwable) {
            observer.onError(error)
            event.endTimestamp()
                .exception(error, true)
                .also(onEvent::accept)
        }

        override fun onComplete() {
            observer.onCompleted()
            event.endTimestamp().also(onEvent::accept)
        }
    }

    private fun ExecuteQueryRequest.toBody(executionId: Long) = ExecuteBodyData(
        getSourceCfg(sourceId),
        before.map(getQueryCfg),
        getQueryCfg(queryId),
        after.map(getQueryCfg),
        parameters,
        executionId
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
    }
}

fun interface OnEvent {
    fun accept(event: Event)
}