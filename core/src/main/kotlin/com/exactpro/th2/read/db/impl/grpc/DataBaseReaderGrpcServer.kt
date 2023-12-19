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

import com.exactpro.th2.common.message.toJavaDuration
import com.exactpro.th2.common.message.toJson
import com.exactpro.th2.read.db.app.DataBaseReader
import com.exactpro.th2.read.db.app.ExecuteQueryRequest
import com.exactpro.th2.read.db.app.PullTableRequest
import com.exactpro.th2.read.db.core.DataSourceId
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
import com.exactpro.th2.read.db.grpc.QueryId as ProtoQueryId

class DataBaseReaderGrpcServer(
    private val app: DataBaseReader,
) : ReadDbGrpc.ReadDbImplBase() {
    override fun execute(request: QueryRequest, responseObserver: StreamObserver<QueryResponse>) {
        LOGGER.info { "Executing 'execute' grpc request ${request.toJson()}" }
        try {
            val associatedMessageType: String? = if (request.hasAssociatedMessageType()) request.associatedMessageType.name else null
            app.executeQuery(
                request.run {
                    ExecuteQueryRequest(
                        sourceId.toModel(),
                        beforeQueryIdsList.map(ProtoQueryId::toModel),
                        queryId.toModel(),
                        afterQueryIdsList.map(ProtoQueryId::toModel),
                        parameters.toModel(),
                    )
                },
                GrpcResultListener(responseObserver),
            ) { row ->
                associatedMessageType?.let { row.copy(associatedMessageType = it) } ?: row
            }
        } catch (ex: Exception) {
            LOGGER.error(ex) { "cannot execute request ${request.toJson()}" }
            responseObserver.onError(Status.INTERNAL.withDescription(ex.message).asRuntimeException())
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

    private class GrpcResultListener(
        private val observer: StreamObserver<QueryResponse>,
    ) : ResultListener {
        override fun onRow(sourceId: DataSourceId, row: TableRow) {
            observer.onNext(
                QueryResponse.newBuilder()
                    .putAllRow(row.columns.mapValues { it.value.toString() })
                    .build()
            )
        }

        override fun onError(error: Throwable) {
            observer.onError(error)
        }

        override fun onComplete() {
            observer.onCompleted()
        }
    }

    private object DummyUpdateListener : UpdateListener {
        override fun onUpdate(dataSourceId: DataSourceId, row: TableRow, properties: Map<String, String>) = Unit

        override fun onError(dataSourceId: DataSourceId, reason: Throwable) = Unit

        override fun onComplete(dataSourceId: DataSourceId) = Unit
    }

    companion object {
        private val LOGGER = KotlinLogging.logger { }
    }
}