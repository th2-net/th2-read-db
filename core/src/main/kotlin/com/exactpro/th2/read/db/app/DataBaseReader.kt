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

import com.exactpro.th2.read.db.core.DataBaseMonitorService
import com.exactpro.th2.read.db.core.DataBaseService
import com.exactpro.th2.read.db.core.DataSourceId
import com.exactpro.th2.read.db.core.DataSourceProvider
import com.exactpro.th2.read.db.core.HashService
import com.exactpro.th2.read.db.core.MessageLoader
import com.exactpro.th2.read.db.core.QueryProvider
import com.exactpro.th2.read.db.core.ResultListener
import com.exactpro.th2.read.db.core.RowListener
import com.exactpro.th2.read.db.core.TableRow
import com.exactpro.th2.read.db.core.TaskId
import com.exactpro.th2.read.db.core.UpdateListener
import com.exactpro.th2.read.db.core.impl.DataBaseMonitorServiceImpl
import com.exactpro.th2.read.db.core.impl.DataBaseServiceImpl
import com.exactpro.th2.read.db.core.impl.BaseDataSourceProvider
import com.exactpro.th2.read.db.core.impl.BaseHashServiceImpl
import com.exactpro.th2.read.db.core.impl.BaseQueryProvider
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.flow.onCompletion
import kotlinx.coroutines.launch
import mu.KotlinLogging
import java.time.Clock
import java.time.Duration

class DataBaseReader(
    private val dataBaseService: DataBaseService,
    private val monitorService: DataBaseMonitorService,
    private val startupTasks: List<StartupTaskConfiguration>,
    private val scope: CoroutineScope,
    private val pullingListener: UpdateListener,
    private val rowListener: RowListener,
    private val messageLoader: MessageLoader,
) : AutoCloseable {

    fun start() {
        LOGGER.info { "Start reader. Has ${startupTasks.size} startup task(s)" }
        startupTasks.forEach { task ->
            when (task) {
                is ReadTaskConfiguration -> scope.launch {
                    LOGGER.info { "Launching read task from ${task.dataSource} with ${task.queryId} query" }
                    dataBaseService.executeQuery(
                        task.dataSource,
                        emptyList(),
                        task.queryId,
                        emptyList(),
                        task.parameters,
                    ).collect { it.transferTo(task.dataSource, rowListener) }
                }

                is PullTaskConfiguration -> with(monitorService) {
                    LOGGER.info { "Launching pull task from ${task.dataSource} with ${task.initQueryId} init query and ${task.updateQueryId} update query" }
                    scope.submitTask(
                        task.dataSource,
                        task.startFromLastReadRow,
                        task.resetStateParameters,
                        task.beforeInitQueryIds,
                        task.initQueryId,
                        task.initParameters,
                        task.afterInitQueryIds,
                        task.useColumns,
                        task.beforeUpdateQueryIds,
                        task.updateQueryId,
                        task.updateParameters,
                        task.afterUpdateQueryIds,
                        pullingListener,
                        messageLoader,
                        Duration.ofMillis(task.interval),
                    )
                }
            }
        }
        LOGGER.info { "Reader started" }
    }

    fun executeQuery(
        request: ExecuteQueryRequest,
        listener: ResultListener,
        rowTransformer: (TableRow) -> TableRow = { it },
    ) {
        scope.launch {
            with(request) {
                dataBaseService.executeQuery(sourceId, emptyList(), queryId, emptyList(), parameters)
                    .onCompletion {
                        if (it == null) {
                            listener.onComplete()
                        } else {
                            listener.onError(it)
                        }
                    }.collect {
                        rowTransformer(it).transferTo(sourceId, listener, rowListener)
                    }
            }
        }
    }

    fun submitPullTask(
        request: PullTableRequest,
        updateListener: UpdateListener,
        rowTransformer: (TableRow) -> TableRow = { it },
    ): TaskId {
        return with(monitorService) {
            with(request) {
                scope.submitTask(
                    dataSourceId,
                    startFromLastReadRow,
                    resetStateParameters,
                    beforeInitQueryIds,
                    initQueryId,
                    initParameters,
                    afterInitQueryIds,
                    useColumns,
                    beforeUpdateQueryIds,
                    updateQueryId,
                    updateParameters,
                    afterUpdateQueryIds,
                    wrap(rowTransformer, updateListener, pullingListener),
                    messageLoader,
                    interval,
                )
            }
        }
    }

    fun stopPullTask(id: TaskId) {
        scope.launch {
            monitorService.cancelTask(id)
        }
    }

    override fun close() {
        LOGGER.info { "Closing reader" }
        monitorService.close()
        LOGGER.info { "Reader closed" }
    }

    private fun TableRow.transferTo(sourceId: DataSourceId, vararg listeners: RowListener) {
        listeners.forEach { listener ->
            runCatching { listener.onRow(sourceId, this) }.onFailure {
                LOGGER.error(it) { "error during row processing by listener ${listener::class}" }
            }
        }
    }

    private fun wrap(
        rowTransformer: (TableRow) -> TableRow,
        vararg listeners: UpdateListener
    ): AggregatedListener = AggregatedListener(listeners.toList(), rowTransformer)

    private class AggregatedListener(
        private val listeners: Collection<UpdateListener>,
        private val rowTransformer: (TableRow) -> TableRow,
    ) : UpdateListener {
        override fun onUpdate(dataSourceId: DataSourceId, row: TableRow, properties: Map<String, String>) {
            val updatedRow = rowTransformer(row)
            forEach { onUpdate(dataSourceId, updatedRow, properties) }
        }

        override fun onError(dataSourceId: DataSourceId, reason: Throwable) {
            forEach { onError(dataSourceId, reason) }
        }

        override fun onComplete(dataSourceId: DataSourceId) {
            forEach { onComplete(dataSourceId) }
        }

        private inline fun forEach(action: UpdateListener.() -> Unit) {
            listeners.forEach { listener ->
                listener.runCatching(action).onFailure {
                    LOGGER.error(it) { "cannot execute action for ${listener::class}" }
                }
            }
        }
    }

    companion object {
        private val LOGGER = KotlinLogging.logger { }

        @JvmStatic
        fun createDataBaseReader(
            configuration: DataBaseReaderConfiguration,
            scope: CoroutineScope,
            pullingListener: UpdateListener,
            rowListener: RowListener,
            messageLoader: MessageLoader,
            clock: Clock = Clock.systemDefaultZone()
        ): DataBaseReader {
            val sourceProvider: DataSourceProvider = BaseDataSourceProvider(configuration.dataSources)
            val queryProvider: QueryProvider = BaseQueryProvider(configuration.queries)
            val dataBaseService: DataBaseService = DataBaseServiceImpl(sourceProvider, queryProvider)
            val hashService: HashService = BaseHashServiceImpl(sourceProvider, queryProvider)
            val monitorService: DataBaseMonitorService = DataBaseMonitorServiceImpl(dataBaseService, hashService, clock)
            return DataBaseReader(
                dataBaseService,
                monitorService,
                configuration.startupTasks,
                scope,
                pullingListener,
                rowListener,
                messageLoader,
            )
        }
    }
}
