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

package com.exactpro.th2.read.db.core.impl

import com.exactpro.th2.read.db.app.ResetState
import com.exactpro.th2.read.db.core.DataSourceId
import com.exactpro.th2.read.db.core.DataBaseMonitorService
import com.exactpro.th2.read.db.core.DataBaseMonitorService.Companion.TH2_PULL_TASK_UPDATE_HASH_PROPERTY
import com.exactpro.th2.read.db.core.DataBaseMonitorService.Companion.calculateNearestResetDate
import com.exactpro.th2.read.db.core.DataBaseService
import com.exactpro.th2.read.db.core.HashService
import com.exactpro.th2.read.db.core.HashService.Companion.calculateHash
import com.exactpro.th2.read.db.core.MessageLoader
import com.exactpro.th2.read.db.core.QueryId
import com.exactpro.th2.read.db.core.QueryParametersValues
import com.exactpro.th2.read.db.core.TableRow
import com.exactpro.th2.read.db.core.TaskId
import com.exactpro.th2.read.db.core.UpdateListener
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.lastOrNull
import kotlinx.coroutines.flow.onCompletion
import kotlinx.coroutines.flow.onEach
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import mu.KotlinLogging
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

class DataBaseMonitorServiceImpl(
    private val dataBaseService: DataBaseService,
    private val hashService: HashService,
    private val clock: Clock,
) : DataBaseMonitorService {
    private val ids = AtomicInteger(1)
    private val runningTasks: MutableMap<TaskId, TaskHolder> = ConcurrentHashMap()
    private var running = true

    override fun CoroutineScope.submitTask(
        dataSourceId: DataSourceId,
        startFromLastReadRow: Boolean,
        resetStateParameters: ResetState,
        initQueryId: QueryId?,
        initParameters: QueryParametersValues,
        useColumns: Set<String>,
        updateQueryId: QueryId,
        updateParameters: QueryParametersValues,
        updateListener: UpdateListener,
        messageLoader: MessageLoader,
        interval: Duration,
    ): TaskId {
        val id = TaskId(ids.getAndIncrement().toString())
        LOGGER.info { "Submitting task $id with init query $initQueryId and update query $updateQueryId for data source $dataSourceId" }
        synchronized(runningTasks) {
            check(running) { "service is stopped" }
            check(id !in runningTasks) { "task with id $id already submitted" }
            val job = launch {
                try {
                    poolUpdates(
                        dataSourceId,
                        startFromLastReadRow,
                        resetStateParameters,
                        initQueryId,
                        initParameters,
                        useColumns,
                        updateParameters,
                        interval.toMillis(),
                        updateQueryId,
                        updateListener,
                        messageLoader
                    )
                } finally {
                    updateListener.onComplete(dataSourceId)
                }
            }
            runningTasks[id] = TaskHolder(job, updateListener)
        }
        LOGGER.info { "Task $id submitted" }
        return id
    }

    override suspend fun cancelTask(id: TaskId) {
        LOGGER.info { "Canceling task $id" }
        val holder: TaskHolder = synchronized(runningTasks) {
            check(running) { "service is stopped" }
            runningTasks.remove(id)
        } ?: error("unknown task id $id")

        LOGGER.debug { "Waiting for task completion" }
        holder.job.cancelAndJoin()

        LOGGER.info { "Task $id completed" }
    }

    private suspend fun poolUpdates(
        dataSourceId: DataSourceId,
        startFromLastReadRow: Boolean,
        resetStateParameters: ResetState,
        initQueryId: QueryId?,
        initParameters: QueryParametersValues,
        useColumns: Set<String>,
        updateParameters: QueryParametersValues,
        intervalMilliseconds: Long,
        updateQueryId: QueryId,
        updateListener: UpdateListener,
        messageLoader: MessageLoader
    ) {
        val properties = mapOf(
            TH2_PULL_TASK_UPDATE_HASH_PROPERTY to hashService.calculateHash(dataSourceId, updateQueryId).toString()
        )
        val finalParameters: MutableMap<String, Collection<String>> = hashMapOf()
        var lastResetTime = Instant.MIN
        var firstIteration = true

        fun extractParameters(lastRow: TableRow): QueryParametersValues {
            return useColumns.associateWith {
                val value = lastRow.columns[it] ?: error("Missing required parameter $it from init query result $lastRow")
                listOf(value.toString())
            }
        }

        do {
            val now = clock.instant()
            val resetDate = resetStateParameters.calculateNearestResetDate(now)
            if (firstIteration || (resetDate != null && lastResetTime < resetDate)) {
                LOGGER.info { "Initialize update query parameters, is first: $firstIteration, nearest reset: $resetDate, last reset: $lastResetTime" }
                firstIteration = false
                lastResetTime = now

                finalParameters.apply {
                    clear()
                    putAll(updateParameters)
                }

                val lastRow: TableRow? = when(startFromLastReadRow) {
                    true -> messageLoader.load(dataSourceId, resetDate, properties)
                    else -> null
                } ?: initQueryId?.let { queryId ->
                    dataBaseService.executeQuery(
                        dataSourceId,
                        queryId,
                        initParameters,
                    ).lastOrNull()
                }

                if (lastRow == null) {
                    finalParameters.putAll(initParameters)
                } else {
                    finalParameters.putAll(extractParameters(lastRow))
                }
            }

            try {
                dataBaseService.executeQuery(
                    dataSourceId,
                    updateQueryId,
                    finalParameters,
                ).onEach {
                    updateListener.onUpdate(dataSourceId, it, properties)
                }.onCompletion { reason ->
                    reason?.also { updateListener.onError(dataSourceId, it) }
                }.lastOrNull()?.also {
                    finalParameters.putAll(extractParameters(it))
                }
            } catch (ex: Exception) {
                updateListener.onError(dataSourceId, ex)
            }

            delay(intervalMilliseconds)
        } while (currentCoroutineContext().isActive)
    }

    companion object {
        private val LOGGER = KotlinLogging.logger { }
    }

    override fun close() {
        synchronized(runningTasks) {
            running = false
            runningTasks.forEach { (id, task) ->
                LOGGER.info { "Canceling task $id" }
                task.job.cancel()
            }
            runningTasks.clear()
        }
    }
}

private class TaskHolder(val job: Job, @Suppress("unused") val updateListener: UpdateListener)
