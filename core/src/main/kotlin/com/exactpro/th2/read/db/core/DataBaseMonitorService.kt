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

package com.exactpro.th2.read.db.core

import com.exactpro.th2.read.db.app.ResetState
import io.github.oshai.kotlinlogging.KotlinLogging
import kotlinx.coroutines.CoroutineScope
import java.time.Duration
import java.time.Instant
import java.time.LocalTime
import java.time.ZoneOffset
import java.time.temporal.ChronoUnit

interface DataBaseMonitorService : AutoCloseable {
    fun CoroutineScope.submitTask(
        dataSourceId: DataSourceId,
        startFromLastReadRow: Boolean,
        resetStateParameters: ResetState,
        beforeInitQueryIds: List<QueryId>,
        initQueryId: QueryId?,
        initParameters: QueryParametersValues,
        afterInitQueryIds: List<QueryId>,
        useColumns: Set<String>,
        beforeUpdateQueryIds: List<QueryId>,
        updateQueryId: QueryId,
        updateParameters: QueryParametersValues,
        afterUpdateQueryIds: List<QueryId>,
        updateListener: UpdateListener,
        messageLoader: MessageLoader,
        interval: Duration,
    ) : TaskId

    suspend fun cancelTask(id: TaskId)

    companion object {
        private val LOGGER = KotlinLogging.logger { }
        internal const val TH2_PULL_TASK_UPDATE_HASH_PROPERTY = "th2.pull_task.update_hash"

        /**
         * Calculates the nearest reset date before or equal [current]. [current] time is excluded
         * @return null when:<br>
         *   * [ResetState.afterDate] and [ResetState.afterTime] are null
         *   * [ResetState.afterDate] is after [current] and [ResetState.afterTime] is null
         *
         * otherwise [Instant]
         */
        internal fun ResetState.calculateNearestResetDate(current: Instant): Instant? {
            val boundaryByAfterDate: Instant = afterDate?.let {
                if (it > current) {
                    null
                } else {
                    it
                }
            } ?: Instant.MIN
            val boundaryAfterTime: Instant = afterTime?.let {
                val currentRestDate = current.withTime(it)
                if (currentRestDate > current) {
                    currentRestDate.minus(1L, ChronoUnit.DAYS)
                } else {
                    currentRestDate
                }

            } ?: Instant.MIN

            val result: Instant = maxOf(boundaryByAfterDate, boundaryAfterTime)

            return (if (result == Instant.MIN) null else result).also {
                LOGGER.trace { "Calculated nearest reset date, result: $it, data boundary: $boundaryByAfterDate, time boundary: $boundaryAfterTime, after date: $afterDate, after time: $afterTime, now: $current" }
            }
        }

        private fun Instant.withTime(time: LocalTime): Instant = atZone(ZoneOffset.UTC)
            .withHour(time.hour)
            .withMinute(time.minute)
            .withSecond(time.second)
            .withNano(time.nano)
            .toInstant()
    }
}