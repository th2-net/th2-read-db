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

package com.exactpro.th2.read.db.core

import com.exactpro.th2.read.db.app.ResetState
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
        initQueryId: QueryId?,
        initParameters: QueryParametersValues,
        useColumns: Set<String>,
        updateQueryId: QueryId,
        updateParameters: QueryParametersValues,
        updateListener: UpdateListener,
        messageLoader: MessageLoader,
        interval: Duration,
    ) : TaskId

    suspend fun cancelTask(id: TaskId)

    companion object {
        internal const val TH2_PULL_TASK_UPDATE_HASH_PROPERTY = "th2.pull_task.update_hash"

        /**
         * Calculates the nearest reset date before or equal [now]
         * @return null when:<br>
         *   * [ResetState.afterDate] and [ResetState.afterTime] are null
         *   * [ResetState.afterDate] is after [now] and [ResetState.afterTime] is null
         *
         * otherwise [Instant]
         */
        internal fun ResetState.calculateNearestResetDate(now: Instant = Instant.now()): Instant? {
            val horizonByAfterDate: Instant = afterDate?.let {
                if (it.isAfter(now)) {
                    null
                } else {
                    it
                }
            } ?: Instant.MIN
            val horizonByAfterTime: Instant = afterTime?.let {
                val currentRestDate = now.withTime(it)
                if (currentRestDate.isAfter(now)) {
                    currentRestDate.minus(1L, ChronoUnit.DAYS)
                } else {
                    currentRestDate
                }

            } ?: Instant.MIN

            val result: Instant = maxOf(horizonByAfterDate, horizonByAfterTime)

            return if (result == Instant.MIN) null else result
        }

        private fun Instant.withTime(time: LocalTime): Instant = atZone(ZoneOffset.UTC)
            .withHour(time.hour)
            .withMinute(time.minute)
            .withSecond(time.second)
            .withNano(time.nano)
            .toInstant()
    }
}