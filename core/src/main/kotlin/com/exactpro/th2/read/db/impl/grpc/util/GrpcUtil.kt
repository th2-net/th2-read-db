/*
 * Copyright 2022 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.th2.read.db.impl.grpc.util

import com.exactpro.th2.common.util.toInstant
import com.exactpro.th2.read.db.app.ResetState
import com.exactpro.th2.read.db.core.DataSourceId
import com.exactpro.th2.read.db.core.QueryParametersValues
import com.exactpro.th2.read.db.core.TaskId
import com.exactpro.th2.read.db.grpc.DbPullResetState
import com.exactpro.th2.read.db.grpc.PullTaskId
import com.exactpro.th2.read.db.grpc.QueryId
import com.exactpro.th2.read.db.grpc.QueryParameters
import com.exactpro.th2.read.db.grpc.SourceId
import com.google.protobuf.util.Timestamps
import java.time.Instant
import java.time.LocalTime
import java.time.ZoneOffset
import java.time.temporal.ChronoField

fun SourceId.toModel(): DataSourceId = DataSourceId(id)

fun QueryId.toModel(): com.exactpro.th2.read.db.core.QueryId = com.exactpro.th2.read.db.core.QueryId(id)

fun QueryParameters.toModel(): QueryParametersValues = valuesMap.mapValues { listOf(it.value) }

fun PullTaskId.toModel(): TaskId = TaskId(id)

fun TaskId.toGrpc(): PullTaskId = PullTaskId.newBuilder().setId(id).build()

fun DbPullResetState.toModel(): ResetState {
    val afterDate = if (hasAfterDate()) afterDate.toInstant() else null
    val afterTime = if (hasAfterTime()) {
        val instant: Instant = afterTime.toInstant()
        check(
            instant.get(ChronoField.DAY_OF_MONTH) == 1
                    && instant.get(ChronoField.MONTH_OF_YEAR) == 1
                    && instant.get(ChronoField.YEAR) == 1970
        ) {
            "'afterTime' field contains date units, expected '1970-01-01 ...', actual '${Timestamps.toString(afterTime)}'"
        }
        LocalTime.ofInstant(instant, ZoneOffset.UTC)
    } else {
        null
    }
    return ResetState(afterDate, afterTime)
}