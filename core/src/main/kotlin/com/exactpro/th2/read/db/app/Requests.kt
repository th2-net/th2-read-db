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

import com.exactpro.th2.read.db.core.DataSourceId
import com.exactpro.th2.read.db.core.QueryId
import com.exactpro.th2.read.db.core.QueryParametersValues
import com.fasterxml.jackson.annotation.JsonInclude
import java.time.Duration

@JsonInclude(JsonInclude.Include.NON_EMPTY)
class ExecuteQueryRequest(
    val sourceId: DataSourceId,
    val before: List<QueryId> = emptyList(),
    val queryId: QueryId,
    val after: List<QueryId> = emptyList(),
    val parameters: QueryParametersValues = emptyMap(),
)

class PullTableRequest(
    val dataSourceId: DataSourceId,
    val startFromLastReadRow: Boolean,
    val resetStateParameters: ResetState,
    val beforeInitQueryIds: List<QueryId>,
    val initQueryId: QueryId?,
    val initParameters: QueryParametersValues,
    val afterInitQueryIds: List<QueryId>,
    val useColumns: Set<String>,
    val beforeUpdateQueryIds: List<QueryId>,
    val updateQueryId: QueryId,
    val updateParameters: QueryParametersValues,
    val afterUpdateQueryIds: List<QueryId>,
    val interval: Duration,
)