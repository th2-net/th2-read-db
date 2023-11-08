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
import java.time.Duration

class ExecuteQueryRequest(
    val sourceId: DataSourceId,
    val queryId: QueryId,
    val parameters: QueryParametersValues,
)

class PullTableRequest(
    val dataSourceId: DataSourceId,
    val loadPreviousState: Boolean,
    val initQueryId: QueryId?,
    val initParameters: QueryParametersValues,
    val useColumns: Set<String>,
    val updateQueryId: QueryId,
    val updateParameters: QueryParametersValues,
    val interval: Duration,
)