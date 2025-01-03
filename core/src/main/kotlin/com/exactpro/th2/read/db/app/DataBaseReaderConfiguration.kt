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

package com.exactpro.th2.read.db.app

import com.exactpro.th2.read.db.core.DataSourceConfiguration
import com.exactpro.th2.read.db.core.DataSourceId
import com.exactpro.th2.read.db.core.QueryConfiguration
import com.exactpro.th2.read.db.core.QueryId
import com.exactpro.th2.read.db.core.QueryParametersValues
import com.fasterxml.jackson.annotation.JsonTypeInfo
import com.fasterxml.jackson.annotation.JsonTypeName
import java.time.Instant
import java.time.LocalTime

class DataBaseReaderConfiguration(
    val dataSources: Map<DataSourceId, DataSourceConfiguration>,
    val queries: Map<QueryId, QueryConfiguration>,
    val startupTasks: List<StartupTaskConfiguration> = emptyList(),
    val publication: PublicationConfiguration = PublicationConfiguration(),
    val eventPublication: EventPublicationConfiguration = EventPublicationConfiguration(),
    val defaultQueryFetchSize: Int = 0,
    val useTransport: Boolean = false
)

class PublicationConfiguration(
    val queueSize: Int = 1000,
    val maxDelayMillis: Long = 1000,
    val maxBatchSize: Int = 100,
)

class EventPublicationConfiguration(
    val maxBatchSizeInItems: Int = 100,
    val maxFlushTime: Long = 1000,
)

@JsonTypeInfo(
    use = JsonTypeInfo.Id.NAME,
    include = JsonTypeInfo.As.PROPERTY,
    property = "type",
    visible = false,
)
sealed interface StartupTaskConfiguration

@JsonTypeName("read")
data class ReadTaskConfiguration(
    val dataSource: DataSourceId,
    val queryId: QueryId,
    val parameters: QueryParametersValues = emptyMap(),
) : StartupTaskConfiguration

@JsonTypeName("pull")
data class PullTaskConfiguration(
    val dataSource: DataSourceId,
    val startFromLastReadRow: Boolean = false,
    val resetStateParameters: ResetState = ResetState(),
    val beforeInitQueryIds: List<QueryId> = emptyList(),
    val initQueryId: QueryId?,
    val initParameters: QueryParametersValues = emptyMap(),
    val afterInitQueryIds: List<QueryId> = emptyList(),
    val beforeUpdateQueryIds: List<QueryId> = emptyList(),
    val updateQueryId: QueryId,
    val updateParameters: QueryParametersValues = emptyMap(),
    val afterUpdateQueryIds: List<QueryId> = emptyList(),
    val useColumns: Set<String> = emptySet(),
    val interval: Long,
) : StartupTaskConfiguration

data class ResetState(
    val afterDate: Instant? = null,
    val afterTime: LocalTime? = null,
)

fun DataBaseReaderConfiguration.validate(): List<String> {
    val sourceIDs = dataSources.keys
    val queryIDs = queries.keys
    return startupTasks.flatMapIndexed { index, task ->
        when (task) {
            is PullTaskConfiguration -> task.validate(index, sourceIDs, queryIDs)
            is ReadTaskConfiguration -> task.validate(index, sourceIDs, queryIDs)
        }
    }
}

private fun PullTaskConfiguration.validate(index: Int, sourceIDs: Set<DataSourceId>, queryIDs: Set<QueryId>): Collection<String> {
    return buildList {
        if (dataSource !in sourceIDs) {
            add("Unknown $dataSource in pull task at index $index. Known sources: $sourceIDs")
        }

        if (initQueryId != null && initQueryId !in queryIDs) {
            add("Unknown init query $initQueryId in pull task at index $index. Known queries: $queryIDs")
        }

        if (updateQueryId !in queryIDs) {
            add("Unknown update query $initQueryId in pull task at index $index. Known queries: $queryIDs")
        }
    }
}

private fun ReadTaskConfiguration.validate(index: Int, sourceIDs: Set<DataSourceId>, queryIDs: Set<QueryId>): Collection<String> {
    return buildList {
        if (dataSource !in sourceIDs) {
            add("Unknown $dataSource in read task at index $index. Known sources: $sourceIDs")
        }

        if (queryId !in queryIDs) {
            add("Unknown query $queryId in read task at index $index. Known queries: $queryIDs")
        }
    }
}