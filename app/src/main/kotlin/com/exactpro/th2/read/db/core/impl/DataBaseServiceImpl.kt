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

package com.exactpro.th2.read.db.core.impl

import com.exactpro.th2.read.db.core.DataSourceId
import com.exactpro.th2.read.db.core.DataSourceProvider
import com.exactpro.th2.read.db.core.DataBaseService
import com.exactpro.th2.read.db.core.QueryHolder
import com.exactpro.th2.read.db.core.QueryId
import com.exactpro.th2.read.db.core.QueryParametersValues
import com.exactpro.th2.read.db.core.QueryProvider
import com.exactpro.th2.read.db.core.TableRow
import com.exactpro.th2.read.db.core.exception.QueryExecutionException
import com.exactpro.th2.read.db.core.get
import com.exactpro.th2.read.db.core.util.getColumnValue
import com.exactpro.th2.read.db.core.util.set
import com.exactpro.th2.read.db.core.util.setCollection
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.onCompletion
import mu.KotlinLogging
import java.sql.Date
import java.sql.ResultSet
import java.sql.Time
import java.sql.Timestamp
import javax.sql.DataSource

class DataBaseServiceImpl(
    private val dataSourceProvider: DataSourceProvider,
    private val queriesProvider: QueryProvider,
) : DataBaseService {
    override fun executeQuery(
        dataSourceId: DataSourceId,
        queryId: QueryId,
        parameters: QueryParametersValues,
    ): Flow<TableRow> {
        LOGGER.info { "Executing query $queryId for $dataSourceId connection" }
        val dataSource: DataSource = dataSourceProvider[dataSourceId]
        val queryHolder: QueryHolder = queriesProvider[queryId]
        val connection = dataSource.connection
        val finalParameters = queryHolder.defaultParameters.toMutableMap().apply {
            putAll(parameters)
        }
        LOGGER.trace { "Execution parameters: $finalParameters" }
        val resultSet: ResultSet = try {
            connection.prepareStatement(queryHolder.query).run {
                queryHolder.parameters.forEach { (name, typeInfos) ->
                    typeInfos.forEach { typeInfo ->
                        val values = finalParameters[name]
                        when {
                            values == null -> set(typeInfo.index, null, typeInfo.type)
                            values.size == 1 -> set(typeInfo.index, values.first(), typeInfo.type)
                            else -> setCollection(typeInfo.index, connection.createArrayOf(typeInfo.type.name, values.toTypedArray()))
                        }
                    }
                }
                executeQuery()
            }
        } catch (ex: Exception) {
            connection.close()
            throw QueryExecutionException("cannot execute $queryId query for $dataSourceId connection", ex)
        }
        return flow {
            var columns: Collection<String>? = null
            while (resultSet.next()) {
                if (columns == null) {
                    columns = resultSet.extractColumns()
                }
                emit(resultSet.transform(columns))
            }
        }.onCompletion { reason ->
            reason?.also { LOGGER.warn(it) { "query $queryId completed with exception for $dataSourceId source" } }
            LOGGER.trace { "Closing connection to $dataSourceId" }
            runCatching { connection.close() }.onFailure { LOGGER.error(it) { "cannot close connection for $dataSourceId" } }
        }
    }

    companion object {
        private val LOGGER = KotlinLogging.logger { }
    }
}

private fun ResultSet.extractColumns(): List<String> = metaData.run {
    (1..columnCount).map { getColumnLabel(it) }
}

private fun ResultSet.transform(columns: Collection<String>): TableRow = TableRow(columns.associateWith(this::getColumnValue.asRegularValues()))

private fun ((String) -> Any).asRegularValues(): (String) -> Any = {
    when (val value = this(it)) {
        is Date -> value.toLocalDate()
        is Time -> value.toLocalTime()
        is Timestamp -> value.toInstant()
        else -> value
    }
}
