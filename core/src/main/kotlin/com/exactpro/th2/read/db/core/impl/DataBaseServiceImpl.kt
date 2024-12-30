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

package com.exactpro.th2.read.db.core.impl

import com.exactpro.th2.read.db.core.DataBaseService
import com.exactpro.th2.read.db.core.DataSourceId
import com.exactpro.th2.read.db.core.DataSourceProvider
import com.exactpro.th2.read.db.core.QueryHolder
import com.exactpro.th2.read.db.core.QueryId
import com.exactpro.th2.read.db.core.QueryParametersValues
import com.exactpro.th2.read.db.core.QueryProvider
import com.exactpro.th2.read.db.core.TableRow
import com.exactpro.th2.read.db.core.ToNullableStringTransformer
import com.exactpro.th2.read.db.core.ValueTransformProvider
import com.exactpro.th2.read.db.core.exception.QueryExecutionException
import com.exactpro.th2.read.db.core.get
import com.exactpro.th2.read.db.core.util.getColumnValue
import com.exactpro.th2.read.db.core.util.runCatchingException
import com.exactpro.th2.read.db.core.util.set
import com.exactpro.th2.read.db.core.util.setCollection
import io.github.oshai.kotlinlogging.KotlinLogging
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.onCompletion
import java.sql.Connection
import java.sql.ResultSet
import javax.sql.DataSource

class DataBaseServiceImpl(
    private val dataSourceProvider: DataSourceProvider,
    private val queriesProvider: QueryProvider,
    private val transformProvider: ValueTransformProvider,
) : DataBaseService {
    override fun executeQuery(
        dataSourceId: DataSourceId,
        before: List<QueryId>,
        queryId: QueryId,
        after: List<QueryId>,
        parameters: QueryParametersValues
    ): Flow<TableRow> {
        LOGGER.info { "Executing query $queryId for $dataSourceId connection with $parameters parameters without default" }
        val dataSource: DataSource = dataSourceProvider[dataSourceId].dataSource
        val transformValue: ToNullableStringTransformer = transformProvider[dataSourceId]
        val queryHolder: QueryHolder = queriesProvider[queryId]
        val beforeQueryHolders: List<QueryHolder> = before.map(queriesProvider::get)
        val afterQueryHolders: List<QueryHolder> = after.map(queriesProvider::get)
        val connection: Connection = dataSource.connection
        val finalParameters = queryHolder.defaultParameters.toMutableMap().apply {
            putAll(parameters)
        }
        LOGGER.trace { "Final execution parameters: $finalParameters" }
        val resultSet: ResultSet = try {
            beforeQueryHolders.forEach { holder ->
                runCatchingException {
                    execute(connection, holder, finalParameters)
                }.getOrElse {
                    throw QueryExecutionException(
                        "cannot execute ${holder.query} before query for $dataSourceId connection", it
                    )
                }
            }

            execute(connection, queryHolder, finalParameters)
        } catch (ex: Exception) {
            connection.close()
            throw QueryExecutionException("cannot execute $queryId query for $dataSourceId connection", ex)
        }

        val associatedMessageType: String? = queryHolder.associatedMessageType
        return flow {
            var columns: Collection<String>? = null
            while (resultSet.next()) {
                if (columns == null) {
                    columns = resultSet.extractColumns()
                }
                emit(resultSet.transform(columns, associatedMessageType, transformValue, connection))
            }
        }.onCompletion { reason ->
            try {
                reason?.also { LOGGER.warn(it) { "query $queryId completed with exception for $dataSourceId source" } }

                afterQueryHolders.forEach { holder ->
                    runCatchingException {
                        execute(connection, holder, finalParameters)
                    }.getOrElse {
                        throw QueryExecutionException(
                            "cannot execute ${holder.query} after query for $dataSourceId connection", it
                        )
                    }
                }
                LOGGER.info { "Query $queryId for $dataSourceId connection was executed" }
            } finally {
                LOGGER.trace { "Closing connection to $dataSourceId" }
                runCatchingException { connection.close() }.onFailure { LOGGER.error(it) { "cannot close connection for $dataSourceId" } }
            }
        }
    }

    private fun execute(
        connection: Connection,
        holder: QueryHolder,
        parameters: Map<String, Collection<String>>
    ): ResultSet = connection.prepareStatement(holder.query).run {
        holder.parameters.forEach { (name, typeInfoList) ->
            typeInfoList.forEach { typeInfo ->
                val values = parameters[name]
                when {
                    values == null -> set(typeInfo.index, null, typeInfo.type)
                    values.size == 1 -> set(typeInfo.index, values.first(), typeInfo.type)
                    else -> setCollection(
                        typeInfo.index,
                        connection.createArrayOf(typeInfo.type.name, values.toTypedArray())
                    )
                }
            }
        }
        fetchSize = holder.fetchSize
        LOGGER.trace { "Execute query: $holder" }
        executeQuery()
    }

    companion object {
        private val LOGGER = KotlinLogging.logger { }
    }

}

private fun ResultSet.extractColumns(): List<String> = metaData.run {
    (1..columnCount).map { getColumnLabel(it) }
}

private fun ResultSet.transform(
    columns: Collection<String>,
    associatedMessageType: String?,
    transform: ToNullableStringTransformer,
    connection: Connection,
): TableRow = TableRow(
    columns.associateWith { transform(getColumnValue(it), connection) },
    associatedMessageType
)
