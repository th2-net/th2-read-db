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

import com.exactpro.th2.read.db.core.ParameterInfo
import com.exactpro.th2.read.db.core.QueryConfiguration
import com.exactpro.th2.read.db.core.QueryHolder
import com.exactpro.th2.read.db.core.QueryId
import com.exactpro.th2.read.db.core.QueryProvider
import mu.KotlinLogging
import org.apache.commons.text.StringSubstitutor
import org.apache.commons.text.lookup.StringLookup
import java.sql.JDBCType
import java.sql.SQLType

class BaseQueryProvider(
    queriesConfiguration: Map<QueryId, QueryConfiguration>
) : QueryProvider {
    private val queryById: Map<QueryId, QueryHolder> = queriesConfiguration.mapValues { (id, cfg) ->
        LOGGER.trace { "Creating holder for $id query" }
        val lookup = ParameterLookup()
        val processedQuery = StringSubstitutor(lookup).replace(cfg.query)
        val parameters = lookup.parameters
        cfg.defaultParameters.keys.forEach {
            if (it !in parameters) {
                LOGGER.warn { "Default parameter $it was not found in query. Known parameters: ${parameters.keys}" }
            }
        }
        QueryHolder(processedQuery, parameters, cfg.defaultParameters).also {
            LOGGER.trace { "Holder for $id query created: $it" }
        }
    }

    override fun query(queryId: QueryId): QueryHolder {
        return queryById[queryId] ?: error("cannot find query with id $queryId. Known queries: ${queryById.keys}")
    }

    private class ParameterLookup : StringLookup {
        private val _parameters: MutableMap<String, MutableList<ParameterInfo>> = HashMap()
        private var index: Int = 1
        val parameters: Map<String, List<ParameterInfo>>
            get() = _parameters

        override fun lookup(key: String): String {
            val result = key.split(TYPE_DELIMITER)
            val (name, sqlType) = when (result.size) {
                1 -> result.first() to DEFAULT_TYPE
                2 -> result.first() to sqlType(result.last())
                else -> error("unsupported format for parameter $key")
            }
            _parameters.computeIfAbsent(name) { arrayListOf() }.add(ParameterInfo(index++, sqlType))
            return PARAMETER_HOLDER
        }

    }

    companion object {
        private val LOGGER = KotlinLogging.logger { }
        private const val PARAMETER_HOLDER = "?"
        private const val TYPE_DELIMITER = ':'
        private val DEFAULT_TYPE: SQLType = JDBCType.VARCHAR

        @JvmStatic
        private fun sqlType(type: String): SQLType =
            JDBCType.values().find { it.name.equals(type, ignoreCase = true) }
                ?: error("unknown jdbc type $type. Known: ${JDBCType.values().joinToString { it.name.lowercase() }}")
    }
}