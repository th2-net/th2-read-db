/*
 * Copyright 2024 Exactpro (Exactpro Systems Limited)
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

import com.exactpro.th2.read.db.core.impl.DefaultValueTransformer.Companion.INSTANCE
import java.net.URI
import java.util.ServiceLoader.load

class ValueTransformProvider private constructor(
    private val transforms: Map<DataSourceId, (Any) -> String>
) {
    fun transformer(dataSourceId: DataSourceId): (Any?) -> String? {
        val func: (Any) -> String = transforms[dataSourceId] ?: DEFAULT_TRANSFORM
        return { source: Any? -> source?.let(func) }
    }

    companion object {
        val DEFAULT_TRANSFORM: (Any) -> String = INSTANCE::transform.transformToString()

        fun create(dataSources: Map<DataSourceId, DataSourceConfiguration>): ValueTransformProvider {
            val dbTypeToFactory = load(ValueTransformerFactory::class.java).associateBy(ValueTransformerFactory::dbType)
            return ValueTransformProvider(
                dataSources.asSequence().associate { (sourceId, config) ->
                    sourceId to findTransform(config.url, dbTypeToFactory)
                }
            )
        }

        private infix fun ((Any) -> Any).orTransform(after: (Any) -> Any): (Any) -> Any {
            return { source: Any ->
                when (val result = this(source)) {
                    is String -> result
                    else -> after(result)
                }
            }
        }

        private fun ((Any) -> Any).transformToString(): (Any) -> String {
            return { source: Any ->
                when (val result = this(source)) {
                    is String -> result
                    else -> error("Value '$result' of type ${result::class.java} type can't be converted to String")
                }
            }
        }

        private fun findTransform(url: String, dbTypeToFactory: Map<String, ValueTransformerFactory>): (Any) -> String {
            val dbType = URI.create(url).schemeSpecificPart.split(":").first()
            return dbTypeToFactory[dbType]?.let {
                (INSTANCE::transform orTransform it::transform).transformToString()
            } ?: DEFAULT_TRANSFORM
        }
    }
}

operator fun ValueTransformProvider.get(dataSourceId: DataSourceId): (Any?) -> String? = transformer(dataSourceId)