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

import com.exactpro.th2.read.db.core.impl.DefaultValueTransform
import java.net.URI
import java.sql.Connection
import java.util.ServiceLoader.load

/**
 * This method transforms an instance of JDBC class to general th2 string format.
 * Some vendor's JDBC classes use connection / session parameters (for example time zone of db or session)
 *  for transformation their instances to general java classes.
 */
typealias ToNullableStringTransformer = (Any?, Connection) -> String?
private typealias ToStringTransformer = (Any, Connection) -> String

class ValueTransformProvider private constructor(
    private val transforms: Map<DataSourceId, ToStringTransformer>
) {
    fun transformer(dataSourceId: DataSourceId): ToNullableStringTransformer {
        val func: ToStringTransformer = transforms[dataSourceId] ?: DEFAULT_TRANSFORM
        return { source, connection -> source?.let { func(it, connection) } }
    }

    companion object {
        @JvmField
        val DEFAULT_TRANSFORM: ToStringTransformer = DefaultValueTransform.transformToString()

        fun create(dataSources: Map<DataSourceId, DataSourceConfiguration>): ValueTransformProvider {
            val dbVendorToFactory = load(ValueTransformerFactory::class.java)
                .associateBy(ValueTransformerFactory::dbVendor)
            return ValueTransformProvider(
                dataSources.asSequence().associate { (sourceId, config) ->
                    sourceId to findTransform(config.url, dbVendorToFactory)
                }
            )
        }

        private infix fun (ValueTransform).orTransform(after: ValueTransform): ValueTransform {
            return { source, connection ->
                when (val result = this(source, connection)) {
                    is String -> result
                    else -> after(result, connection)
                }
            }
        }

        private fun (ValueTransform).transformToString(): ToStringTransformer {
            return { source, connection ->
                when (val result = this(source, connection)) {
                    is String -> result
                    else -> error("Value '$result' of type ${result::class.java} type can't be converted to String")
                }
            }
        }

        private fun findTransform(url: String, dbTypeToFactory: Map<String, ValueTransformerFactory>): ToStringTransformer {
            val dbType = URI.create(url).schemeSpecificPart.split(":").first()
            return dbTypeToFactory[dbType]?.let {
                (DefaultValueTransform orTransform it.transformer).transformToString()
            } ?: DEFAULT_TRANSFORM
        }
    }
}

operator fun ValueTransformProvider.get(dataSourceId: DataSourceId): ToNullableStringTransformer = transformer(dataSourceId)