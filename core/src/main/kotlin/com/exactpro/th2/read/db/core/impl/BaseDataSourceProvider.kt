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

import com.exactpro.th2.read.db.core.DataSourceConfiguration
import com.exactpro.th2.read.db.core.DataSourceId
import com.exactpro.th2.read.db.core.DataSourceProvider
import mu.KotlinLogging
import org.apache.commons.dbcp2.BasicDataSource
import javax.sql.DataSource

class BaseDataSourceProvider(
    configurations: Map<DataSourceId, DataSourceConfiguration>,
) : DataSourceProvider {
    private val sourcesById: Map<DataSourceId, DataSource> = configurations.mapValues { (id, cfg) ->
        LOGGER.trace { "Creating data source for $id" }
        BasicDataSource().apply {
            url = cfg.url
            cfg.username?.also { username = it }
            cfg.password?.also { password = it }
            cfg.properties.forEach { (key, value) -> addConnectionProperty(key, value) }
            LOGGER.trace { "Data source for $id created" }
        }
    }

    override fun dataSource(dataSourceId: DataSourceId): DataSource {
        return sourcesById[dataSourceId] ?: error("cannot find data source $dataSourceId. Known: ${sourcesById.keys}")
    }

    companion object {
        private val LOGGER = KotlinLogging.logger { }
    }
}