/*
 * Copyright 2023 Exactpro (Exactpro Systems Limited)
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
import com.exactpro.th2.read.db.core.HashService
import com.exactpro.th2.read.db.core.QueryId
import com.exactpro.th2.read.db.core.QueryProvider
import com.exactpro.th2.read.db.core.get
import org.apache.commons.lang3.builder.HashCodeBuilder

class BaseHashServiceImpl(
    private val dataSourceProvider: DataSourceProvider,
    private val queriesProvider: QueryProvider,
) : HashService {
    override fun dataSourceHash(dataSourceId: DataSourceId): Int = dataSourceProvider.dataSource(dataSourceId).cfg.hashCode()
    override fun queryHash(queryId: QueryId): Int = queriesProvider[queryId].let { holder ->
        HashCodeBuilder()
            .append(holder.query)
            .append(holder.defaultParameters)
            .toHashCode()
    }
}