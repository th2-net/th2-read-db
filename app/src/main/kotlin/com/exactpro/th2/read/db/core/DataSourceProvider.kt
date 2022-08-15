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

package com.exactpro.th2.read.db.core

import javax.sql.DataSource

interface DataSourceProvider {
    /**
     * Returns [DataSource] for specified [dataSourceId] or throws an exception
     */
    fun dataSource(dataSourceId: DataSourceId): DataSource
}

operator fun DataSourceProvider.get(dataSourceId: DataSourceId): DataSource = dataSource(dataSourceId)
