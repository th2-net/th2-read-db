/*
 * Copyright 2023-2024 Exactpro (Exactpro Systems Limited)
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
import com.exactpro.th2.read.db.core.QueryConfiguration
import com.exactpro.th2.read.db.core.QueryId
import com.exactpro.th2.read.db.core.HashService.Companion.calculateHash
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

internal class BaseHashServiceTest {
    @Test
    fun calculateHashTest() {
        val dataSourceId = DataSourceId("test-data-source-id")
        val queryId = QueryId("test-query-id")
        val dataSourceProvider = BaseDataSourceProvider(
            mapOf(dataSourceId to DataSourceConfiguration(
                "jdbc:mysql://localhost:1234/test_data",
                "test-username",
                "test-password",
                mapOf("test-property" to "test-property-value")
            ))
        )
        val queryProvider = BaseQueryProvider(
            mapOf(queryId to QueryConfiguration(
                "test-query",
                mapOf("test-query-parameter" to listOf("test-query-parameter-value-a", "test-query-parameter-value-b"))
            )), 0
        )
        val hashService = BaseHashServiceImpl(
            dataSourceProvider,
            queryProvider
        )

        assertEquals(-1879617647, hashService.calculateHash(dataSourceId, queryId))
    }
}