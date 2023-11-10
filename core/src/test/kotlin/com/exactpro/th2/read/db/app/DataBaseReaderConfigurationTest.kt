/*
 * Copyright 2022-2023 Exactpro (Exactpro Systems Limited)
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

import com.exactpro.th2.common.schema.configuration.ConfigurationManager
import com.exactpro.th2.common.schema.dictionary.DictionaryType
import com.exactpro.th2.common.schema.factory.AbstractCommonFactory
import com.exactpro.th2.common.schema.factory.FactorySettings
import com.exactpro.th2.read.db.core.DataSourceConfiguration
import com.exactpro.th2.read.db.core.DataSourceId
import com.exactpro.th2.read.db.core.QueryConfiguration
import com.exactpro.th2.read.db.core.QueryId
import com.fasterxml.jackson.databind.ObjectMapper
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import strikt.api.expectThat
import strikt.assertions.filterIsInstance
import strikt.assertions.hasEntry
import strikt.assertions.hasSize
import strikt.assertions.isEqualTo
import strikt.assertions.single
import java.io.InputStream
import java.nio.file.Path

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
internal class DataBaseReaderConfigurationTest {
    private val factory = object : AbstractCommonFactory(FactorySettings()) {
        fun mapper(): ObjectMapper = MAPPER

        override fun loadSingleDictionary(): InputStream = TODO("Not yet implemented")

        override fun getDictionaryAliases(): MutableSet<String> = TODO("Not yet implemented")

        override fun loadDictionary(alias: String?): InputStream = TODO("Not yet implemented")

        @Deprecated("Deprecated in Java")
        override fun readDictionary(): InputStream = TODO("Not yet implemented")

        @Deprecated("Deprecated in Java")
        override fun readDictionary(dictionaryType: DictionaryType?): InputStream = TODO("Not yet implemented")

        override fun getConfigurationManager(): ConfigurationManager = TODO("Not yet implemented")

        override fun getPathToCustomConfiguration(): Path = TODO("Not yet implemented")

        @Deprecated("Deprecated in Java")
        override fun getPathToDictionaryTypesDir(): Path = TODO("Not yet implemented")

        override fun getPathToDictionaryAliasesDir(): Path = TODO("Not yet implemented")

        @Deprecated("Deprecated in Java")
        override fun getOldPathToDictionariesDir(): Path = TODO("Not yet implemented")
    }

    @Test
    fun `deserializes config`() {
        val cfg = factory.mapper().readValue(
            """
            {
              "dataSources": {
                $TEST_DATA_SOURCE
              },
              "queries": {
                $TEST_QUERY
              },
              "startupTasks": [
                $READ_TASK,
                $PULL_TASK
              ]
            }
        """.trimIndent(), DataBaseReaderConfiguration::class.java)

        val dataSource = DataSourceId("test")
        val queryId = QueryId("test_query")
        expectThat(cfg) {
            get { dataSources }
                .hasSize(1)
                .hasEntry(
                    dataSource, DataSourceConfiguration(
                        "test.url",
                        "user",
                        "pwd",
                        mapOf(
                            "prop1" to "value1"
                        )
                    )
                )
            get { queries }.hasSize(1)
                .hasEntry(
                    queryId, QueryConfiguration(
                        "SELECT * FROM table;",
                        mapOf(
                            "param1" to listOf("value1")
                        )
                    )
                )
            get { startupTasks }.hasSize(2)
                .apply {
                    filterIsInstance<ReadTaskConfiguration>().single()
                        .isEqualTo(
                            ReadTaskConfiguration(
                                dataSource,
                                queryId,
                                mapOf(
                                    "param1" to listOf("value1")
                                )
                            )
                        )
                    filterIsInstance<PullTaskConfiguration>().single()
                        .isEqualTo(
                            PullTaskConfiguration(
                                dataSource,
                                true,
                                queryId,
                                mapOf(
                                    "param1" to listOf("value1")
                                ),
                                queryId,
                                setOf("a"),
                                mapOf(
                                    "param1" to listOf("value1")
                                ),
                                1000
                            )
                        )
                }
        }
    }

    @Test
    fun `validate config`() {
        val cfg = factory.mapper().readValue(
            """
            {
              "dataSources": {
                $TEST_DATA_SOURCE
              },
              "queries": {
                $TEST_QUERY
              },
              "startupTasks": [
                $READ_TASK,
                $PULL_TASK
              ]
            }
        """.trimIndent(), DataBaseReaderConfiguration::class.java)

        assertEquals(arrayListOf<String>(), cfg.validate())
    }

    @Test
    fun `validate config without data sources and queries`() {
        val cfg = factory.mapper().readValue(
            """
            {
              "dataSources": {},
              "queries": {},
              "startupTasks": [
                $READ_TASK,
                $PULL_TASK
              ]
            }
        """.trimIndent(), DataBaseReaderConfiguration::class.java)

        assertEquals(
            arrayListOf(
                "Unknown DataSourceId(id=test) in read task at index 0. Known sources: []",
                "Unknown query QueryId(id=test_query) in read task at index 0. Known queries: []",
                "Unknown DataSourceId(id=test) in pull task at index 1. Known sources: []",
                "Unknown init query QueryId(id=test_query) in pull task at index 1. Known queries: []",
                "Unknown update query QueryId(id=test_query) in pull task at index 1. Known queries: []"
            ),
            cfg.validate()
        )
    }

    @Test
    fun `deserializes config without initQueryId`() {
        val cfg = factory.mapper().readValue(
            """
            {
              "dataSources": {
                $TEST_DATA_SOURCE
              },
              "queries": {
                $TEST_QUERY
              },
              "startupTasks": [
                {
                  "type": "pull",
                  "dataSource": "test",
                  "startFromLastReadRow": true,
                  "initParameters": {
                    "param1": ["value1"]
                  },
                  "updateQueryId": "test_query",
                  "useColumns": ["a"],
                  "updateParameters": {
                    "param1": ["value1"]
                  },
                  "interval": 1000
                }
              ]
            }
        """.trimIndent(), DataBaseReaderConfiguration::class.java)

        val dataSource = DataSourceId("test")
        val queryId = QueryId("test_query")
        expectThat(cfg) {
            get { dataSources }
                .hasSize(1)
                .hasEntry(
                    dataSource, DataSourceConfiguration(
                        "test.url",
                        "user",
                        "pwd",
                        mapOf(
                            "prop1" to "value1"
                        )
                    )
                )
            get { queries }.hasSize(1)
                .hasEntry(
                    queryId, QueryConfiguration(
                        "SELECT * FROM table;",
                        mapOf(
                            "param1" to listOf("value1")
                        )
                    )
                )
            get { startupTasks }.hasSize(1)
                .apply {
                    filterIsInstance<PullTaskConfiguration>().single()
                        .isEqualTo(
                            PullTaskConfiguration(
                                dataSource,
                                true,
                                null,
                                mapOf(
                                    "param1" to listOf("value1")
                                ),
                                queryId,
                                setOf("a"),
                                mapOf(
                                    "param1" to listOf("value1")
                                ),
                                1000
                            )
                        )
                }
        }
    }

    @Test
    fun `validate config without initQueryId`() {
        val cfg = factory.mapper().readValue(
            """
            {
              "dataSources": {
                $TEST_DATA_SOURCE
              },
              "queries": {
                $TEST_QUERY
              },
              "startupTasks": [
                {
                  "type": "pull",
                  "dataSource": "test",
                  "startFromLastReadRow": true,
                  "initParameters": {
                    "param1": ["value1"]
                  },
                  "updateQueryId": "test_query",
                  "useColumns": ["a"],
                  "updateParameters": {
                    "param1": ["value1"]
                  },
                  "interval": 1000
                }
              ]
            }
        """.trimIndent(), DataBaseReaderConfiguration::class.java)

        assertEquals(arrayListOf<String>(), cfg.validate())
    }

    companion object {
        private const val TEST_DATA_SOURCE = """
                "test": {
                  "url": "test.url",
                  "username": "user",
                  "password": "pwd",
                  "properties": {
                    "prop1": "value1"
                  }
                }"""

        private const val TEST_QUERY = """
                "test_query": {
                  "query": "SELECT * FROM table;",
                  "defaultParameters": {
                    "param1": ["value1"]
                  }
                }"""

        private const val READ_TASK = """
                {
                  "type": "read",
                  "dataSource": "test",
                  "queryId": "test_query",
                  "parameters": {
                    "param1": [ "value1" ]
                  }
                }"""

        private const val PULL_TASK = """
                {
                  "type": "pull",
                  "dataSource": "test",
                  "startFromLastReadRow": true,
                  "initQueryId": "test_query",
                  "initParameters": {
                    "param1": ["value1"]
                  },
                  "updateQueryId": "test_query",
                  "useColumns": ["a"],
                  "updateParameters": {
                    "param1": ["value1"]
                  },
                  "interval": 1000
                }"""
    }
}