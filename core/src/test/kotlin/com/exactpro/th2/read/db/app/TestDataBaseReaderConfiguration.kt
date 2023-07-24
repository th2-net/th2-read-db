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
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import strikt.api.expectThat
import strikt.assertions.filterIsInstance
import strikt.assertions.getValue
import strikt.assertions.hasEntry
import strikt.assertions.hasSize
import strikt.assertions.isEqualTo
import strikt.assertions.isNull
import strikt.assertions.single
import java.io.InputStream
import java.nio.file.Path

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
internal class TestDataBaseReaderConfiguration {
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
    fun deserializes() {
        val cfg = factory.mapper().readValue(
            """
            {
              "dataSources": {
                "test": {
                  "url": "test.url",
                  "username": "user",
                  "password": "pwd",
                  "properties": {
                    "prop1": "value1"
                  }
                },
                "testWithBook": {
                  "url": "test.url",
                  "username": "user",
                  "password": "pwd",
                  "properties": {
                    "prop1": "value1"
                  },
                  "bookName": "testBook"
                }
              },
              "queries": {
                "test_query": {
                  "query": "SELECT * FROM table;",
                  "defaultParameters": {
                    "param1": ["value1"]
                  }
                }
              },
              "startupTasks": [
                {
                  "type": "read",
                  "dataSource": "test",
                  "queryId": "test_query",
                  "parameters": {
                    "param1": [ "value1" ]
                  }
                },
                {
                  "type": "pull",
                  "dataSource": "test",
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
                }
              ]
            }
        """.trimIndent(), DataBaseReaderConfiguration::class.java)

        val dataSource = DataSourceId("test")
        val queryId = QueryId("test_query")
        expectThat(cfg) {
            get { dataSources }
                .hasSize(2).apply {
                    getValue(dataSource).apply {
                        get { sourceConfiguration }
                            .isEqualTo(
                                DataSourceConfiguration(
                                    "test.url",
                                    "user",
                                    "pwd",
                                    mapOf(
                                        "prop1" to "value1"
                                    )
                                )
                            )
                        get { bookName }.isNull()
                    }
                    getValue(DataSourceId("testWithBook")).apply {
                        get { sourceConfiguration }
                            .isEqualTo(
                                DataSourceConfiguration(
                                    "test.url",
                                    "user",
                                    "pwd",
                                    mapOf(
                                        "prop1" to "value1"
                                    )
                                )
                            )
                        get { bookName } isEqualTo "testBook"
                    }
                }
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
}