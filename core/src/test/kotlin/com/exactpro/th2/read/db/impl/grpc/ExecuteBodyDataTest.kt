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

package com.exactpro.th2.read.db.impl.grpc

import com.exactpro.th2.common.schema.factory.AbstractCommonFactory.MAPPER
import com.exactpro.th2.read.db.app.ExecuteQueryRequest
import com.exactpro.th2.read.db.core.DataSourceConfiguration
import com.exactpro.th2.read.db.core.DataSourceId
import com.exactpro.th2.read.db.core.QueryConfiguration
import com.exactpro.th2.read.db.core.QueryId
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class ExecuteBodyDataTest {

    @Test
    fun `serializes body data - fulfilled`() {
        val bean = ExecuteBodyData(
            123,
            DataSourceConfiguration(
                "jdbc:mysql://localhost:1234/test_data",
                "test-username",
                "test-password",
                mapOf("test-property" to "test-property-value")
            ),
            listOf(
                QueryConfiguration(
                    "test-before-query",
                    mapOf("test-before-parameter" to listOf("test-before-parameter-value"))
                )
            ),
            QueryConfiguration(
                "test-query",
                mapOf("test-parameter" to listOf("test-parameter-value")),
                "test-message-type"
            ),
            listOf(
                QueryConfiguration(
                    "test-after-query",
                    mapOf("test-after-parameter" to listOf("test-after-parameter-value"))
                )
            ),
            ExecuteQueryRequest(
                DataSourceId("test-data-source-id"),
                listOf(QueryId("test-before-query-id")),
                QueryId("test-query-id"),
                listOf(QueryId("test-after-query-id")),
                mapOf("test-user-parameter" to listOf("test-user-parameter-value"))
            )
        )

        val actual = MAPPER.writeValueAsString(bean)
        assertEquals(
            """
            |{
              |"executionId":123,
              |"dataSource":{
                |"url":"jdbc:mysql://localhost:1234/test_data",
                |"username":"test-username",
                |"properties":{
                  |"test-property":"test-property-value"
                |}
              |},
              |"beforeQueries":[
                |{
                  |"query":"test-before-query",
                  |"defaultParameters":{
                    |"test-before-parameter":[
                      |"test-before-parameter-value"
                    |]
                  |}
                |}
              |],
              |"query":{
                |"query":"test-query",
                |"defaultParameters":{
                  |"test-parameter":[
                    |"test-parameter-value"
                  |]
                |},
                |"messageType":"test-message-type"
              |},
              |"afterQueries":[
                |{
                  |"query":"test-after-query",
                  |"defaultParameters":{
                    |"test-after-parameter":[
                      |"test-after-parameter-value"
                    |]
                  |}
                |}
              |],
              |"request":{
                |"sourceId":"test-data-source-id",
                |"before":[
                  |"test-before-query-id"
                |],
                |"queryId":"test-query-id",
                |"after":[
                    |"test-after-query-id"
                |],
                |"parameters":{
                  |"test-user-parameter":[
                    |"test-user-parameter-value"
                  |]
                |}
              |}
            |}
        """.trimMargin().replace(Regex("\n"), ""), actual
        )
    }

    @Test
    fun `serializes body data - low-filled`() {
        val bean = ExecuteBodyData(
            123,
            DataSourceConfiguration(
                "jdbc:mysql://localhost:1234/test_data",
                "test-username",
                "test-password"
            ),
            emptyList(),
            QueryConfiguration(
                "test-query",
            ),
            emptyList(),
            ExecuteQueryRequest(
                DataSourceId("test-data-source-id"),
                emptyList(),
                QueryId("test-query-id"),
                emptyList(),
                emptyMap()
            )
        )

        val actual = MAPPER.writeValueAsString(bean)
        assertEquals(
            """
            |{
              |"executionId":123,
              |"dataSource":{
                |"url":"jdbc:mysql://localhost:1234/test_data",
                |"username":"test-username"
              |},
              |"query":{
                |"query":"test-query"
              |},
              |"request":{
                |"sourceId":"test-data-source-id",
                |"queryId":"test-query-id"
              |}
            |}
        """.trimMargin().replace(Regex("\n"), ""), actual
        )
    }
}