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

package com.exactpro.th2.read.db.core.util

import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource
import org.mockito.kotlin.eq
import org.mockito.kotlin.mock
import org.mockito.kotlin.same
import org.mockito.kotlin.verify
import java.sql.JDBCType
import java.sql.PreparedStatement
import java.sql.SQLFeatureNotSupportedException
import java.sql.Timestamp
import java.time.LocalDateTime
import java.time.OffsetDateTime
import java.time.ZonedDateTime

class StatementUtilsTest {

    @ParameterizedTest
    @CsvSource(
        "2023-03-15T12:34:56.123456789Z,2023-03-15T12:34:56.123456789Z",
        "2023-03-15T12:34:56.123456Z,2023-03-15T12:34:56.123456Z",
        "2023-03-15T12:34:56.123Z,2023-03-15T12:34:56.123Z",
        "2023-03-15T12:34:56Z,2023-03-15T12:34:56Z",
    )
    fun `set timestamp in ISO format test`(verification: String, trusted: String) {
        val preparedStatement: PreparedStatement = mock {
            on { setObject(eq(0), same(verification), eq(JDBCType.TIMESTAMP)) }.thenAnswer {
                throw SQLFeatureNotSupportedException("test exception")
            }
        }
        preparedStatement.set(0, verification, JDBCType.TIMESTAMP)
        val expected = Timestamp.from(ZonedDateTime.parse(trusted).toInstant())
        verify(preparedStatement).setTimestamp(eq(0), eq(expected))
    }

    @ParameterizedTest
    @CsvSource(
        "2023-03-15 12:34:56.123456789,2023-03-15T12:34:56.123456789",
        "2023-03-15 12:34:56.123456,2023-03-15T12:34:56.123456",
        "2023-03-15 12:34:56.123,2023-03-15T12:34:56.123",
        "2023-03-15 12:34:56,2023-03-15T12:34:56",
        "2023-3-15 12:34:56,2023-03-15T12:34:56",
        "2023-03-5 12:34:56,2023-03-05T12:34:56",
    )
    fun `set timestamp test`(verification: String, trusted: String) {
        val preparedStatement: PreparedStatement = mock {
            on { setObject(eq(0), same(verification), eq(JDBCType.TIMESTAMP)) }.thenAnswer {
                throw SQLFeatureNotSupportedException("test exception")
            }
        }
        preparedStatement.set(0, verification, JDBCType.TIMESTAMP)
        val expected = Timestamp.from(LocalDateTime.parse(trusted).toInstant(OffsetDateTime.now().offset))
        verify(preparedStatement).setTimestamp(eq(0), eq(expected))
    }
}