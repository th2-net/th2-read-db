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

package com.exactpro.th2.read.db.core.impl

import oracle.jdbc.internal.OracleConnection
import oracle.sql.TIMESTAMP
import oracle.sql.TIMESTAMPLTZ
import oracle.sql.TIMESTAMPTZ
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.mockito.kotlin.any
import org.mockito.kotlin.eq
import org.mockito.kotlin.mock
import org.mockito.kotlin.times
import org.mockito.kotlin.verify
import org.mockito.kotlin.verifyNoMoreInteractions
import java.sql.Connection
import java.time.LocalDateTime
import java.time.ZoneOffset
import java.time.ZonedDateTime
import kotlin.test.assertEquals

class OracleValueTransformTest {
    private val zoneId = ZoneOffset.UTC

    private val oracleConnection = mock<OracleConnection> {
        on { databaseZoneId }.thenReturn(zoneId)
        on { sessionZoneId }.thenReturn(zoneId)
    }
    private val connection = mock<Connection> {
        on { unwrap(eq(OracleConnection::class.java)) }.thenReturn(oracleConnection)

    }

    @AfterEach
    fun afterEach() {
        verifyNoMoreInteractions(oracleConnection)
        verifyNoMoreInteractions(connection)
    }

    @Test
    fun `transform TIMESTAMP test`() {
        val expected = "2024-04-03T01:02:03.123456789"
        assertEquals(expected, OracleValueTransform(TIMESTAMP.of(LocalDateTime.parse(expected)), connection))
    }

    @Test
    fun `transform TIMESTAMPTZ test`() {
        val expected = "2024-04-03T01:02:03.123456789Z"
        assertEquals(expected, OracleValueTransform(TIMESTAMPTZ.of(ZonedDateTime.parse(expected)), connection))
    }

    @Test
    fun `transform TIMESTAMPLTZ test`() {
        val expected = "2024-04-03T01:02:03.123456789"
        assertEquals(
            expected,
            OracleValueTransform(TIMESTAMPLTZ.of(connection, ZonedDateTime.parse("${expected}Z")), connection)
        )
        verify(connection, times(2)).unwrap(any<Class<*>>())
        verify(oracleConnection, times(2)).databaseZoneId
        verify(oracleConnection).sessionZoneId
    }
}