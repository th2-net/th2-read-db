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

import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments.of
import org.junit.jupiter.params.provider.MethodSource
import org.mockito.kotlin.mock
import org.mockito.kotlin.verifyNoInteractions
import java.math.BigDecimal
import java.sql.Connection
import java.sql.Date
import java.sql.Time
import java.sql.Timestamp
import java.time.Instant
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import java.time.ZoneId
import kotlin.test.assertEquals

@TestInstance(PER_CLASS)
class DefaultValueTransformTest {
    private val connection = mock<Connection> {  }

    @AfterEach
    fun afterEach() {
        verifyNoInteractions(connection)
    }

    @ParameterizedTest
    @MethodSource("values")
    fun `transform test`(value: Any, expected: Any) {
        assertEquals(expected, DefaultValueTransform(value, connection), "value: $value, class: ${value::class.java}")
    }

    private fun values() = listOf(
        Instant.parse("2024-04-03T01:02:03.123456789Z").run { of(this, this) },
        of(BigDecimal("00123.45600"), "123.456"),
        of(Date.valueOf(LocalDate.parse("2024-04-03")), "2024-04-03"),
        of(Time.valueOf(LocalTime.parse("01:02:03.123456789")), "01:02:03"),
        LocalDateTime.parse("2024-04-03T01:02:03.123456789").run {
            of(Timestamp.valueOf(this), this.atZone(ZoneId.systemDefault()).toInstant().toString())
        },
        of("clob".toByteArray(), "636c6f62")
    )
}