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

import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments.of
import org.junit.jupiter.params.provider.MethodSource
import java.math.BigDecimal
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
class DefaultValueTransformerTest {
    @ParameterizedTest
    @MethodSource("values")
    fun `transform test`(value: Any, expected: String) {
        assertEquals(expected, DefaultValueTransformer.transform(value), "value: $value, class: ${value::class.java}")
    }

    private fun values() = listOf(
        of(BigDecimal("00123.45600"), "123.456"),
        of((-128).toByte(), "-128"),
        of(127.toByte(), "127"),
        of(255.toUByte(), "255"),
        of((-32768).toShort(), "-32768"),
        of(32767.toShort(), "32767"),
        of(65535.toUShort(), "65535"),
        of(-2147483648, "-2147483648"),
        of(2147483647, "2147483647"),
        of(4294967295.toUInt(), "4294967295"),
        of(-9223372036854775807L, "-9223372036854775807"),
        of(9223372036854775807L, "9223372036854775807"),
        of("18446744073709551615".toULong(), "18446744073709551615"),
        of(Date.valueOf(LocalDate.parse("2024-04-03")), "2024-04-03"),
        of(Time.valueOf(LocalTime.parse("01:02:03.123456789")), "01:02:03"),
        LocalDateTime.parse("2024-04-03T01:02:03.123456789").run {
            of(Timestamp.valueOf(this), this.atZone(ZoneId.systemDefault()).toInstant().toString())
        },
        of(LocalDate.parse("2024-04-03"), "2024-04-03"),
        of(LocalTime.parse("01:02:03.123456789"), "01:02:03.123456789"),
        of(LocalDateTime.parse("2024-04-03T01:02:03.123456789"), "2024-04-03T01:02:03.123456789"),
        of(Instant.parse("2024-04-03T01:02:03.123456789Z"), "2024-04-03T01:02:03.123456789Z"),
        of("clob".toByteArray(), "636c6f62")
    )
}