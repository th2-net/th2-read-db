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

package com.exactpro.th2.read.db.core

import com.exactpro.th2.read.db.app.ResetState
import com.exactpro.th2.read.db.core.DataBaseMonitorService.Companion.calculateNearestResetDate
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.Test
import java.time.Instant
import java.time.LocalTime
import java.time.ZoneOffset
import java.time.temporal.ChronoUnit.DAYS
import java.time.temporal.ChronoUnit.MINUTES

class DataBaseMonitorServiceTest {
    @Test
    fun `is reset required where empty test`() {
        assertNull(ResetState().calculateNearestResetDate(Instant.now()))
    }

    @Test
    fun `is reset required where after-date before now test`() {
        val now = Instant.now()
        val afterDate = now.minus(1L, MINUTES)
        assertSame(afterDate, ResetState(afterDate).calculateNearestResetDate(Instant.now()))
    }

    @Test
    fun `is reset required where after-date equals now test`() {
        val now = Instant.now()
        assertSame(now, ResetState(now).calculateNearestResetDate(now))
    }

    @Test
    fun `is reset required where after-date after now test`() {
        val now = Instant.now()
        val afterDate = now.plus(1L, MINUTES)
        assertNull(ResetState(afterDate).calculateNearestResetDate(Instant.now()))
    }

    @Test
    fun `is reset required where after-time before now test`() {
        val expected = BASE_DATE.minus(1L, MINUTES)
        val afterTime = LocalTime.ofInstant(expected, ZoneOffset.UTC)

        assertEquals(expected, ResetState(afterTime = afterTime).calculateNearestResetDate(BASE_DATE))
    }

    @Test
    fun `is reset required where after-time equals now test`() {
        val afterTime = LocalTime.ofInstant(BASE_DATE, ZoneOffset.UTC)
        assertEquals(BASE_DATE, ResetState(afterTime = afterTime).calculateNearestResetDate(BASE_DATE))
    }

    @Test
    fun `is reset required where after-time after now test`() {
        val expected = BASE_DATE.plus(1L, MINUTES).minus(1L, DAYS)
        val afterTime = LocalTime.ofInstant(expected, ZoneOffset.UTC)
        assertEquals(expected, ResetState(afterTime = afterTime).calculateNearestResetDate(BASE_DATE))
    }

    @Test
    fun `is reset required where after-date before after-time and both before now test`() {
        val expected = BASE_DATE.minus(1L, MINUTES)
        val afterDate = BASE_DATE.minus(2L, MINUTES)
        val afterTime = LocalTime.ofInstant(expected, ZoneOffset.UTC)
        assertEquals(expected, ResetState(afterDate, afterTime).calculateNearestResetDate(BASE_DATE))
    }

    @Test
    fun `is reset required where after-date after after-time and both before now test`() {
        val afterDate = BASE_DATE.minus(1L, MINUTES)
        val afterTime = LocalTime.ofInstant(BASE_DATE.minus(2L, MINUTES), ZoneOffset.UTC)
        assertEquals(afterDate, ResetState(afterDate, afterTime).calculateNearestResetDate(BASE_DATE))
    }

    @Test
    fun `is reset required where after-date before after-time and both after now test`() {
        val expected = BASE_DATE.plus(2L, MINUTES).minus(1L, DAYS)
        val afterDate = BASE_DATE.plus(1L, MINUTES)
        val afterTime = LocalTime.ofInstant(expected, ZoneOffset.UTC)
        assertEquals(expected, ResetState(afterDate, afterTime).calculateNearestResetDate(BASE_DATE))
    }

    @Test
    fun `is reset required where after-date after after-time and both after now test`() {
        val expected = BASE_DATE.plus(1L, MINUTES).minus(1L, DAYS)
        val afterDate = BASE_DATE.plus(2L, MINUTES)
        val afterTime = LocalTime.ofInstant(expected, ZoneOffset.UTC)
        assertEquals(expected, ResetState(afterDate, afterTime).calculateNearestResetDate(BASE_DATE))
    }

    companion object {
        /**
         * This date is middle of day to execute after-time tests  
         */
        private val BASE_DATE = Instant.parse("2023-11-14T12:12:34.567890123Z")
    }
}