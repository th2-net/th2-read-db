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

package com.exactpro.th2.read.db.core.util

import mu.KotlinLogging
import java.sql.Array
import java.sql.Date
import java.sql.PreparedStatement
import java.sql.SQLFeatureNotSupportedException
import java.sql.SQLType
import java.sql.Time
import java.sql.Timestamp
import java.sql.Types
import java.time.Instant
import java.time.LocalDate

private val LOGGER = KotlinLogging.logger { }

fun PreparedStatement.set(paramIndex: Int, value: String?, type: SQLType) {
    try {
        setObject(paramIndex, value, type)
    } catch (ex: SQLFeatureNotSupportedException) {
        LOGGER.debug(ex) { "Feature with auto object conversion is not supported by the driver. Back of to manual conversion" }
        setManual(paramIndex, value, type)
    }
}

fun PreparedStatement.setCollection(paramIndex: Int, array: Array) {
    setArray(paramIndex, array)
}

private fun PreparedStatement.setManual(paramIndex: Int, value: String?, type: SQLType) {
    if (value == null) {
        setNull(paramIndex, type.vendorTypeNumber)
        return
    }
    when (type.vendorTypeNumber) {
        Types.DECIMAL, Types.NUMERIC -> setBigDecimal(paramIndex, value.toBigDecimal())
        Types.DOUBLE, Types.FLOAT -> setDouble(paramIndex, value.toDouble())
        Types.REAL -> setFloat(paramIndex, value.toFloat())
        Types.BIGINT -> setLong(paramIndex, value.toLong())
        Types.INTEGER -> setInt(paramIndex, value.toInt())
        Types.TINYINT, Types.SMALLINT -> setShort(paramIndex, value.toShort())
        Types.CHAR, Types.VARCHAR, Types.LONGVARCHAR,
        Types.NCHAR, Types.NVARCHAR, Types.LONGNVARCHAR -> setString(paramIndex, value)
        Types.BOOLEAN, Types.BIT -> setBoolean(paramIndex, value.toBoolean())
        Types.TIMESTAMP -> setTimestamp(paramIndex, value.toTimestamp())
        Types.DATE -> setDate(paramIndex, value.toDate())
        Types.TIME -> setTime(paramIndex, value.toTime())
        else -> error("unsupported type; $type. Please contact developers")
    }
}

private fun String.toTimestamp(): Timestamp = Timestamp.valueOf(this)

private fun String.toDate(): Date = Date.valueOf(this)

private fun String.toTime(): Time = Time.valueOf(this)