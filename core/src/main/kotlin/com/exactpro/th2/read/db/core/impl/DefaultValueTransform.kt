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

import io.netty.buffer.ByteBufUtil.hexDump
import java.math.BigDecimal
import java.sql.Clob
import java.sql.Date
import java.sql.Time
import java.sql.Timestamp
import java.time.temporal.Temporal
import com.exactpro.th2.read.db.core.ValueTransform
import java.sql.Connection

object DefaultValueTransform: ValueTransform {
    override fun invoke(value: Any, connection: Connection): Any = when(value) {
        is BigDecimal -> value.stripTrailingZeros().toPlainString()
        is Byte, is UByte, is Short, is UShort, is Int, is UInt, is Long, is ULong -> value.toString()
        is Double -> invoke(value.toBigDecimal(), connection)
        is Float -> invoke(value.toBigDecimal(), connection)
        is ByteArray -> hexDump(value)
        is Date -> value.toLocalDate().toString()
        is Time -> value.toLocalTime().toString()
        is Timestamp -> value.toInstant().toString()
        is Temporal -> value.toString()
        is Clob -> value.characterStream.readText()
        else -> value
    }
}