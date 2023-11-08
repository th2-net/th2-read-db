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

package com.exactpro.th2.read.db.bootstrap

import com.exactpro.th2.common.message.direction
import com.exactpro.th2.common.message.sessionAlias
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.*
import com.exactpro.th2.read.db.core.DataSourceId
import com.exactpro.th2.read.db.core.TableRow
import com.google.protobuf.UnsafeByteOperations
import com.opencsv.CSVWriterBuilder
import io.netty.buffer.ByteBufUtil.hexDump
import io.netty.buffer.Unpooled
import java.io.ByteArrayOutputStream
import java.math.BigDecimal
import com.exactpro.th2.common.grpc.Direction as ProtoDirection
import com.exactpro.th2.common.grpc.RawMessage as ProtoRawMessage

internal fun TableRow.toProtoMessage(dataSourceId: DataSourceId): ProtoRawMessage.Builder {
    return ProtoRawMessage.newBuilder()
        .setBody(UnsafeByteOperations.unsafeWrap(toCsvBody()))
        .apply {
            sessionAlias = dataSourceId.id
            direction = ProtoDirection.FIRST
            associatedMessageType?.also {
                metadataBuilder.putProperties("th2.csv.override_message_type", it)
            }
        }
}

internal fun TableRow.toTransportMessage(dataSourceId: DataSourceId): RawMessage.Builder {
    val builder = RawMessage.builder()
        .setBody(Unpooled.wrappedBuffer(toCsvBody()))
        .apply {
            idBuilder()
                .setSessionAlias(dataSourceId.id)
                .setDirection(Direction.INCOMING)
        }

    if (associatedMessageType != null) {
        builder.setMetadata(mapOf("th2.csv.override_message_type" to associatedMessageType))
    }

    return builder
}

internal fun TableRow.toCsvBody(): ByteArray {
    return ByteArrayOutputStream().use {
        CSVWriterBuilder(it.writer())
            .withSeparator(',')
            .build().use { writer ->
                val columnNames = columns.keys.toTypedArray()
                val values: Array<String?> = columnNames.map { name -> columns[name]?.toStringValue() }
                    .toTypedArray()
                writer.writeNext(columnNames)
                writer.writeNext(values)
            }
        it
    }.toByteArray()
}

private fun Any.toStringValue(): String = when (this) {
    is BigDecimal -> stripTrailingZeros().toPlainString()
    is Double -> toBigDecimal().toStringValue()
    is Float -> toBigDecimal().toStringValue()
    is ByteArray -> hexDump(this)
    else -> toString()
}