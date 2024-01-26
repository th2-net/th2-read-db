/*
 * Copyright 2023-2024 Exactpro (Exactpro Systems Limited)
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

import com.exactpro.th2.common.grpc.Direction.FIRST
import com.exactpro.th2.common.message.direction
import com.exactpro.th2.common.message.sessionAlias
import com.exactpro.th2.common.message.toJson
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.Direction
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.RawMessage
import com.exactpro.th2.dataprovider.lw.grpc.MessageSearchResponse
import com.exactpro.th2.read.db.core.DataSourceId
import com.exactpro.th2.read.db.core.TableRow
import com.google.protobuf.UnsafeByteOperations
import com.opencsv.CSVParserBuilder
import com.opencsv.CSVReaderBuilder
import com.opencsv.CSVWriterBuilder
import com.opencsv.enums.CSVReaderNullFieldIndicator
import io.netty.buffer.ByteBufUtil.hexDump
import io.netty.buffer.Unpooled
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.math.BigDecimal
import com.exactpro.th2.common.grpc.RawMessage as ProtoRawMessage

internal const val TH2_CSV_OVERRIDE_MESSAGE_TYPE_PROPERTY = "th2.csv.override_message_type"
private const val TH2_READ_DB_UNIQUE_ID = "th2.read-db.execute.uid"
private const val SEPARATOR = ','

internal fun TableRow.toProtoMessage(dataSourceId: DataSourceId, properties: Map<String, String>): ProtoRawMessage.Builder {
    return ProtoRawMessage.newBuilder()
        .setBody(UnsafeByteOperations.unsafeWrap(toCsvBody()))
        .apply {
            sessionAlias = dataSourceId.id
            direction = FIRST
            associatedMessageType?.also {
                metadataBuilder.putProperties(TH2_CSV_OVERRIDE_MESSAGE_TYPE_PROPERTY, it)
            }
            metadataBuilder.putAllProperties(properties)
        }
}

internal fun TableRow.toTransportMessage(dataSourceId: DataSourceId, properties: Map<String, String>): RawMessage.Builder {
    val builder = RawMessage.builder()
        .setBody(Unpooled.wrappedBuffer(toCsvBody()))
        .apply {
            idBuilder()
                .setSessionAlias(dataSourceId.id)
                .setDirection(Direction.INCOMING)
        }

    if (associatedMessageType != null) {
        builder.addMetadataProperty(TH2_CSV_OVERRIDE_MESSAGE_TYPE_PROPERTY, associatedMessageType)
    }
    if (executionId != null) {
        builder.addMetadataProperty(TH2_READ_DB_UNIQUE_ID, executionId.toString())
    }
    properties.forEach(builder::addMetadataProperty)

    return builder
}

internal fun TableRow.toCsvBody(): ByteArray {
    return ByteArrayOutputStream().use {
        CSVWriterBuilder(it.writer())
            .withSeparator(SEPARATOR)
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

/**
 * NOTE: crated TableRow contains only [String] values
 */
internal fun MessageSearchResponse.toTableRow(): TableRow {
    return ByteArrayInputStream(message.bodyRaw.toByteArray()).use {
        CSVReaderBuilder(it.reader())
            .withCSVParser(
                CSVParserBuilder()
                .withFieldAsNull(CSVReaderNullFieldIndicator.EMPTY_SEPARATORS)
                .withSeparator(SEPARATOR)
                .build()
            ).build().use { reader ->
                val lines: MutableList<Array<String>> = reader.readAll()
                check(lines.size == 2) {
                    "CSV content of '${message.messageId.toJson()}' message id has ${lines.size} rows instead of 2"
                }

                val (header, row) = lines
                check(header.size == row.size) {
                    "CSV content of '${message.messageId.toJson()}' message id has different length of header [${header.size}] and row [${row.size}]"
                }

                val columns = HashMap<String, String>()
                header.forEachIndexed { index, column ->
                    columns.put(column, row[index]).also { previous ->
                        check(previous == null) {
                            "CSV content of '${message.messageId.toJson()}' message id has duplicate column: $column with values: [$previous, ${row[index]}]"
                        }
                    }
                }

                TableRow(
                    columns,
                    message.getMessagePropertiesOrDefault(TH2_CSV_OVERRIDE_MESSAGE_TYPE_PROPERTY, null)
                )
            }
    }
}

internal fun Any.toStringValue(): String = when (this) {
    is BigDecimal -> stripTrailingZeros().toPlainString()
    is Double -> toBigDecimal().toStringValue()
    is Float -> toBigDecimal().toStringValue()
    is ByteArray -> hexDump(this)
    else -> toString()
}