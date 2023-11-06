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

import com.exactpro.th2.common.message.toJson
import com.exactpro.th2.dataprovider.lw.grpc.MessageSearchResponse
import com.exactpro.th2.read.db.core.TableRow
import com.opencsv.CSVParserBuilder
import com.opencsv.CSVReaderBuilder
import com.opencsv.CSVWriterBuilder
import com.opencsv.enums.CSVReaderNullFieldIndicator
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.math.BigDecimal
import java.util.HashMap

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

/**
 * NOTE: crated TableRow contains only [String] values
 */
internal fun MessageSearchResponse.toTableRow(): TableRow {
    return ByteArrayInputStream(message.bodyRaw.toByteArray()).use {
        CSVReaderBuilder(it.reader())
            .withCSVParser(
                CSVParserBuilder()
                .withFieldAsNull(CSVReaderNullFieldIndicator.EMPTY_SEPARATORS)
                .withSeparator(',')
                .build()
            ).build().use { reader ->
                val lines = reader.readAll()
                check(lines.size == 2) {
                    "CSV content of '${message.messageId.toJson()}' message id has ${lines.size} rows instead of 2"
                }

                val header = lines[0]
                val row = lines[1]
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

private fun Any.toStringValue(): String = when (this) {
    is BigDecimal -> stripTrailingZeros().toPlainString()
    is Double -> toBigDecimal().toStringValue()
    is Float -> toBigDecimal().toStringValue()
    else -> toString()
}