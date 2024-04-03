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

import com.exactpro.th2.dataprovider.lw.grpc.MessageSearchResponse
import com.exactpro.th2.read.db.core.TableRow
import com.exactpro.th2.read.db.core.ValueTransformProvider.Companion.DEFAULT_TRANSFORM
import com.google.protobuf.ByteString
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.math.BigDecimal

class UtilsTest {

    @Test
    fun `toCsvBody test`() {
        val tableRow = TableRow(linkedMapOf(
            "test-double-column" to DEFAULT_TRANSFORM(0123.4560),
            "test-float-column" to DEFAULT_TRANSFORM(0789.0120f),
            "test-big-decimal-column" to DEFAULT_TRANSFORM(BigDecimal("0345.6780")),
            "test-string-column" to "abc",
            "test-blob-column" to DEFAULT_TRANSFORM("blob".toByteArray()),
            "test-null-column" to null,
        ))

        assertEquals(
            """
                "test-double-column","test-float-column","test-big-decimal-column","test-string-column","test-blob-column","test-null-column"
                "123.456","789.012","345.678","abc","626c6f62",
                
            """.trimIndent(),
            String(tableRow.toCsvBody()),
        )
    }

    @Test
    fun `toTableRow test`() {
        val expected = TableRow(linkedMapOf(
            "test-double-column" to "123.456",
            "test-float-column" to "789.012",
            "test-big-decimal-column" to "345.678",
            "test-string-column" to "abc",
            "test-blob-column" to "626c6f62",
            "test-null-column" to null,
        ), "test-message-type")

        val actual = MessageSearchResponse.newBuilder().apply {
            messageBuilder.apply {
                putMessageProperties(TH2_CSV_OVERRIDE_MESSAGE_TYPE_PROPERTY, expected.associatedMessageType)
                bodyRaw = ByteString.copyFrom(
                    """
                        "test-double-column","test-float-column","test-big-decimal-column","test-string-column","test-blob-column","test-null-column"
                        "123.456","789.012","345.678","abc","626c6f62",
                        
                    """.trimIndent().toByteArray()
                )
            }
        }.build().toTableRow()

        assertEquals(expected, actual)
    }
}