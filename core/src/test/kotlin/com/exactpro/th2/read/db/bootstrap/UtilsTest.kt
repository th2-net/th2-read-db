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

import com.exactpro.th2.read.db.core.TableRow
import org.junit.jupiter.api.Test

import org.junit.jupiter.api.Assertions.*
import java.math.BigDecimal

class UtilsTest {

    @Test
    fun `toCsvBody test`() {
        val tableRow = TableRow(linkedMapOf(
            "test-double-column" to 0123.4560,
            "test-float-column" to 0789.0120f,
            "test-big-decimal-column" to BigDecimal("0345.6780"),
            "test-string-column" to "abc",
            "test-blob-column" to "blob".toByteArray(),
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
}