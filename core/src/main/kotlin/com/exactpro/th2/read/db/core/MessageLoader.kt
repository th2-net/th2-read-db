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

import java.time.Instant

fun interface MessageLoader {
    /**
     * Loads the most recent message from the Cradle produced by data source with [dataSourceId]
     * that message timestamp is after or equal [lowerBoundary] and has a specified set of [properties] in its metadata
     * @param dataSourceId is used for session alias determination related to the data source
     * @param lowerBoundary the time boundary for message loading. Only messages with a timestamp greater or equal to this value will be loaded if they exist.
     * Must be lower than the current time if set
     * @param properties is used for filtering messages to find the first suitable
     */
    fun load(dataSourceId: DataSourceId, lowerBoundary: Instant?, properties: Map<String, String>): TableRow?

    companion object {
        @JvmField
        val DISABLED = MessageLoader { _, _, _ -> error("Message loader doesn't configured to execute request") }
    }
}