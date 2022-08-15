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

package com.exactpro.th2.read.db.core

import com.fasterxml.jackson.databind.DeserializationContext
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import com.fasterxml.jackson.databind.deser.std.FromStringDeserializer

typealias QueryParametersValues = Map<String, Collection<String>>

@JsonDeserialize(using = DataSourceIdDeserializer::class)
data class DataSourceId(val id: String)

@JsonDeserialize(using = QueryIdDeserializer::class)
data class QueryId(val id: String)

data class TaskId(val id: String)

data class TableRow(val columns: Map<String, Any>, val associatedMessageType: String? = null)

private class DataSourceIdDeserializer : FromStringDeserializer<DataSourceId>(DataSourceId::class.java) {
    override fun _deserialize(value: String, ctxt: DeserializationContext?): DataSourceId = DataSourceId(value)
}

private class QueryIdDeserializer : FromStringDeserializer<QueryId>(QueryId::class.java) {
    override fun _deserialize(value: String, ctxt: DeserializationContext?): QueryId = QueryId(value)
}