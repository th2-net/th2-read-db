/*
 * Copyright 2022-2023 Exactpro (Exactpro Systems Limited)
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

import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.databind.DeserializationContext
import com.fasterxml.jackson.databind.SerializerProvider
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import com.fasterxml.jackson.databind.annotation.JsonSerialize
import com.fasterxml.jackson.databind.deser.std.FromStringDeserializer
import com.fasterxml.jackson.databind.ser.std.StdSerializer

typealias QueryParametersValues = Map<String, Collection<String>>

@JsonDeserialize(using = DataSourceIdDeserializer::class)
@JsonSerialize(using = DataSourceIdSerializer::class)
data class DataSourceId(val id: String)

@JsonDeserialize(using = QueryIdDeserializer::class)
@JsonSerialize(using = QueryIdSerializer::class)
data class QueryId(val id: String)

data class TaskId(val id: String)

data class TableRow(
    val columns: Map<String, Any?>,
    val associatedMessageType: String? = null,
    val executionId: Long? = null
)

private class DataSourceIdDeserializer : FromStringDeserializer<DataSourceId>(DataSourceId::class.java) {
    override fun _deserialize(value: String, ctxt: DeserializationContext?): DataSourceId = DataSourceId(value)
}

private class DataSourceIdSerializer : StdSerializer<DataSourceId>(DataSourceId::class.java) {
    override fun serialize(value: DataSourceId, gen: JsonGenerator, provider: SerializerProvider) =
        gen.writeString(value.id)
}

private class QueryIdDeserializer : FromStringDeserializer<QueryId>(QueryId::class.java) {
    override fun _deserialize(value: String, ctxt: DeserializationContext?): QueryId = QueryId(value)
}

private class QueryIdSerializer : StdSerializer<QueryId>(QueryId::class.java) {
    override fun serialize(value: QueryId, gen: JsonGenerator, provider: SerializerProvider) =
        gen.writeString(value.id)
}