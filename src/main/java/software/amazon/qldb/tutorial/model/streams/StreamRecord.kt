/*
 * Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: MIT-0
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this
 * software and associated documentation files (the "Software"), to deal in the Software
 * without restriction, including without limitation the rights to use, copy, modify,
 * merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
 * INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
 * PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
 * HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
 * OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
 * SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */
package software.amazon.qldb.tutorial.model.streams

import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.DeserializationContext
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import com.fasterxml.jackson.databind.deser.std.StdDeserializer
import java.io.IOException

/**
 * Represents a record on the Qldb stream. An Amazon QLDB stream writes three
 * types of data records to a given Amazon Kinesis Data Streams resource:
 * control, block summary, and revision details.
 *
 *
 * Control records indicate the start and completion of your QLDB streams.
 * Whenever a revision is committed to your ledger, a QLDB stream writes all of
 * the associated journal block data in block summary and revision details
 * records.
 *
 * @see ControlRecord
 *
 * @see BlockSummaryRecord
 *
 * @see RevisionDetailsRecord
 */
@JsonDeserialize(using = StreamRecord.Deserializer::class)
class StreamRecord(val qldbStreamArn: String, val recordType: String, val payload: StreamRecordPayload) {
    override fun toString(): String {
        return "StreamRecord{qldbStreamArn='$qldbStreamArn', recordType='$recordType', payload=$payload}"
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) {
            return true
        }
        if (other == null || javaClass != other.javaClass) {
            return false
        }
        val that = other as StreamRecord
        if (qldbStreamArn != that.qldbStreamArn) {
            return false
        }
        return if (recordType != that.recordType) {
            false
        } else payload == that.payload
    }

    override fun hashCode(): Int {
        var result = qldbStreamArn.hashCode()
        result = 31 * result + (recordType.hashCode())
        result = 31 * result + (payload.hashCode())
        return result
    }

    interface StreamRecordPayload
    internal class Deserializer private constructor(vc: Class<*>? = null) : StdDeserializer<StreamRecord>(vc) {
        @Throws(IOException::class)
        override fun deserialize(jp: JsonParser, dc: DeserializationContext): StreamRecord {
            val codec = jp.codec
            val node = codec.readTree<JsonNode>(jp)
            val qldbStreamArn = node["qldbStreamArn"].textValue()
            val recordType = node["recordType"].textValue()
            val payloadJson = node["payload"]
            val payload = when (recordType) {
                "CONTROL" -> codec.treeToValue(payloadJson, ControlRecord::class.java)
                "BLOCK_SUMMARY" -> codec.treeToValue(payloadJson, BlockSummaryRecord::class.java)
                "REVISION_DETAILS" -> codec.treeToValue(payloadJson, RevisionDetailsRecord::class.java)
                else -> throw RuntimeException("Unsupported record type: $recordType")
            }
            return StreamRecord(qldbStreamArn, recordType, payload)
        }
    }
}