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

import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.databind.annotation.JsonSerialize
import com.fasterxml.jackson.dataformat.ion.IonTimestampSerializers.IonTimestampJavaDateSerializer
import software.amazon.qldb.tutorial.model.streams.StreamRecord.StreamRecordPayload
import software.amazon.qldb.tutorial.qldb.BlockAddress
import software.amazon.qldb.tutorial.qldb.TransactionInfo
import java.util.*

/**
 * Represents a summary for a Journal Block that was recorded after executing a
 * transaction in the ledger.
 */
class BlockSummaryRecord @JsonCreator constructor(
    @param:JsonProperty("blockAddress") val blockAddress: BlockAddress,
    @param:JsonProperty("transactionId") val transactionId: String,
    @field:JsonSerialize(using = IonTimestampJavaDateSerializer::class) @param:JsonProperty("blockTimestamp") val blockTimestamp: Date,
    @param:JsonProperty("blockHash") val blockHash: ByteArray,
    @param:JsonProperty("entriesHash") val entriesHash: ByteArray,
    @param:JsonProperty("previousBlockHash") val previousBlockHash: ByteArray,
    @param:JsonProperty("entriesHashList") val entriesHashList: Array<ByteArray>,
    @param:JsonProperty("transactionInfo") val transactionInfo: TransactionInfo,
    @param:JsonProperty("revisionSummaries") val revisionSummaries: List<RevisionSummary>
) : StreamRecordPayload {

    override fun toString(): String {
        return ("JournalBlock{"
                + "blockAddress=" + blockAddress
                + ", transactionId='" + transactionId + '\''
                + ", blockTimestamp=" + blockTimestamp
                + ", blockHash=" + blockHash.contentToString()
                + ", entriesHash=" + entriesHash.contentToString()
                + ", previousBlockHash=" + previousBlockHash.contentToString()
                + ", entriesHashList=" + entriesHashList.map { it.contentToString() }
                + ", transactionInfo=" + transactionInfo
                + ", revisionSummaries=" + revisionSummaries
                + '}')
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) {
            return true
        }
        if (other == null || javaClass != other.javaClass) {
            return false
        }
        val that = other as BlockSummaryRecord
        if (blockAddress != that.blockAddress) {
            return false
        }
        if (transactionId != that.transactionId) {
            return false
        }
        if (blockTimestamp != that.blockTimestamp) {
            return false
        }
        if (!blockHash.contentEquals(that.blockHash)) {
            return false
        }
        if (!entriesHash.contentEquals(that.entriesHash)) {
            return false
        }
        if (!previousBlockHash.contentEquals(that.previousBlockHash)) {
            return false
        }
        if (!entriesHashList.contentDeepEquals(that.entriesHashList)) {
            return false
        }
        if (transactionInfo != that.transactionInfo) {
            return false
        }
        return revisionSummaries == that.revisionSummaries
    }

    override fun hashCode(): Int {
        var result = blockAddress.hashCode()
        result = 31 * result + transactionId.hashCode()
        result = 31 * result + blockTimestamp.hashCode()
        result = 31 * result + blockHash.contentHashCode()
        result = 31 * result + entriesHash.contentHashCode()
        result = 31 * result + previousBlockHash.contentHashCode()
        result = 31 * result + entriesHashList.contentDeepHashCode()
        result = 31 * result + transactionInfo.hashCode()
        result = 31 * result + revisionSummaries.hashCode()
        return result
    }
}