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

/**
 * Represents the revision summary that appears in the [ ]. Some revisions might not have a documentId. These are
 * internal-only system revisions that don't contain user data. Only the
 * revisions that do have a document ID are published in separate revision
 * details record.
 */
class RevisionSummary @JsonCreator constructor(
    @param:JsonProperty("documentId") val documentId: String,
    @param:JsonProperty("hash") val hash: ByteArray
) {
    override fun toString(): String {
        return "RevisionSummary{documentId='$documentId', hash=${hash.contentToString()}}"
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) {
            return true
        }
        if (other == null || javaClass != other.javaClass) {
            return false
        }
        val that = other as RevisionSummary
        return if (documentId != that.documentId) {
            false
        } else hash.contentEquals(that.hash)
    }

    override fun hashCode(): Int {
        var result = documentId.hashCode()
        result = 31 * result + hash.contentHashCode()
        return result
    }
}