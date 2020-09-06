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
package software.amazon.qldb.tutorial.qldb

import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.annotation.JsonProperty
import org.slf4j.LoggerFactory
import java.util.*

/**
 * Represents the BlockAddress field of a QLDB document.
 */
class BlockAddress @JsonCreator constructor(
    @param:JsonProperty("strandId") val strandId: String,
    @param:JsonProperty("sequenceNo") val sequenceNo: Long
) {
    override fun toString(): String {
        return ("BlockAddress{"
                + "strandId='" + strandId + '\''
                + ", sequenceNo=" + sequenceNo
                + '}')
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) {
            return true
        }
        if (other == null || javaClass != other.javaClass) {
            return false
        }
        val that = other as BlockAddress
        return (sequenceNo == that.sequenceNo
                && strandId == that.strandId)
    }

    override fun hashCode(): Int {
        // CHECKSTYLE:OFF - Disabling as we are generating a hashCode of multiple properties.
        return Objects.hash(strandId, sequenceNo)
        // CHECKSTYLE:ON
    }

    companion object {
        private val log = LoggerFactory.getLogger(BlockAddress::class.java)
    }
}