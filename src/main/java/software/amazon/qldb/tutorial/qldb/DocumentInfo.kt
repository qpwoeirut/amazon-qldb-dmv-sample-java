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

/**
 * Information about an individual document in a table and the transactions associated with that document
 * in the given JournalBlock.
 */
class DocumentInfo @JsonCreator constructor(
    @param:JsonProperty("tableName") val tableName: String,
    @param:JsonProperty("tableId") val tableId: String,
    @get:JsonProperty("statements")
    @param:JsonProperty("statements") val statementIndexList: List<Int>
) {

    override fun equals(other: Any?): Boolean {
        if (this === other) {
            return true
        }
        if (other !is DocumentInfo) {
            return false
        }
        val that = other
        if (tableName != that.tableName) {
            return false
        }
        return if (tableId != that.tableId) {
            false
        } else statementIndexList == that.statementIndexList
    }

    override fun hashCode(): Int {
        var result = tableName.hashCode()
        result = 31 * result + tableId.hashCode()
        result = 31 * result + statementIndexList.hashCode()
        return result
    }

    override fun toString(): String {
        return "DocumentInfo{tableName='$tableName', tableId='$tableId', statementIndexList='$statementIndexList'}"
    }
}