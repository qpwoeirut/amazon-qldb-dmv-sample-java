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
import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.annotation.JsonProperty

/**
 * Information about the transaction. Contains all the statements executed as
 * part of the transaction and mapping between the documents to
 * tableName/tableId which were updated as part of the transaction.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
class TransactionInfo @JsonCreator constructor(
    @param:JsonProperty("statements") private val statements: List<StatementInfo>,
    @param:JsonProperty("documents") private val documents: Map<String, DocumentInfo>
) {
    fun getStatements(): List<StatementInfo>? {
        return statements
    }

    fun getDocuments(): Map<String, DocumentInfo>? {
        return documents
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) {
            return true
        }
        if (other !is TransactionInfo) {
            return false
        }
        if (if (getStatements() != null) getStatements() != other.getStatements() else other.getStatements() != null) {
            return false
        }
        return if (getDocuments() != null) getDocuments() == other.getDocuments() else other.getDocuments() == null
    }

    override fun hashCode(): Int {
        var result = if (getStatements() != null) getStatements().hashCode() else 0
        result = 31 * result + if (getDocuments() != null) getDocuments().hashCode() else 0
        return result
    }

    override fun toString(): String {
        return "TransactionInfo{statements=$statements, documents=$documents}"
    }
}