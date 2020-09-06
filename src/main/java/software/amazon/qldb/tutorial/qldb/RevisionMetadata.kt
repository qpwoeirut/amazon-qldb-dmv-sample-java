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

import com.amazon.ion.IonInt
import com.amazon.ion.IonString
import com.amazon.ion.IonStruct
import com.amazon.ion.IonTimestamp
import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.databind.annotation.JsonSerialize
import com.fasterxml.jackson.dataformat.ion.IonTimestampSerializers.IonTimestampJavaDateSerializer
import org.slf4j.LoggerFactory
import java.util.*

/**
 * Represents the metadata field of a QLDB Document
 */
class RevisionMetadata @JsonCreator constructor(
    /**
     * Gets the unique ID of a QLDB document.
     *
     * @return the document ID.
     */
    @param:JsonProperty("id") val id: String,
    /**
     * Gets the version number of the document in the document's modification history.
     * @return the version number.
     */
    @param:JsonProperty("version") val version: Long,
    /**
     * Gets the time during which the document was modified.
     *
     * @return the transaction time.
     */
    @field:JsonSerialize(using = IonTimestampJavaDateSerializer::class) @param:JsonProperty("txTime") val txTime: Date,
    /**
     * Gets the transaction ID associated with this document.
     *
     * @return the transaction ID.
     */
    @param:JsonProperty("txId") val txId: String
) {

    /**
     * Converts a [RevisionMetadata] object to a string.
     *
     * @return the string representation of the [QldbRevision] object.
     */
    override fun toString(): String {
        return ("Metadata{"
                + "id='" + id + '\''
                + ", version=" + version
                + ", txTime=" + txTime
                + ", txId='" + txId
                + '\''
                + '}')
    }

    /**
     * Check whether two [RevisionMetadata] objects are equivalent.
     *
     * @return `true` if the two objects are equal, `false` otherwise.
     */
    override fun equals(o: Any?): Boolean {
        if (this === o) {
            return true
        }
        if (o == null || javaClass != o.javaClass) {
            return false
        }
        val metadata = o as RevisionMetadata
        return version == metadata.version && id == metadata.id && txTime == metadata.txTime && txId == metadata.txId
    }

    /**
     * Generate a hash code for the [RevisionMetadata] object.
     *
     * @return the hash code.
     */
    override fun hashCode(): Int {
        // CHECKSTYLE:OFF - Disabling as we are generating a hashCode of multiple properties.
        return Objects.hash(id, version, txTime, txId)
        // CHECKSTYLE:ON
    }

    companion object {
        private val log = LoggerFactory.getLogger(RevisionMetadata::class.java)
        fun fromIon(ionStruct: IonStruct?): RevisionMetadata {
            requireNotNull(ionStruct) { "Metadata cannot be null" }
            return try {
                val id = ionStruct["id"] as IonString
                val version = ionStruct["version"] as IonInt
                val txTime = ionStruct["txTime"] as IonTimestamp
                val txId = ionStruct["txId"] as IonString
                require(!(id == null || version == null || txTime == null || txId == null)) { "Document is missing required fields" }
                RevisionMetadata(id.stringValue(), version.longValue(), Date(txTime.millis), txId.stringValue())
            } catch (e: ClassCastException) {
                log.error("Failed to parse ion document")
                throw IllegalArgumentException("Document members are not of the correct type", e)
            }
        }
    }
}