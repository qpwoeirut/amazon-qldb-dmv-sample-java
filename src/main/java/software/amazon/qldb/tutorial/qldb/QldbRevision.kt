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

import com.amazon.ion.IonBlob
import com.amazon.ion.IonStruct
import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.annotation.JsonProperty
import org.slf4j.LoggerFactory
import software.amazon.qldb.tutorial.Constants
import software.amazon.qldb.tutorial.Verifier
import software.amazon.qldb.tutorial.qldb.QldbIonUtils.hashIonValue
import java.io.IOException
import java.util.*

/**
 * Represents a QldbRevision including both user data and metadata.
 */
class QldbRevision @JsonCreator constructor(
    /**
     * Gets the unique ID of a QLDB document.
     *
     * @return the [BlockAddress] object.
     */
    @param:JsonProperty("blockAddress") val blockAddress: BlockAddress,

    /**
     * Gets the metadata of the revision.
     *
     * @return the [RevisionMetadata] object.
     */
    @param:JsonProperty("metadata") val metadata: RevisionMetadata,

    /**
     * Gets the SHA-256 hash value of the data.
     *
     * @return the byte array representing the hash.
     */
    @param:JsonProperty("hash") val hash: ByteArray,
    /**
     * Gets the revision data.
     *
     * @return the revision data.
     */
    @param:JsonProperty("data") val data: IonStruct
) {

    /**
     * Converts a [QldbRevision] object to string.
     *
     * @return the string representation of the [QldbRevision] object.
     */
    override fun toString(): String {
        return "QldbRevision{" +
                "blockAddress=" + blockAddress +
                ", metadata=" + metadata +
                ", hash=" + hash.contentToString() +
                ", data=" + data +
                '}'
    }

    /**
     * Check whether two [QldbRevision] objects are equivalent.
     *
     * @return `true` if the two objects are equal, `false` otherwise.
     */
    override fun equals(other: Any?): Boolean {
        if (this === other) {
            return true
        }
        if (other !is QldbRevision) {
            return false
        }
        val that = other
        return blockAddress == that.blockAddress && metadata == that.metadata && Arrays.equals(
            hash,
            that.hash
        ) && data == that.data
    }

    /**
     * Create a hash code for the [QldbRevision] object.
     *
     * @return the hash code.
     */
    override fun hashCode(): Int {
        // CHECKSTYLE:OFF - Disabling as we are generating a hashCode of multiple properties.
        var result = Objects.hash(blockAddress, metadata, data)
        // CHECKSTYLE:ON
        result = 31 * result + Arrays.hashCode(hash)
        return result
    }

    /**
     * Throws an IllegalArgumentException if the hash of the revision data and metadata
     * does not match the hash provided by QLDB with the revision.
     */
    fun verifyRevisionHash() {
        // Certain internal-only system revisions only contain a hash which cannot be
        // further computed. However, these system hashes still participate to validate
        // the journal block. User revisions will always contain values for all fields
        // and can therefore have their hash computed.
        if (blockAddress == null && metadata == null && data == null) {
            return
        }
        try {
            val metadataIon = Constants.MAPPER.writeValueAsIonValue(
                metadata
            ) as IonStruct
            verifyRevisionHash(metadataIon, data, hash)
        } catch (e: IOException) {
            throw IllegalArgumentException("Could not encode revision metadata to ion.", e)
        }
    }

    companion object {
        private val log = LoggerFactory.getLogger(QldbRevision::class.java)

        /**
         * Constructs a new [QldbRevision] from an [IonStruct].
         *
         * The specified [IonStruct] must include the following fields
         *
         * - blockAddress -- a [BlockAddress],
         * - metadata -- a [RevisionMetadata],
         * - hash -- the document's hash calculated by QLDB,
         * - data -- an [IonStruct] containing user data in the document.
         *
         * If any of these fields are missing or are malformed, then throws [IllegalArgumentException].
         *
         * If the document hash calculated from the members of the specified [IonStruct] does not match
         * the hash member of the [IonStruct] then throws [IllegalArgumentException].
         *
         * @param ionStruct
         * The [IonStruct] that contains a [QldbRevision] object.
         * @return the converted [QldbRevision] object.
         * @throws IOException if failed to parse parameter [IonStruct].
         */
        @JvmStatic
        @Throws(IOException::class)
        fun fromIon(ionStruct: IonStruct): QldbRevision {
            return try {
                val blockAddress = Constants.MAPPER.readValue(ionStruct["blockAddress"], BlockAddress::class.java)
                val hash = ionStruct["hash"] as IonBlob
                val metadataStruct = ionStruct["metadata"] as IonStruct
                val data = ionStruct["data"] as IonStruct
                require(!(hash == null || data == null)) { "Document is missing required fields" }
                verifyRevisionHash(metadataStruct, data, hash.bytes)
                val metadata = RevisionMetadata.fromIon(metadataStruct)
                QldbRevision(blockAddress, metadata, hash.bytes, data)
            } catch (e: ClassCastException) {
                log.error("Failed to parse ion document")
                throw IllegalArgumentException("Document members are not of the correct type", e)
            }
        }

        private fun verifyRevisionHash(metadata: IonStruct, revisionData: IonStruct?, expectedHash: ByteArray) {
            val metadataHash = hashIonValue(metadata)
            val dataHash = hashIonValue(revisionData)
            val candidateHash = Verifier.dot(metadataHash, dataHash)
            require(Arrays.equals(candidateHash, expectedHash)) {
                ("Hash entry of QLDB revision and computed hash "
                        + "of QLDB revision do not match")
            }
        }
    }
}