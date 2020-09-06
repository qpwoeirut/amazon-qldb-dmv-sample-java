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
import com.fasterxml.jackson.databind.annotation.JsonSerialize
import com.fasterxml.jackson.dataformat.ion.IonTimestampSerializers.IonTimestampJavaDateSerializer
import org.slf4j.LoggerFactory
import software.amazon.qldb.tutorial.Constants
import software.amazon.qldb.tutorial.Verifier
import java.io.IOException
import java.nio.ByteBuffer
import java.util.*
import java.util.function.Consumer
import java.util.stream.Collectors

/**
 * Represents a JournalBlock that was recorded after executing a transaction
 * in the ledger.
 */
class JournalBlock @JsonCreator constructor(
    @param:JsonProperty("blockAddress") val blockAddress: BlockAddress,
    @param:JsonProperty("transactionId") val transactionId: String,
    @field:JsonSerialize(using = IonTimestampJavaDateSerializer::class) @param:JsonProperty("blockTimestamp") val blockTimestamp: Date,
    @param:JsonProperty("blockHash") val blockHash: ByteArray,
    @param:JsonProperty("entriesHash") val entriesHash: ByteArray,
    @param:JsonProperty("previousBlockHash") val previousBlockHash: ByteArray,
    @param:JsonProperty("entriesHashList") val entriesHashList: Array<ByteArray>,
    @param:JsonProperty("transactionInfo") val transactionInfo: TransactionInfo,
    @param:JsonProperty("revisions") val revisions: List<QldbRevision>
) {

    override fun toString(): String {
        return """JournalBlock{blockAddress=$blockAddress, 
                    |transactionId='$transactionId', 
                    |blockTimestamp=$blockTimestamp, 
                    |blockHash=${Arrays.toString(blockHash)}, 
                    |entriesHash=${Arrays.toString(entriesHash)}, 
                    |previousBlockHash=${Arrays.toString(previousBlockHash)},
                    |entriesHashList=${Arrays.toString(entriesHashList)},
                    |transactionInfo=$transactionInfo,
                    |revisions=$revisions
                    |}""".trimMargin()
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) {
            return true
        }
        if (other !is JournalBlock) {
            return false
        }
        if (!blockAddress.equals(other.blockAddress)) {
            return false
        }
        if (transactionId != other.transactionId) {
            return false
        }
        if (blockTimestamp != other.blockTimestamp) {
            return false
        }
        if (!blockHash.contentEquals(other.blockHash)) {
            return false
        }
        if (!entriesHash.contentEquals(other.entriesHash)) {
            return false
        }
        if (!previousBlockHash.contentEquals(other.previousBlockHash)) {
            return false
        }
        if (!entriesHashList.contentDeepEquals(other.entriesHashList)) {
            return false
        }
        if (transactionInfo != other.transactionInfo) {
            return false
        }
        return revisions == other.revisions
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
        result = 31 * result + revisions.hashCode()
        return result
    }

    /**
     * This method validates that the hashes of the components of a journal block make up the block
     * hash that is provided with the block itself.
     *
     * The components that contribute to the hash of the journal block consist of the following:
     * - user transaction information (contained in [transactionInfo])
     * - user revisions (contained in [revisions])
     * - hashes of internal-only system metadata (contained in [revisions] and in [entriesHashList])
     * - the previous block hash
     *
     * If any of the computed hashes of user information cannot be validated or any of the system
     * hashes do not result in the correct computed values, this method will throw an IllegalArgumentException.
     *
     * Internal-only system metadata is represented by its hash, and can be present in the form of certain
     * items in the [revisions] list that only contain a hash and no user data, as well as some hashes
     * in [entriesHashList].
     *
     * To validate that the hashes of the user data are valid components of the [blockHash], this method
     * performs the following steps:
     *
     * 1. Compute the hash of the [transactionInfo] and validate that it is included in the [entriesHashList].
     * 2. Validate the hash of each user revision was correctly computed and matches the hash published
     * with that revision.
     * 3. Compute the hash of the [revisions] by treating the revision hashes as the leaf nodes of a Merkle tree
     * and calculating the root hash of that tree. Then validate that hash is included in the [entriesHashList].
     * 4. Compute the hash of the [entriesHashList] by treating the hashes as the leaf nodes of a Merkle tree
     * and calculating the root hash of that tree. Then validate that hash matches [entriesHash].
     * 5. Finally, compute the block hash by computing the hash resulting from concatenating the [entriesHash]
     * and previous block hash, and validate that the result matches the [blockHash] provided by QLDB with the block.
     *
     * This method is called by ValidateQldbHashChain::verify for each journal block to validate its
     * contents before verifying that the hash chain between consecutive blocks is correct.
     */
    fun verifyBlockHash() {
        val entriesHashSet: MutableSet<ByteBuffer> = HashSet()
        Arrays.stream(entriesHashList)
            .forEach { hash: ByteArray? -> entriesHashSet.add(ByteBuffer.wrap(hash).asReadOnlyBuffer()) }
        val computedTransactionInfoHash = computeTransactionInfoHash()
        require(
            entriesHashSet.contains(
                ByteBuffer.wrap(computedTransactionInfoHash).asReadOnlyBuffer()
            )
        ) { "Block transactionInfo hash is not contained in the QLDB block entries hash list." }
        if (revisions != null) {
            revisions.forEach(Consumer { obj: QldbRevision -> obj.verifyRevisionHash() })
            val computedRevisionsHash = computeRevisionsHash()
            require(
                entriesHashSet.contains(
                    ByteBuffer.wrap(computedRevisionsHash).asReadOnlyBuffer()
                )
            ) { "Block revisions list hash is not contained in the QLDB block entries hash list." }
        }
        val computedEntriesHash = computeEntriesHash()
        require(
            Arrays.equals(
                computedEntriesHash,
                entriesHash
            )
        ) { "Computed entries hash does not match entries hash provided in the block." }
        val computedBlockHash = Verifier.dot(computedEntriesHash, previousBlockHash)
        require(
            Arrays.equals(
                computedBlockHash,
                blockHash
            )
        ) { "Computed block hash does not match block hash provided in the block." }
    }

    private fun computeTransactionInfoHash(): ByteArray {
        return try {
            QldbIonUtils.hashIonValue(
                Constants.MAPPER.writeValueAsIonValue(
                    transactionInfo
                )
            )
        } catch (e: IOException) {
            throw IllegalArgumentException("Could not compute transactionInfo hash to verify block hash.", e)
        }
    }

    private fun computeRevisionsHash(): ByteArray {
        return Verifier.calculateMerkleTreeRootHash(revisions.stream().map { obj: QldbRevision -> obj.hash }
            .collect(Collectors.toList()))
    }

    private fun computeEntriesHash(): ByteArray {
        return Verifier.calculateMerkleTreeRootHash(Arrays.asList(*entriesHashList))
    }

    companion object {
        private val log = LoggerFactory.getLogger(JournalBlock::class.java)
    }
}