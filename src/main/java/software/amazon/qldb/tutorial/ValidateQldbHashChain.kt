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
package software.amazon.qldb.tutorial

import com.amazonaws.services.qldb.model.S3EncryptionConfiguration
import com.amazonaws.services.qldb.model.S3ObjectEncryptionType
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder
import com.amazonaws.services.securitytoken.model.GetCallerIdentityRequest
import org.slf4j.LoggerFactory
import software.amazon.qldb.tutorial.DescribeJournalExport.describeExport
import software.amazon.qldb.tutorial.ExportJournal.createJournalExportAndAwaitCompletion
import software.amazon.qldb.tutorial.JournalS3ExportReader.readExport
import software.amazon.qldb.tutorial.qldb.JournalBlock
import java.time.Instant
import java.util.*

/**
 * Validate the hash chain of a QLDB ledger by stepping through its S3 export.
 *
 * This code accepts an exportId as an argument, if exportId is passed the code
 * will use that or request QLDB to generate a new export to perform QLDB hash
 * chain validation.
 *
 * This code expects that you have AWS credentials setup per:
 * http://docs.aws.amazon.com/java-sdk/latest/developer-guide/setup-credentials.html
 */
object ValidateQldbHashChain {
    val log = LoggerFactory.getLogger(ValidateQldbHashChain::class.java)
    private const val TIME_SKEW = 20

    /**
     * Export journal contents to a S3 bucket.
     *
     * @return the ExportId of the journal export.
     * @throws InterruptedException if the thread is interrupted while waiting for export to complete.
     */
    @Throws(InterruptedException::class)
    private fun createExport(): String {
        val accountId = AWSSecurityTokenServiceClientBuilder.defaultClient()
            .getCallerIdentity(GetCallerIdentityRequest()).account
        val bucketName = Constants.JOURNAL_EXPORT_S3_BUCKET_NAME_PREFIX + "-" + accountId
        val prefix = Constants.LEDGER_NAME + "-" + Instant.now().epochSecond + "/"
        val encryptionConfiguration = S3EncryptionConfiguration()
            .withObjectEncryptionType(S3ObjectEncryptionType.SSE_S3)
        val exportJournalToS3Result = createJournalExportAndAwaitCompletion(
            Constants.LEDGER_NAME,
            bucketName, prefix, null, encryptionConfiguration, ExportJournal.DEFAULT_EXPORT_TIMEOUT_MS
        )
        return exportJournalToS3Result.exportId
    }

    /**
     * Validates that the chain hash on the [JournalBlock] is valid.
     *
     * @param journalBlocks
     * [JournalBlock] containing hashes to validate.
     * @throws IllegalStateException if previous block hash does not match.
     */
    @JvmStatic
    fun verify(journalBlocks: List<JournalBlock>) {
        if (journalBlocks.isEmpty()) {
            return
        }
        journalBlocks.stream().reduce(null) { previousJournalBlock: JournalBlock?, journalBlock: JournalBlock ->
            journalBlock.verifyBlockHash()
            if (previousJournalBlock == null) {
                return@reduce journalBlock
            }
            check(
                Arrays.equals(
                    previousJournalBlock.blockHash,
                    journalBlock.previousBlockHash
                )
            ) { "Previous block hash doesn't match." }
            val blockHash = Verifier.dot(journalBlock.entriesHash, previousJournalBlock.blockHash)
            check(Arrays.equals(blockHash, journalBlock.blockHash)) {
                ("Block hash doesn't match entriesHash dot previousBlockHash, the chain is "
                        + "broken.")
            }
            journalBlock
        }
    }

    @Throws(InterruptedException::class)
    @JvmStatic
    fun main(args: Array<String>) {
        try {
            val exportId: String
            if (args.size == 1) {
                exportId = args[0]
                log.info("Validating QLDB hash chain for exportId: $exportId")
            } else {
                log.info("Requesting QLDB to create an export.")
                exportId = createExport()
            }
            val journalBlocks: List<JournalBlock> = readExport(
                describeExport(
                    Constants.LEDGER_NAME,
                    exportId
                ), AmazonS3ClientBuilder.defaultClient()
            )
            verify(journalBlocks)
        } catch (e: Exception) {
            log.error("Unable to perform hash chain verification.", e)
            throw e
        }
    }
}