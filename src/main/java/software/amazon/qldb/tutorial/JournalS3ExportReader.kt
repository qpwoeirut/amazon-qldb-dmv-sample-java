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

import com.amazon.ion.*
import com.amazon.ion.system.IonReaderBuilder
import com.amazon.ion.system.IonSystemBuilder
import com.amazonaws.services.qldb.model.DescribeJournalS3ExportResult
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.ListObjectsV2Request
import com.amazonaws.services.s3.model.S3Object
import com.amazonaws.services.s3.model.S3ObjectSummary
import org.slf4j.LoggerFactory
import software.amazon.qldb.tutorial.qldb.JournalBlock
import java.io.IOException
import java.util.*
import java.util.function.Consumer

/**
 * Given bucket, prefix and exportId, read the contents of the export and return
 * a list of [JournalBlock].
 *
 * This code expects that you have AWS credentials setup per:
 * http://docs.aws.amazon.com/java-sdk/latest/developer-guide/setup-credentials.html
 */
object JournalS3ExportReader {
    val log = LoggerFactory.getLogger(JournalS3ExportReader::class.java)
    private val SYSTEM = IonSystemBuilder.standard().build()

    /**
     * Read the S3 export within a [JournalBlock].
     *
     * @param describeJournalS3ExportResult
     * The result from the QLDB database describing a journal export.
     * @param amazonS3
     * The low level S3 client.
     * @return a list of [JournalBlock].
     */
    @JvmStatic
    fun readExport(
        describeJournalS3ExportResult: DescribeJournalS3ExportResult,
        amazonS3: AmazonS3
    ): List<JournalBlock> {
        val exportConfiguration = describeJournalS3ExportResult.exportDescription.s3ExportConfiguration
        val listObjectsRequest = ListObjectsV2Request()
            .withBucketName(exportConfiguration.bucket)
            .withPrefix(exportConfiguration.prefix)
        val listObjectsV2Result = amazonS3.listObjectsV2(listObjectsRequest)
        log.info("Found the following objects for list from s3: ")
        listObjectsV2Result.objectSummaries
            .forEach(Consumer { s3ObjectSummary: S3ObjectSummary -> log.info(s3ObjectSummary.key) })

        // Validate initial manifest file was written.
        val expectedManifestKey = exportConfiguration.prefix +
                describeJournalS3ExportResult.exportDescription.exportId + ".started" + ".manifest"
        val initialManifestKey = listObjectsV2Result
            .objectSummaries
            .stream()
            .filter { s3ObjectSummary: S3ObjectSummary ->
                s3ObjectSummary.key.equals(
                    expectedManifestKey,
                    ignoreCase = true
                )
            }
            .map { obj: S3ObjectSummary -> obj.key }
            .findFirst().orElseThrow { IllegalStateException("Initial manifest not found.") }
        log.info("Found the initial manifest with key $initialManifestKey")

        // Find the final manifest file, it should contain the exportId in it.
        val completedManifestFileKey = listObjectsV2Result
            .objectSummaries
            .stream()
            .filter { s3ObjectSummary: S3ObjectSummary ->
                (s3ObjectSummary.key.endsWith("completed.manifest")
                        && s3ObjectSummary
                    .key
                    .contains(describeJournalS3ExportResult.exportDescription.exportId))
            }
            .map { obj: S3ObjectSummary -> obj.key }
            .findFirst().orElseThrow { IllegalStateException("Completed manifest not found.") }
        log.info("Found the completed manifest with key $completedManifestFileKey")

        // Read manifest file to find data file keys.
        val completedManifestObject = amazonS3.getObject(exportConfiguration.bucket, completedManifestFileKey)
        val dataFileKeys = getDataFileKeysFromManifest(completedManifestObject)
        log.info("Found the following keys in the manifest files: $dataFileKeys")
        val journalBlocks: MutableList<JournalBlock> = ArrayList()
        for (key in dataFileKeys) {
            log.info("Reading file with S3 key " + key + " from bucket: " + exportConfiguration.bucket)
            val s3Object = amazonS3.getObject(exportConfiguration.bucket, key)
            val blocks = getJournalBlocks(s3Object)
            compareKeyWithContentRange(key, blocks[0], blocks[blocks.size - 1])
            journalBlocks.addAll(blocks)
        }
        return journalBlocks
    }

    /**
     * Compares the expected block range, derived from File Key, with the actual object content.
     *
     * @param fileKey
     * The key of data file containing the chunk of [JournalBlock].
     * The fileKey pattern is `[strandId].[firstSequenceNo]-[lastSequenceNo].ion`.
     * @param firstBlock
     * The first block decoded from the object content.
     * @param lastBlock
     * The last block decoded from the object content.
     * @throws IllegalStateException if either of the [JournalBlock]s' sequenceNo does not match the expected number.
     */
    private fun compareKeyWithContentRange(
        fileKey: String, firstBlock: JournalBlock,
        lastBlock: JournalBlock
    ) {
        // the key pattern is [strandId].[firstSequenceNo]-[lastSequenceNo].ion
        val sequenceNoRange = fileKey.split("\\.").toTypedArray()[1]
        val keyTokens = sequenceNoRange.split("-").toTypedArray()
        val startSequenceNo = java.lang.Long.valueOf(keyTokens[0])
        val lastSequenceNo = java.lang.Long.valueOf(keyTokens[1])

        // compare the first sequenceNo of the fileKey to the sequenceNo of the first block.
        // block address is [strandId]/[sequenceNo]
        check(firstBlock.blockAddress.sequenceNo == startSequenceNo) { "Expected first block SequenceNo to be $startSequenceNo" }

        // compare the second sequenceNo of the fileKey to the sequenceNo of the last block.
        check(lastBlock.blockAddress.sequenceNo == lastSequenceNo) { "Expected last block SequenceNo to be $lastSequenceNo" }
    }

    /**
     * Retrieve a list of [JournalBlock] from the given [S3Object].
     *
     * @param s3Object
     * A [S3Object].
     * @return a list of [JournalBlock].
     * @throws IllegalStateException if invalid IonType is found in the S3 Object.
     */
    private fun getJournalBlocks(s3Object: S3Object): List<JournalBlock> {
        val ionReader = SYSTEM.newReader(s3Object.objectContent)
        val journalBlocks: MutableList<JournalBlock> = ArrayList()
        // data files contain list of blocks
        while (ionReader.next() != null) {
            check(ionReader.type == IonType.STRUCT) { "Expected ion STRUCT but found " + ionReader.type }
            try {
                journalBlocks.add(Constants.MAPPER.readValue(SYSTEM.newValue(ionReader), JournalBlock::class.java))
            } catch (ioe: IOException) {
                throw IllegalStateException(ioe)
            }
        }
        log.info("Found " + journalBlocks.size + " blocks(s) from data file - " + s3Object.key)
        return journalBlocks
    }

    /**
     * Given the S3Object to the completed manifest file, return the keys
     * which are part of this export request.
     *
     * @param s3Object
     * A [S3Object].
     * @return a list of data file keys containing the chunk of [JournalBlock].
     */
    private fun getDataFileKeysFromManifest(s3Object: S3Object): List<String> {
        val ionReader = IonReaderBuilder.standard().build(s3Object.objectContent)
        ionReader.next() // Read the data
        val keys: MutableList<String> = ArrayList()
        val ionStruct = SYSTEM.newValue(ionReader) as IonStruct
        val ionKeysList = ionStruct["keys"] as IonList
        ionKeysList.forEach(Consumer { key: IonValue -> keys.add((key as IonString).stringValue()) })
        return keys
    }
}