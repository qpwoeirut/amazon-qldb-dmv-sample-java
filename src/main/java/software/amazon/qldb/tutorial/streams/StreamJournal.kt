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
package software.amazon.qldb.tutorial.streams

import com.amazon.ion.IonStruct
import com.amazon.ion.IonWriter
import com.amazon.ion.system.IonReaderBuilder
import com.amazon.ion.system.IonTextWriterBuilder
import com.amazonaws.auth.AWSCredentialsProvider
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import com.amazonaws.services.identitymanagement.AmazonIdentityManagementClientBuilder
import com.amazonaws.services.identitymanagement.model.*
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShutdownReason
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker
import com.amazonaws.services.kinesis.model.DescribeStreamRequest
import com.amazonaws.services.kinesis.model.Record
import com.amazonaws.services.kinesis.model.ResourceNotFoundException
import com.amazonaws.services.qldb.AmazonQLDBClientBuilder
import com.amazonaws.services.qldb.model.*
import org.slf4j.LoggerFactory
import software.amazon.qldb.tutorial.Constants
import software.amazon.qldb.tutorial.Constants.LEDGER_NAME
import software.amazon.qldb.tutorial.Constants.STREAM_NAME
import software.amazon.qldb.tutorial.CreateLedger.create
import software.amazon.qldb.tutorial.CreateLedger.waitForActive
import software.amazon.qldb.tutorial.CreateTable
import software.amazon.qldb.tutorial.DeleteLedger.delete
import software.amazon.qldb.tutorial.DeleteLedger.waitForDeleted
import software.amazon.qldb.tutorial.DeletionProtection.setDeletionProtection
import software.amazon.qldb.tutorial.InsertDocument
import software.amazon.qldb.tutorial.ValidateQldbHashChain.verify
import software.amazon.qldb.tutorial.model.SampleData
import software.amazon.qldb.tutorial.model.streams.BlockSummaryRecord
import software.amazon.qldb.tutorial.model.streams.RevisionDetailsRecord
import software.amazon.qldb.tutorial.model.streams.StreamRecord
import software.amazon.qldb.tutorial.qldb.JournalBlock
import software.amazon.qldb.tutorial.qldb.QldbRevision
import software.amazon.qldb.tutorial.qldb.QldbRevision.Companion.fromIon
import software.amazon.qldb.tutorial.streams.StreamJournal.RevisionProcessor
import java.io.IOException
import java.nio.ByteBuffer
import java.time.Duration
import java.time.Instant
import java.util.*
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.function.Consumer
import java.util.function.Function
import java.util.stream.Collectors
import kotlin.system.exitProcess

/**
 * Demonstrates the QLDB stream functionality.
 *
 *
 * In this tutorial, we will create a Ledger and a stream for that Ledger. We
 * will then start a stream reader and wait for few documents to be inserted
 * that will be pulled from the Kinesis stream into the [.recordBuffer]
 * and the logs and for this tutorial code using the Amazon Kinesis Client
 * library
 *
 * @see [Amazon Kinesis Client library](https://github.com/awslabs/amazon-kinesis-client)
 */
object StreamJournal {
    /**
     * AWS service clients used throughout the tutorial code.
     */
    private val kinesis = AmazonKinesisClientBuilder.defaultClient()
    private val iam = AmazonIdentityManagementClientBuilder.defaultClient()
    private val qldb = AmazonQLDBClientBuilder.defaultClient()
    private val credentialsProvider: AWSCredentialsProvider = DefaultAWSCredentialsProviderChain.getInstance()

    /**
     * Shared variables to make the tutorial code easy to follow.
     */
    private const val ledgerName: String = LEDGER_NAME
    private const val streamName: String = STREAM_NAME
    private var streamId: String? = null
    private var kdsArn: String? = null
    private var roleArn: String? = null
    private var kdsName: String? = null
    private var kdsRoleName: String? = null
    private var kdsPolicyName: String? = null
    private val kclConfig: KinesisClientLibConfiguration
    private val kdsReader: Worker
    private const val regionName = "us-east-1"
    private var exclusiveEndTime: Date? = null
    private var isAggregationEnabled = false
    private var bufferCapacity = 0
    private val waiter: CompletableFuture<Void?>
    private val recordBuffer: MutableList<StreamRecord> = ArrayList()
    private var areAllRecordsFound: Function<StreamRecord, Boolean>? = null
    private const val STREAM_ROLE_KINESIS_STATEMENT_TEMPLATE = "{" +
            "   \"Sid\": \"QLDBStreamKinesisPermissions\"," +
            "   \"Action\": [\"kinesis:PutRecord\", \"kinesis:PutRecords\", " +
            "\"kinesis:DescribeStream\", \"kinesis:ListShards\"]," +
            "   \"Effect\": \"Allow\"," +
            "   \"Resource\": \"{kdsArn}\"" +
            "}"
    private const val POLICY_TEMPLATE = "{" +
            "   \"Version\" : \"2012-10-17\"," +
            "   \"Statement\": [ {statements} ]" +
            "}"
    private val ASSUME_ROLE_POLICY = POLICY_TEMPLATE.replace(
        "{statements}",
        "{" +
                "   \"Effect\": \"Allow\"," +
                "   \"Principal\": {" +
                "       \"Service\": [\"qldb.amazonaws.com\"]" +
                "   }," +
                "   \"Action\": [ \"sts:AssumeRole\" ]" +
                "}"
    )
    private val reader = Constants.MAPPER.readerFor(
        StreamRecord::class.java
    )
    private val log = LoggerFactory.getLogger(StreamJournal::class.java)
    private const val MAX_RETRIES = 60

    /**
     * Runs the tutorial.
     *
     * @param args not required.
     */
    @JvmStatic
    fun main(args: Array<String>) {
        try {
            runStreamJournalTutorial()
            log.info("You can use the AWS CLI or Console to browse the resources that were created.")
        } catch (ex: Exception) {
            log.error("Something went wrong.", ex)
        } finally {
            log.info("Press Enter to clean up and exit.")
            try {
                System.`in`.read()
            } catch (ignore: Exception) {
            }
            log.info("Starting cleanup...")
            cleanupQldbResources()
            cleanupKinesisResources()
            exitProcess(0)
        }
    }

    /**
     * We will stream 24 records to Kinesis:
     * 1) Control record with CREATED status
     * 2) 2 Summary records
     * 3) 20 Revision records
     * 4) Control record with COMPLETED status (if the stream is bounded)
     *
     * @param isBounded true if the stream has an exclusive end time.
     * @return the number of expected records.
     */
    private fun numberOfExpectedRecords(isBounded: Boolean): Int {
        val baseNumber = SampleData.LICENSES.size +
                SampleData.PEOPLE.size +
                SampleData.REGISTRATIONS.size +
                SampleData.VEHICLES.size +  // records of Block Summary
                2 +  // CREATED Control Record
                1

        // Bounded Stream will contain also COMPLETED Control Record
        return if (isBounded) baseNumber + 1 else baseNumber
    }

    /**
     * Initialize the tutorial code.
     */
    init {
        kdsName = "$ledgerName-kinesis-stream"
        kdsRoleName = "$ledgerName-stream-role"
        kdsPolicyName = "$ledgerName-stream-policy"
        kclConfig = KinesisClientLibConfiguration(ledgerName, kdsName, credentialsProvider, "tutorial")
            .withInitialPositionInStream(InitialPositionInStream.TRIM_HORIZON)
            .withRegionName(regionName)
        kdsReader = Worker.Builder()
            .recordProcessorFactory(RevisionProcessorFactory())
            .config(kclConfig)
            .build()
        isAggregationEnabled = true
        bufferCapacity = numberOfExpectedRecords(exclusiveEndTime != null)
        areAllRecordsFound = Function { recordBuffer.size == bufferCapacity }
        waiter = CompletableFuture()
    }

    /**
     * Runs the tutorial code.
     *
     * @throws Exception is thrown when something goes wrong in the tutorial.
     */
    @Throws(Exception::class)
    fun runStreamJournalTutorial() {
        createLedger()
        createTables()
        insertDocuments()
        createQldbStream()
        startStreamReader()
        waiter.get()
        if (exclusiveEndTime != null) {
            waitForQldbStreamCompletion()
        }
        log.info("Buffered {} records so far.", recordBuffer.size)
        validateStreamRecordsHashChain()
    }

    /**
     * Creates a ledger.
     *
     * @throws Exception if the ledger creation fails.
     */
    @Throws(Exception::class)
    fun createLedger() {
        create(ledgerName)
        waitForActive(ledgerName)
    }

    /**
     * Creates a few tables in the ledger created using [.createLedger].
     */
    private fun createTables() {
        CreateTable.main()
    }

    private fun insertDocuments() {
        InsertDocument.main()
    }

    /**
     * Create a QLDB Stream.
     *
     * @return the QLDB Stream description.
     * @throws InterruptedException if the thread is interrupted while waiting
     * for stream creation.
     */
    @Throws(InterruptedException::class)
    fun createQldbStream(): JournalKinesisStreamDescription {
        log.info("Creating Kinesis data stream with name: '{}'...", kdsName)
        createKdsIfNotExists()
        log.info("Creating QLDB stream...")
        var request = StreamJournalToKinesisRequest()
            .withKinesisConfiguration(kdsConfig)
            .withInclusiveStartTime(Date.from(Instant.now().minus(Duration.ofDays(1))))
            .withRoleArn(orCreateKdsRole)
            .withLedgerName(ledgerName)
            .withStreamName(streamName)
        if (exclusiveEndTime != null) {
            request = request.withExclusiveEndTime(exclusiveEndTime)
        }
        val result = qldb.streamJournalToKinesis(request)
        streamId = result.streamId
        val describeResult = describeQldbStream()
        log.info(
            "Created QLDB stream: {} Current status: {}.",
            streamId,
            describeResult.stream.status
        )
        return describeResult.stream
    }

    /**
     * Create a Kinesis Data Stream to stream Journal data to Kinesis.
     */
    private fun createKdsIfNotExists() {
        try {
            log.info("Check if Kinesis Data Stream already exists.")
            val describeStreamRequest = DescribeStreamRequest()
                .withStreamName(kdsName)
            val describeStreamResponse = kinesis.describeStream(describeStreamRequest)
            log.info("Describe stream response: $describeStreamResponse")
            kdsArn = describeStreamResponse.streamDescription.streamARN
            val streamStatus = describeStreamResponse.streamDescription.streamStatus
            if (streamStatus == "ACTIVE") {
                log.info("Kinesis Data Stream is already Active.")
                return
            }
        } catch (e: ResourceNotFoundException) {
            kinesis.createStream(kdsName, 1)
        }
        waitForKdsActivation()
    }

    /**
     * Wait for Kinesis Data Stream completion.
     */
    private fun waitForQldbStreamCompletion() {
        val describeStreamRequest = DescribeJournalKinesisStreamRequest()
            .withStreamId(streamId)
            .withLedgerName(ledgerName)
        var retries = 0
        while (retries < MAX_RETRIES) {
            val describeStreamResponse = qldb.describeJournalKinesisStream(describeStreamRequest)
            val streamStatus = describeStreamResponse.stream.status
            log.info("Waiting for Stream Completion. Current streamStatus: {}.", streamStatus)
            if (streamStatus == "COMPLETED") {
                break
            }
            try {
                Thread.sleep(1000)
            } catch (ignore: Exception) {
            }
            retries++
        }
        if (retries >= MAX_RETRIES) {
            throw RuntimeException("Kinesis Stream with name $kdsName never went completed.")
        }
    }

    /**
     * Wait for Kinesis Data Stream activation.
     */
    private fun waitForKdsActivation() {
        log.info("Waiting for Kinesis Stream to become Active.")
        val describeStreamRequest = DescribeStreamRequest()
            .withStreamName(kdsName)
        var retries = 0
        while (retries < MAX_RETRIES) {
            try {
                log.info("Sleeping for 5 sec before polling Kinesis Stream status.")
                Thread.sleep(5 * 1000.toLong())
            } catch (ignore: Exception) {
            }
            val describeStreamResponse = kinesis.describeStream(describeStreamRequest)
            kdsArn = describeStreamResponse.streamDescription.streamARN
            val streamStatus = describeStreamResponse.streamDescription.streamStatus
            if (streamStatus == "ACTIVE") {
                break
            }
            log.info("Still waiting for Kinesis Stream to become Active. Current streamStatus: {}.", streamStatus)
            try {
                Thread.sleep(1000)
            } catch (ignore: Exception) {
            }
            retries++
        }
        if (retries >= MAX_RETRIES) {
            throw RuntimeException("Kinesis Stream with name $kdsName never went active")
        }
    }

    /**
     * Create role and attaches policy to allow put records in KDS.
     *
     * @return roleArn for Kinesis Data Streams.
     * @throws InterruptedException if the thread is interrupted while creating
     * the KDS role.
     */
    @get:Throws(InterruptedException::class)
    val orCreateKdsRole: String?
        get() {
            try {
                roleArn = iam.getRole(GetRoleRequest().withRoleName(kdsRoleName)).role.arn
                try {
                    iam.getRolePolicy(GetRolePolicyRequest().withPolicyName(kdsPolicyName).withRoleName(kdsRoleName))
                } catch (e: NoSuchEntityException) {
                    attachPolicyToKdsRole()
                    Thread.sleep(20 * 1000.toLong())
                }
            } catch (e: NoSuchEntityException) {
                log.info(
                    "The provided role doesn't exist. Creating the role with name: {}. Please wait...",
                    kdsRoleName
                )
                val createRole = CreateRoleRequest()
                    .withRoleName(kdsRoleName)
                    .withAssumeRolePolicyDocument(ASSUME_ROLE_POLICY)
                roleArn = iam.createRole(createRole).role.arn
                attachPolicyToKdsRole()
                Thread.sleep(20 * 1000.toLong())
            }
            return roleArn
        }

    private fun attachPolicyToKdsRole() {
        val rolePolicy = POLICY_TEMPLATE
            .replace("{statements}", STREAM_ROLE_KINESIS_STATEMENT_TEMPLATE)
            .replace("{kdsArn}", kdsArn!!)
        val putPolicy = PutRolePolicyRequest()
            .withRoleName(kdsRoleName)
            .withPolicyName(kdsPolicyName)
            .withPolicyDocument(rolePolicy)
        iam.putRolePolicy(putPolicy)
    }// By default, aggregationEnabled is true so it can be specified only if needed.

    /**
     * Generate the [KinesisConfiguration] for the QLDB stream.
     *
     * @return [KinesisConfiguration] for the QLDB stream.
     */
    private val kdsConfig: KinesisConfiguration
        get() {
            var kinesisConfiguration = KinesisConfiguration().withStreamArn(kdsArn)

            // By default, aggregationEnabled is true so it can be specified only if needed.
            if (!isAggregationEnabled) {
                kinesisConfiguration = kinesisConfiguration.withAggregationEnabled(isAggregationEnabled)
            }
            return kinesisConfiguration
        }

    /**
     * List QLDB streams for the ledger.
     *
     * @return map of stream Id to description for the ledger's QLDB streams.
     */
    fun listQldbStreamsForLedger(): Map<String, JournalKinesisStreamDescription> {
        val streams: MutableMap<String, JournalKinesisStreamDescription> = HashMap()
        var nextToken: String? = null
        do {
            val listRequest = ListJournalKinesisStreamsForLedgerRequest()
                .withLedgerName(ledgerName)
                .withNextToken(nextToken)
            val listResult = qldb.listJournalKinesisStreamsForLedger(listRequest)
            listResult.streams.forEach(Consumer { streamDescription: JournalKinesisStreamDescription ->
                streams[streamDescription.streamId] = streamDescription
            })
            nextToken = listResult.nextToken
        } while (nextToken != null)
        return streams
    }

    /**
     * Describe the QLDB stream.
     *
     * @return description of the QLDB stream.
     */
    private fun describeQldbStream(): DescribeJournalKinesisStreamResult {
        val describeStreamRequest = DescribeJournalKinesisStreamRequest()
            .withStreamId(streamId)
            .withLedgerName(ledgerName)
        return qldb.describeJournalKinesisStream(describeStreamRequest)
    }

    /**
     * Starts the stream reader using Kinesis client library.
     */
    fun startStreamReader() {
        log.info("Starting stream reader...")
        Executors.newSingleThreadExecutor().submit { kdsReader.run() }
    }

    /**
     * Clean up QLDB resources used by the tutorial code.
     */
    private fun cleanupQldbResources() {
        stopStreamReader()
        if (exclusiveEndTime == null) {
            cancelQldbStream()
        }
        deleteLedger()
    }

    /**
     * Cancel the QLDB stream.
     */
    private fun cancelQldbStream() {
        if (null == streamId) {
            return
        }
        try {
            val request = CancelJournalKinesisStreamRequest()
                .withLedgerName(ledgerName)
                .withStreamId(streamId)
            qldb.cancelJournalKinesisStream(request)
            log.info("QLDB stream was cancelled.")
        } catch (ex: com.amazonaws.services.qldb.model.ResourceNotFoundException) {
            log.info("No QLDB stream to cancel.")
        } catch (ex: Exception) {
            log.warn("Error cancelling QLDB stream.", ex)
        }
    }

    /**
     * Stops the Stream Reader.
     */
    private fun stopStreamReader() {
        try {
            kdsReader.startGracefulShutdown()[30, TimeUnit.SECONDS]
            log.info("Stream reader was stopped.")
        } catch (ex: Exception) {
            log.warn("Error stopping Stream reader.", ex)
        }
    }

    private fun validateStreamRecordsHashChain() {
        val journalBlocks = streamRecordsToJournalBlocks()
        verify(journalBlocks)
    }

    private fun streamRecordsToJournalBlocks(): List<JournalBlock> {
        val revisionsByHash: MutableMap<ByteBuffer, QldbRevision> = HashMap()
        recordBuffer.stream()
            .filter { record: StreamRecord -> record.recordType == "REVISION_DETAILS" }
            .forEach { record: StreamRecord ->
                try {
                    val revision = (record.payload as RevisionDetailsRecord).revision
                    val revisionHash = revision.hash
                    revisionsByHash[ByteBuffer.wrap(revisionHash).asReadOnlyBuffer()] =
                        fromIon((Constants.MAPPER.writeValueAsIonValue(revision) as IonStruct))
                } catch (e: IOException) {
                    throw IllegalArgumentException("Could not map RevisionDetailsRecord to QldbRevision.", e)
                }
            }
        return recordBuffer.stream()
            .filter { streamRecord: StreamRecord -> streamRecord.recordType == "BLOCK_SUMMARY" }
            .map { streamRecord: StreamRecord -> streamRecord.payload as BlockSummaryRecord }
            .distinct()
            .map { blockSummaryRecord: BlockSummaryRecord ->
                blockSummaryRecordToJournalBlock(
                    blockSummaryRecord,
                    revisionsByHash
                )
            }
            .sorted(Comparator.comparingLong { o: JournalBlock -> o.blockAddress.sequenceNo })
            .collect(Collectors.toList())
    }

    private fun blockSummaryRecordToJournalBlock(
        blockSummaryRecord: BlockSummaryRecord,
        revisionsByHash: Map<ByteBuffer, QldbRevision>
    ): JournalBlock {
        val revisions = blockSummaryRecord.revisionSummaries.map {
            revisionsByHash[ByteBuffer.wrap(it.hash).asReadOnlyBuffer()] ?: error("Cannot find revision by hash $it")
        }

        return JournalBlock(
            blockSummaryRecord.blockAddress,
            blockSummaryRecord.transactionId,
            blockSummaryRecord.blockTimestamp,
            blockSummaryRecord.blockHash,
            blockSummaryRecord.entriesHash,
            blockSummaryRecord.previousBlockHash,
            blockSummaryRecord.entriesHashList,
            blockSummaryRecord.transactionInfo,
            revisions
        )
    }

    /**
     * Deletes the ledger
     */
    private fun deleteLedger() {
        try {
            setDeletionProtection(ledgerName, false)
            delete(ledgerName)
            waitForDeleted(ledgerName)
            log.info("Ledger was deleted.")
        } catch (ex: com.amazonaws.services.qldb.model.ResourceNotFoundException) {
            log.info("No Ledger to delete.")
        } catch (ex: Exception) {
            log.warn("Error deleting Ledger.", ex)
        }
    }

    /**
     * Deletes the KDS.
     */
    fun cleanupKinesisResources() {
        try {
            kinesis.deleteStream(kdsName)
            waitForKdsDeletion()
            log.info("KDS was deleted.")
        } catch (ex: ResourceNotFoundException) {
            log.info("No KDS to delete.")
        } catch (ex: Exception) {
            log.warn("Error deleting KDS.", ex)
        }
    }

    /**
     * Waits for KDS to be deleted.
     */
    private fun waitForKdsDeletion() {
        val describeStreamRequest = DescribeStreamRequest()
        describeStreamRequest.streamName = kdsName
        var retries = 0
        while (retries < MAX_RETRIES) {
            try {
                Thread.sleep(20 * 1000.toLong())
            } catch (ignore: Exception) {
            }
            try {
                kinesis.describeStream(describeStreamRequest)
            } catch (ex: ResourceNotFoundException) {
                break
            }
            try {
                Thread.sleep(1000)
            } catch (ignore: Exception) {
            }
            retries++
        }
        if (retries >= MAX_RETRIES) {
            throw RuntimeException("Kinesis Stream with name $kdsName could not be deleted.")
        }
    }

    /**
     * Factory for [IRecordProcessor]s that process records from KDS.
     */
    private class RevisionProcessorFactory : IRecordProcessorFactory {
        override fun createProcessor(): IRecordProcessor {
            return RevisionProcessor()
        }
    }

    /**
     * Processes records that show up on the KDS.
     */
    private class RevisionProcessor : IRecordProcessor {
        override fun initialize(shardId: String) {
            log.info("Starting RevisionProcessor.")
        }

        override fun processRecords(records: List<Record>, iRecordProcessorCheckpointer: IRecordProcessorCheckpointer) {
            log.info("Processing {} record(s)", records.size)
            records.forEach(Consumer { r: Record ->
                try {
                    log.info("------------------------------------------------")
                    log.info(
                        "Processing Record with Seq: {}, PartitionKey: {}, IonText: {}",
                        r.sequenceNumber, r.partitionKey, toIonText(r.data)
                    )
                    val record = reader.readValue<StreamRecord>(r.data.array())
                    log.info("Record Type: {}, Payload: {}.", record.recordType, record.payload)
                    if (record.qldbStreamArn.contains(streamId!!)) {
                        recordBuffer.add(record)
                        if (areAllRecordsFound!!.apply(record)) {
                            waiter.complete(null)
                        }
                    }
                } catch (e: Exception) {
                    log.warn("Error processing record. ", e)
                }
            })
        }

        @Throws(IOException::class)
        fun rewrite(data: ByteArray?, writer: IonWriter) {
            val reader = IonReaderBuilder.standard().build(data)
            writer.writeValues(reader)
        }

        @Throws(IOException::class)
        private fun toIonText(data: ByteBuffer): String {
            val stringBuilder = StringBuilder()
            IonTextWriterBuilder.minimal().build(stringBuilder)
                .use { prettyWriter -> rewrite(data.array(), prettyWriter) }
            return stringBuilder.toString()
        }

        override fun shutdown(
            iRecordProcessorCheckpointer: IRecordProcessorCheckpointer,
            shutdownReason: ShutdownReason
        ) {
            log.info("Shutting down RevisionProcessor.")
        }

        companion object {
            private val log = LoggerFactory.getLogger(RevisionProcessor::class.java)
        }
    }
}