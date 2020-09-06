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

import com.amazon.ion.IonStruct
import com.amazon.ion.system.IonReaderBuilder
import com.amazon.ion.system.IonSystemBuilder
import com.amazonaws.services.qldb.model.GetRevisionRequest
import com.amazonaws.services.qldb.model.GetRevisionResult
import com.amazonaws.services.qldb.model.ValueHolder
import org.slf4j.LoggerFactory
import software.amazon.qldb.QldbDriver
import software.amazon.qldb.TransactionExecutor
import software.amazon.qldb.tutorial.ConnectToLedger.driver
import software.amazon.qldb.tutorial.GetDigest.getDigest
import software.amazon.qldb.tutorial.model.SampleData
import software.amazon.qldb.tutorial.qldb.BlockAddress
import software.amazon.qldb.tutorial.qldb.QldbRevision.Companion.fromIon
import software.amazon.qldb.tutorial.qldb.QldbStringUtils.toUnredactedString
import java.io.ByteArrayOutputStream
import java.io.IOException
import java.util.*

/**
 * Verify the integrity of a document revision in a QLDB ledger.
 *
 * This code expects that you have AWS credentials setup per:
 * http://docs.aws.amazon.com/java-sdk/latest/developer-guide/setup-credentials.html
 */
object GetRevision {
    val log = LoggerFactory.getLogger(GetRevision::class.java)
    val client = CreateLedger.client

    private val SYSTEM = IonSystemBuilder.standard().build()

    @Throws(Exception::class)
    @JvmStatic
    fun main(args: Array<String>) {
        val vin = SampleData.REGISTRATIONS[0].vin
        verifyRegistration(driver, Constants.LEDGER_NAME, vin)
    }

    /**
     * Verify each version of the registration for the given VIN.
     *
     * @param driver
     * A QLDB driver.
     * @param ledgerName
     * The ledger to get digest from.
     * @param vin
     * VIN to query the revision history of a specific registration with.
     * @throws Exception if failed to verify digests.
     * @throws AssertionError if document revision verification failed.
     */
    @Throws(Exception::class)
    fun verifyRegistration(driver: QldbDriver, ledgerName: String, vin: String) {
        log.info(String.format("Let's verify the registration with VIN=%s, in ledger=%s.", vin, ledgerName))
        try {
            log.info("First, let's get a digest.")
            val digestResult = getDigest(ledgerName)
            val digestTipAddress = digestResult.digestTipAddress
            val digestBytes = Verifier.convertByteBufferToByteArray(digestResult.digest)
            log.info(
                "Got a ledger digest. Digest end address={}, digest={}.",
                toUnredactedString(digestTipAddress),
                Verifier.toBase64(digestBytes)
            )
            log.info(
                String.format(
                    "Next, let's query the registration with VIN=%s. "
                            + "Then we can verify each version of the registration.", vin
                )
            )
            val documentsWithMetadataList: MutableList<IonStruct> = ArrayList()
            driver.execute { txn: TransactionExecutor ->
                documentsWithMetadataList.addAll(
                    queryRegistrationsByVin(
                        txn,
                        vin
                    )
                )
            }
            log.info("Registrations queried successfully!")
            log.info(
                String.format(
                    "Found %s revisions of the registration with VIN=%s.",
                    documentsWithMetadataList.size, vin
                )
            )
            for (ionStruct in documentsWithMetadataList) {
                val document = fromIon(ionStruct)
                log.info(String.format("Let's verify the document: %s", document))
                log.info("Let's get a proof for the document.")
                val proofResult = getRevision(
                    ledgerName,
                    document.metadata.id,
                    digestTipAddress,
                    document.blockAddress
                )
                val proof = Constants.MAPPER.writeValueAsIonValue(proofResult.proof)
                val reader = IonReaderBuilder.standard().build(proof)
                reader.next()
                val baos = ByteArrayOutputStream()
                val writer = SYSTEM.newBinaryWriter(baos)
                writer.writeValue(reader)
                writer.close()
                baos.flush()
                baos.close()
                val byteProof = baos.toByteArray()
                log.info(String.format("Got back a proof: %s", Verifier.toBase64(byteProof)))
                var verified = Verifier.verify(
                    document.hash,
                    digestBytes,
                    proofResult.proof.ionText
                )
                if (!verified) {
                    throw AssertionError("Document revision is not verified!")
                } else {
                    log.info("Success! The document is verified")
                }
                val alteredDigest = Verifier.flipRandomBit(digestBytes)
                log.info(
                    String.format(
                        "Flipping one bit in the digest and assert that the document is NOT verified. "
                                + "The altered digest is: %s", Verifier.toBase64(alteredDigest)
                    )
                )
                verified = Verifier.verify(
                    document.hash,
                    alteredDigest,
                    proofResult.proof.ionText
                )
                if (verified) {
                    throw AssertionError("Expected document to not be verified against altered digest.")
                } else {
                    log.info("Success! As expected flipping a bit in the digest causes verification to fail.")
                }
                val alteredDocumentHash = Verifier.flipRandomBit(document.hash)
                log.info(
                    String.format(
                        "Flipping one bit in the document's hash and assert that it is NOT verified. "
                                + "The altered document hash is: %s.", Verifier.toBase64(alteredDocumentHash)
                    )
                )
                verified = Verifier.verify(
                    alteredDocumentHash,
                    digestBytes,
                    proofResult.proof.ionText
                )
                if (verified) {
                    throw AssertionError("Expected altered document hash to not be verified against digest.")
                } else {
                    log.info("Success! As expected flipping a bit in the document hash causes verification to fail.")
                }
            }
        } catch (e: Exception) {
            log.error("Failed to verify digests.", e)
            throw e
        }
        log.info(String.format("Finished verifying the registration with VIN=%s in ledger=%s.", vin, ledgerName))
    }

    /**
     * Get the revision of a particular document specified by the given document ID and block address.
     *
     * @param ledgerName
     * Name of the ledger containing the document.
     * @param documentId
     * Unique ID for the document to be verified, contained in the committed view of the document.
     * @param digestTipAddress
     * The latest block location covered by the digest.
     * @param blockAddress
     * The location of the block to request.
     * @return the requested revision.
     */
    fun getRevision(
        ledgerName: String, documentId: String,
        digestTipAddress: ValueHolder, blockAddress: BlockAddress
    ): GetRevisionResult {
        return try {
            val request = GetRevisionRequest()
                .withName(ledgerName)
                .withDigestTipAddress(digestTipAddress)
                .withBlockAddress(
                    ValueHolder().withIonText(
                        Constants.MAPPER.writeValueAsIonValue(blockAddress)
                            .toString()
                    )
                )
                .withDocumentId(documentId)
            client.getRevision(request)
        } catch (ioe: IOException) {
            throw IllegalStateException(ioe)
        }
    }

    /**
     * Query the registration history for the given VIN.
     *
     * @param txn
     * The [TransactionExecutor] for lambda execute.
     * @param vin
     * The unique VIN to query.
     * @return a list of [IonStruct] representing the registration history.
     * @throws IllegalStateException if failed to convert parameters into [IonValue]
     */
    fun queryRegistrationsByVin(txn: TransactionExecutor, vin: String): List<IonStruct> {
        log.info(String.format("Let's query the 'VehicleRegistration' table for VIN: %s...", vin))
        log.info("Let's query the 'VehicleRegistration' table for VIN: {}...", vin)
        val query = String.format(
            "SELECT * FROM _ql_committed_%s WHERE data.VIN = ?",
            Constants.VEHICLE_REGISTRATION_TABLE_NAME
        )
        return try {
            val parameters = listOf(Constants.MAPPER.writeValueAsIonValue(vin))
            val result = txn.execute(query, parameters)
            val list = ScanTable.toIonStructs(result)
            log.info(String.format("Found %d document(s)!", list.size))
            list
        } catch (ioe: IOException) {
            throw IllegalStateException(ioe)
        }
    }
}