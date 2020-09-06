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
import com.amazonaws.services.qldb.model.GetBlockRequest
import com.amazonaws.services.qldb.model.GetBlockResult
import com.amazonaws.services.qldb.model.ValueHolder
import org.slf4j.LoggerFactory
import software.amazon.qldb.TransactionExecutor
import software.amazon.qldb.tutorial.ConnectToLedger.driver
import software.amazon.qldb.tutorial.GetDigest.getDigest
import software.amazon.qldb.tutorial.model.SampleData
import software.amazon.qldb.tutorial.qldb.BlockAddress
import software.amazon.qldb.tutorial.qldb.JournalBlock
import software.amazon.qldb.tutorial.qldb.QldbRevision
import software.amazon.qldb.tutorial.qldb.QldbStringUtils.toUnredactedString
import java.io.IOException

/**
 * Get a journal block from a QLDB ledger.
 *
 * After getting the block, we get the digest of the ledger and validate the
 * proof returned in the getBlock response.
 *
 *
 *
 * This code expects that you have AWS credentials setup per:
 * http://docs.aws.amazon.com/java-sdk/latest/developer-guide/setup-credentials.html
 */
object GetBlock {
    val log = LoggerFactory.getLogger(QueryHistory::class.java)
    val client = CreateLedger.client

    @JvmStatic
    fun main(args: Array<String>) {
        try {
            val results = driver.execute<List<IonStruct>> { txn ->
                val vin = SampleData.VEHICLES[1].vin
                GetRevision.queryRegistrationsByVin(txn, vin)
            }
            val blockAddress = Constants.MAPPER.readValue(results[0], QldbRevision::class.java).blockAddress
            verifyBlock(Constants.LEDGER_NAME, blockAddress)
        } catch (e: Exception) {
            log.error("Unable to query vehicle registration by Vin.", e)
        }
    }

    fun getBlock(ledgerName: String, blockAddress: BlockAddress): GetBlockResult {
        log.info("Let's get the block for block address {} of the ledger named {}.", blockAddress, ledgerName)
        return try {
            val request = GetBlockRequest()
                .withName(ledgerName)
                .withBlockAddress(
                    ValueHolder().withIonText(
                        Constants.MAPPER.writeValueAsIonValue(blockAddress).toString()
                    )
                )
            val result = client.getBlock(request)
            log.info("Success. GetBlock: {}.", toUnredactedString(result))
            result
        } catch (ioe: IOException) {
            throw IllegalStateException(ioe)
        }
    }

    fun getBlockWithProof(
        ledgerName: String,
        blockAddress: BlockAddress,
        tipBlockAddress: BlockAddress
    ): GetBlockResult {
        log.info(
            "Let's get the block for block address {}, digest tip address {}, for the ledger named {}.", blockAddress,
            tipBlockAddress, ledgerName
        )
        return try {
            val request = GetBlockRequest()
                .withName(ledgerName)
                .withBlockAddress(
                    ValueHolder().withIonText(
                        Constants.MAPPER.writeValueAsIonValue(blockAddress).toString()
                    )
                )
                .withDigestTipAddress(
                    ValueHolder().withIonText(
                        Constants.MAPPER.writeValueAsIonValue(tipBlockAddress)
                            .toString()
                    )
                )
            val result = client.getBlock(request)
            log.info("Success. GetBlock: {}.", toUnredactedString(result))
            result
        } catch (ioe: IOException) {
            throw IllegalStateException(ioe)
        }
    }

    @Throws(Exception::class)
    fun verifyBlock(ledgerName: String, blockAddress: BlockAddress) {
        log.info("Lets verify blocks for ledger with name={}.", ledgerName)
        try {
            log.info("First, let's get a digest")
            val digestResult = getDigest(ledgerName)
            val tipBlockAddress = Constants.MAPPER.readValue(
                digestResult.digestTipAddress.ionText,
                BlockAddress::class.java
            )
            val digestTipAddress = digestResult.digestTipAddress
            val digestBytes = Verifier.convertByteBufferToByteArray(digestResult.digest)
            log.info(
                "Got a ledger digest. Digest end address={}, digest={}.",
                toUnredactedString(digestTipAddress),
                Verifier.toBase64(digestBytes)
            )
            val getBlockResult = getBlockWithProof(ledgerName, blockAddress, tipBlockAddress)
            val block = Constants.MAPPER.readValue(getBlockResult.block.ionText, JournalBlock::class.java)
            var verified = Verifier.verify(
                block.blockHash,
                digestBytes,
                getBlockResult.proof.ionText
            )
            if (!verified) {
                throw AssertionError("Block is not verified!")
            } else {
                log.info("Success! The block is verified.")
            }
            val alteredDigest = Verifier.flipRandomBit(digestBytes)
            log.info(
                "Let's try flipping one bit in the digest and assert that the block is NOT verified. "
                        + "The altered digest is: {}.", Verifier.toBase64(alteredDigest)
            )
            verified = Verifier.verify(
                block.blockHash,
                alteredDigest,
                getBlockResult.proof.ionText
            )
            if (verified) {
                throw AssertionError("Expected block to not be verified against altered digest.")
            } else {
                log.info("Success! As expected flipping a bit in the digest causes verification to fail.")
            }
            val alteredBlockHash = Verifier.flipRandomBit(block.blockHash)
            log.info(
                "Let's try flipping one bit in the block's hash and assert that the block is NOT "
                        + "verified. The altered block hash is: {}.", Verifier.toBase64(alteredBlockHash)
            )
            verified = Verifier.verify(
                alteredBlockHash,
                digestBytes,
                getBlockResult.proof.ionText
            )
            if (verified) {
                throw AssertionError("Expected altered block hash to not be verified against digest.")
            } else {
                log.info("Success! As expected flipping a bit in the block hash causes verification to fail.")
            }
        } catch (e: Exception) {
            log.error("Failed to verify blocks in the ledger with name={}.", ledgerName, e)
            throw e
        }
    }
}