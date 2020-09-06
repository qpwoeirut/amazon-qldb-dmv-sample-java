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

import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.services.qldb.AmazonQLDB
import com.amazonaws.services.qldb.AmazonQLDBClientBuilder
import com.amazonaws.services.qldb.model.*
import org.slf4j.LoggerFactory

/**
 * Create a ledger and wait for it to be active.
 *
 *
 * This code expects that you have AWS credentials setup per:
 * http://docs.aws.amazon.com/java-sdk/latest/developer-guide/setup-credentials.html
 */
object CreateLedger {
    val log = LoggerFactory.getLogger(CreateLedger::class.java)
    const val LEDGER_CREATION_POLL_PERIOD_MS = 10000L
    var endpoint: String? = null
    var region: String? = null

    val client = buildClient()
        @JvmStatic get

    /**
     * Build a low-level QLDB client.
     *
     * @return [AmazonQLDB] control plane client.
     */
    @JvmStatic
    fun buildClient(): AmazonQLDB {
        val builder = AmazonQLDBClientBuilder.standard()
        if (null != endpoint && null != region) {
            builder.setEndpointConfiguration(EndpointConfiguration(endpoint, region))
        }
        return builder.build()
    }

    @Throws(Exception::class)
    @JvmStatic
    fun main(args: Array<String>) {
        try {
            create(Constants.LEDGER_NAME)
            waitForActive(Constants.LEDGER_NAME)
        } catch (e: Exception) {
            log.error("Unable to create the ledger!", e)
            throw e
        }
    }

    /**
     * Create a new ledger with the specified ledger name.
     *
     * @param ledgerName Name of the ledger to be created.
     * @return [CreateLedgerResult] from QLDB.
     */
    @JvmStatic
    fun create(ledgerName: String?): CreateLedgerResult {
        log.info("Let's create the ledger with name: {}...", ledgerName)
        val request = CreateLedgerRequest()
            .withName(ledgerName)
            .withPermissionsMode(PermissionsMode.ALLOW_ALL)
        val result = client.createLedger(request)
        log.info("Success. Ledger state: {}.", result.state)
        return result
    }

    /**
     * Wait for a newly created ledger to become active.
     *
     * @param ledgerName Name of the ledger to wait on.
     * @return [DescribeLedgerResult] from QLDB.
     * @throws InterruptedException if thread is being interrupted.
     */
    @JvmStatic
    @Throws(InterruptedException::class)
    fun waitForActive(ledgerName: String?): DescribeLedgerResult {
        log.info("Waiting for ledger to become active...")
        while (true) {
            val result = DescribeLedger.describe(ledgerName)
            if (result.state == LedgerState.ACTIVE.name) {
                log.info("Success. Ledger is active and ready to use.")
                return result
            }
            log.info("The ledger is still creating. Please wait...")
            Thread.sleep(LEDGER_CREATION_POLL_PERIOD_MS)
        }
    }
}