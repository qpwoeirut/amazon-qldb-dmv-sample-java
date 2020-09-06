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

import com.amazonaws.services.qldb.model.DeleteLedgerRequest
import com.amazonaws.services.qldb.model.DeleteLedgerResult
import com.amazonaws.services.qldb.model.ResourceNotFoundException
import org.slf4j.LoggerFactory

/**
 * Delete a ledger.
 *
 * This code expects that you have AWS credentials setup per:
 * http://docs.aws.amazon.com/java-sdk/latest/developer-guide/setup-credentials.html
 */
object DeleteLedger {
    val log = LoggerFactory.getLogger(DeleteLedger::class.java)
    const val LEDGER_DELETION_POLL_PERIOD_MS = 20_000L
    val client = CreateLedger.client

    @Throws(Exception::class)
    @JvmStatic
    fun main(args: Array<String>) {
        try {
            DeletionProtection.setDeletionProtection(Constants.LEDGER_NAME, false)
            delete(Constants.LEDGER_NAME)
            waitForDeleted(Constants.LEDGER_NAME)
        } catch (e: Exception) {
            log.error("Unable to delete the ledger.", e)
            throw e
        }
    }

    /**
     * Send a request to the QLDB database to delete the specified ledger.
     * Disables deletion protection before sending the deletion request.
     *
     * @param ledgerName
     * Name of the ledger to be deleted.
     * @return DeleteLedgerResult.
     */
    @JvmStatic
    fun delete(ledgerName: String?): DeleteLedgerResult {
        log.info("Attempting to delete the ledger with name: {}...", ledgerName)
        val request = DeleteLedgerRequest().withName(ledgerName)
        val result = client.deleteLedger(request)
        log.info("Success.")
        return result
    }

    /**
     * Wait for the ledger to be deleted.
     *
     * @param ledgerName
     * Name of the ledger being deleted.
     * @throws InterruptedException if thread is being interrupted.
     */
    @JvmStatic
    @Throws(InterruptedException::class)
    fun waitForDeleted(ledgerName: String?) {
        log.info("Waiting for the ledger to be deleted...")
        while (true) {
            try {
                DescribeLedger.describe(ledgerName)
                log.info("The ledger is still being deleted. Please wait...")
                Thread.sleep(LEDGER_DELETION_POLL_PERIOD_MS)
            } catch (ex: ResourceNotFoundException) {
                log.info("Success. The ledger is deleted.")
                break
            }
        }
    }
}