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

import com.amazonaws.services.qldb.model.*
import org.slf4j.LoggerFactory
import software.amazon.qldb.tutorial.CreateLedger.waitForActive
import software.amazon.qldb.tutorial.DeleteLedger.delete

/**
 * Demonstrate the protection of QLDB ledgers against deletion.
 *
 * This code expects that you have AWS credentials setup per:
 * http://docs.aws.amazon.com/java-sdk/latest/developer-guide/setup-credentials.html
 */
object DeletionProtection {
    val log = LoggerFactory.getLogger(DeletionProtection::class.java)
    const val LEDGER_NAME = "tag-demo"
    val client = CreateLedger.client

    @Throws(Exception::class)
    @JvmStatic
    fun main(args: Array<String>) {
        try {
            createWithDeletionProtection(LEDGER_NAME)
            waitForActive(LEDGER_NAME)
            setDeletionProtection(LEDGER_NAME, true)
            try {
                delete(LEDGER_NAME)
            } catch (e: ResourcePreconditionNotMetException) {
                log.info("Ledger protected against deletions!")
            }
            setDeletionProtection(LEDGER_NAME, false)
            delete(LEDGER_NAME)
        } catch (e: Exception) {
            log.error("Error while updating or deleting the ledger!", e)
            throw e
        }
    }

    fun createWithDeletionProtection(ledgerName: String): CreateLedgerResult {
        log.info("Let's create the ledger with name: {}...", ledgerName)
        val request = CreateLedgerRequest()
            .withName(ledgerName)
            .withPermissionsMode(PermissionsMode.ALLOW_ALL)
            .withDeletionProtection(true)
        val result = client.createLedger(request)
        log.info("Success. Ledger state: {}", result.state)
        return result
    }

    @JvmStatic
    fun setDeletionProtection(ledgerName: String, deletionProtection: Boolean): UpdateLedgerResult {
        log.info("Let's set deletionProtection to {} for the ledger with name {}", deletionProtection, ledgerName)
        val request = UpdateLedgerRequest()
            .withName(ledgerName)
            .withDeletionProtection(deletionProtection)
        val result = client.updateLedger(request)
        log.info("Success. Ledger updated: {}", result)
        return result
    }
}