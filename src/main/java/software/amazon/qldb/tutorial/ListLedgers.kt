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

import com.amazonaws.services.qldb.model.LedgerSummary
import com.amazonaws.services.qldb.model.ListLedgersRequest
import org.slf4j.LoggerFactory
import java.util.*

/**
 * List all QLDB ledgers in a given account.
 *
 * This code expects that you have AWS credentials setup per:
 * http://docs.aws.amazon.com/java-sdk/latest/developer-guide/setup-credentials.html
 */
object ListLedgers {
    val log = LoggerFactory.getLogger(ListLedgers::class.java)
    val client = CreateLedger.client

    @Throws(Exception::class)
    @JvmStatic
    fun main(args: Array<String>) {
        try {
            list()
        } catch (e: Exception) {
            log.error("Unable to list ledgers!", e)
            throw e
        }
    }

    /**
     * List all ledgers.
     *
     * @return a list of [LedgerSummary].
     */
    fun list(): List<LedgerSummary> {
        log.info("Let's list all the ledgers...")
        val ledgerSummaries: MutableList<LedgerSummary> = ArrayList()
        var nextToken: String? = null
        do {
            val request = ListLedgersRequest().withNextToken(nextToken)
            val result = client.listLedgers(request)
            ledgerSummaries.addAll(result.ledgers)
            nextToken = result.nextToken
        } while (nextToken != null)
        log.info("Success. List of ledgers: {}", ledgerSummaries)
        return ledgerSummaries
    }
}