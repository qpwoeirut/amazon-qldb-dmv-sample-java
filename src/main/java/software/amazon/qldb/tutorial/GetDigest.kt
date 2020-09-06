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

import com.amazonaws.services.qldb.model.GetDigestRequest
import com.amazonaws.services.qldb.model.GetDigestResult
import org.slf4j.LoggerFactory
import software.amazon.qldb.tutorial.qldb.QldbStringUtils.toUnredactedString

/**
 * This is an example for retrieving the digest of a particular ledger.
 *
 * This code expects that you have AWS credentials setup per:
 * http://docs.aws.amazon.com/java-sdk/latest/developer-guide/setup-credentials.html
 */
object GetDigest {
    val log = LoggerFactory.getLogger(GetDigest::class.java)
    var client = CreateLedger.client

    /**
     * Calls [.getDigest] for a ledger.
     *
     * @param args
     * Arbitrary command-line arguments.
     * @throws Exception if failed to get a ledger digest.
     */
    @Throws(Exception::class)
    @JvmStatic
    fun main(args: Array<String>) {
        try {
            getDigest(Constants.LEDGER_NAME)
        } catch (e: Exception) {
            log.error("Unable to get a ledger digest!", e)
            throw e
        }
    }

    /**
     * Get the digest for the specified ledger.
     *
     * @param ledgerName
     * The ledger to get digest from.
     * @return [GetDigestResult].
     */
    @JvmStatic
    fun getDigest(ledgerName: String): GetDigestResult {
        log.info("Let's get the current digest of the ledger named {}.", ledgerName)
        val request = GetDigestRequest()
            .withName(ledgerName)
        val result = client.getDigest(request)
        log.info("Success. LedgerDigest: {}.", toUnredactedString(result))
        return result
    }
}