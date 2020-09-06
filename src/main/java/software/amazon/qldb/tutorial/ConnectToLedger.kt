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

import org.slf4j.LoggerFactory
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider
import software.amazon.awssdk.services.qldbsession.QldbSessionClient
import software.amazon.awssdk.services.qldbsession.QldbSessionClientBuilder
import software.amazon.qldb.QldbDriver
import software.amazon.qldb.RetryPolicy
import java.net.URI
import java.net.URISyntaxException
import java.sql.DriverManager.getDriver

/**
 * Connect to a session for a given ledger using default settings.
 *
 *
 * This code expects that you have AWS credentials setup per:
 * http://docs.aws.amazon.com/java-sdk/latest/developer-guide/setup-credentials.html
 */
object ConnectToLedger {
    val log = LoggerFactory.getLogger(ConnectToLedger::class.java)
    var credentialsProvider: AwsCredentialsProvider? = null
    var endpoint: String? = null
    val ledgerName = Constants.LEDGER_NAME
    val region: String? = null

    @get:JvmStatic
    var driver: QldbDriver = createQldbDriver()

    /**
     * Create a pooled driver for creating sessions.
     *
     * @param retryAttempts How many times the transaction will be retried in
     * case of a retryable issue happens like Optimistic Concurrency Control exception,
     * server side failures or network issues.
     * @return The pooled driver for creating sessions.
     */
    fun createQldbDriver(retryAttempts: Int): QldbDriver {
        val builder = amazonQldbSessionClientBuilder
        return QldbDriver.builder()
            .ledger(ledgerName)
            .transactionRetryPolicy(
                RetryPolicy
                    .builder()
                    .maxRetries(retryAttempts)
                    .build()
            )
            .sessionClientBuilder(builder)
            .build()
    }

    /**
     * Create a pooled driver for creating sessions.
     *
     * @return The pooled driver for creating sessions.
     */
    fun createQldbDriver(): QldbDriver {
        val builder = amazonQldbSessionClientBuilder
        return QldbDriver.builder()
            .ledger(ledgerName)
            .transactionRetryPolicy(
                RetryPolicy.builder()
                    .maxRetries(Constants.RETRY_LIMIT).build()
            )
            .sessionClientBuilder(builder)
            .build()
    }

    /**
     * Creates a QldbSession builder that is passed to the QldbDriver to connect to the Ledger.
     *
     * @return An instance of the AmazonQLDBSessionClientBuilder
     */
    @JvmStatic
    val amazonQldbSessionClientBuilder: QldbSessionClientBuilder
        get() {
            val builder = QldbSessionClient.builder()
            if (null != endpoint && null != region) {
                try {
                    builder.endpointOverride(URI(endpoint))
                } catch (e: URISyntaxException) {
                    throw IllegalArgumentException(e)
                }
            }
            if (null != credentialsProvider) {
                builder.credentialsProvider(credentialsProvider)
            }
            return builder
        }

    @JvmStatic
    fun main(args: Array<String>) {
        val tables = driver.tableNames
        log.info("Existing tables in the ledger:")
        for (table in tables) {
            log.info("- {} ", table)
        }
    }
}