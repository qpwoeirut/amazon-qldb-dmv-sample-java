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
import software.amazon.qldb.TransactionExecutor
import software.amazon.qldb.tutorial.ConnectToLedger.driver
import software.amazon.qldb.tutorial.model.SampleData
import software.amazon.qldb.tutorial.model.VehicleRegistration.Companion.getDocumentIdByVin
import java.io.IOException
import java.time.Instant
import java.time.temporal.ChronoUnit

/**
 * Query a table's history for a particular set of documents.
 *
 * This code expects that you have AWS credentials setup per:
 * http://docs.aws.amazon.com/java-sdk/latest/developer-guide/setup-credentials.html
 */
object QueryHistory {
    val log = LoggerFactory.getLogger(QueryHistory::class.java)
    private const val THREE_MONTHS = 90

    /**
     * In this example, query the 'VehicleRegistration' history table to find all previous primary owners for a VIN.
     *
     * @param txn
     * The [TransactionExecutor] for lambda execute.
     * @param vin
     * VIN to find previous primary owners for.
     * @param query
     * The query to find previous primary owners.
     * @throws IllegalStateException if failed to convert document ID to an [IonValue].
     */
    private fun previousPrimaryOwners(txn: TransactionExecutor, vin: String, query: String) {
        try {
            val docId = getDocumentIdByVin(txn, vin)
            log.info("Querying the 'VehicleRegistration' table's history using VIN: {}...", vin)
            val result = txn.execute(query, Constants.MAPPER.writeValueAsIonValue(docId))
            ScanTable.printDocuments(result)
        } catch (ioe: IOException) {
            throw IllegalStateException(ioe)
        }
    }

    @JvmStatic
    fun main(args: Array<String>) {
        val threeMonthsAgo = Instant.now().minus(THREE_MONTHS.toLong(), ChronoUnit.DAYS).toString()
        val query = String.format(
            "SELECT data.Owners.PrimaryOwner, metadata.version "
                    + "FROM history(VehicleRegistration, `%s`) "
                    + "AS h WHERE h.metadata.id = ?", threeMonthsAgo
        )
        driver.execute { txn ->
            val vin = SampleData.VEHICLES[0].vin
            previousPrimaryOwners(txn, vin, query)
        }
        log.info("Successfully queried history.")
    }
}