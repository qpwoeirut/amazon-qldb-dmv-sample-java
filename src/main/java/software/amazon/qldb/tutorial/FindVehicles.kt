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
import software.amazon.qldb.tutorial.model.Person.Companion.getDocumentIdByGovId
import software.amazon.qldb.tutorial.model.SampleData
import java.io.IOException

/**
 * Find all vehicles registered under a person.
 *
 * This code expects that you have AWS credentials setup per:
 * http://docs.aws.amazon.com/java-sdk/latest/developer-guide/setup-credentials.html
 */
object FindVehicles {
    private val log = LoggerFactory.getLogger(FindVehicles::class.java)

    /**
     * Find vehicles registered under a driver using their government ID.
     *
     * @param txn
     * The [TransactionExecutor] for lambda execute.
     * @param govId
     * The government ID of the owner.
     * @throws IllegalStateException if failed to convert parameters into [IonValue].
     */
    private fun findVehiclesForOwner(txn: TransactionExecutor, govId: String) {
        try {
            val documentId = getDocumentIdByGovId(txn, govId)
            val query = ("SELECT v FROM Vehicle AS v INNER JOIN VehicleRegistration AS r "
                    + "ON v.VIN = r.VIN WHERE r.Owners.PrimaryOwner.PersonId = ?")
            val result = txn.execute(query, Constants.MAPPER.writeValueAsIonValue(documentId))
            log.info("List of Vehicles for owner with GovId: {}...", govId)
            ScanTable.printDocuments(result)
        } catch (ioe: IOException) {
            throw IllegalStateException(ioe)
        }
    }

    @JvmStatic
    fun main(args: Array<String>) {
        val person = SampleData.PEOPLE[0]
        driver.execute { txn -> findVehiclesForOwner(txn, person.govId) }
    }
}