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

import com.amazon.ion.IonValue
import org.slf4j.LoggerFactory
import software.amazon.qldb.TransactionExecutor
import software.amazon.qldb.tutorial.ConnectToLedger.driver
import software.amazon.qldb.tutorial.model.Owner
import software.amazon.qldb.tutorial.model.Owners
import software.amazon.qldb.tutorial.model.Person.Companion.getDocumentIdByGovId
import software.amazon.qldb.tutorial.model.SampleData
import java.io.IOException

/**
 * Finds and adds secondary owners for a vehicle.
 *
 * This code expects that you have AWS credentials setup per:
 * http://docs.aws.amazon.com/java-sdk/latest/developer-guide/setup-credentials.html
 */
object AddSecondaryOwner {
    val log = LoggerFactory.getLogger(AddSecondaryOwner::class.java)

    /**
     * Check whether a secondary owner has already been registered for the given VIN.
     *
     * @param txn
     * The [TransactionExecutor] for lambda execute.
     * @param vin
     * Unique VIN for a vehicle.
     * @param secondaryOwnerId
     * The secondary owner to add.
     * @return `true` if the given secondary owner has already been registered, `false` otherwise.
     * @throws IllegalStateException if failed to convert VIN to an [IonValue].
     */
    private fun isSecondaryOwnerForVehicle(
        txn: TransactionExecutor, vin: String,
        secondaryOwnerId: String
    ): Boolean {
        return try {
            log.info("Finding secondary owners for vehicle with VIN: {}...", vin)
            val query = "SELECT Owners.SecondaryOwners FROM VehicleRegistration AS v WHERE v.VIN = ?"
            val parameters = listOf(Constants.MAPPER.writeValueAsIonValue(vin))
            val result = txn.execute(query, parameters)
            val itr: Iterator<IonValue> = result.iterator()
            if (!itr.hasNext()) {
                return false
            }
            val owners = Constants.MAPPER.readValue(itr.next(), Owners::class.java)
            for (owner in owners.secondaryOwners) {
                if (secondaryOwnerId.equals(owner.personId, ignoreCase = true)) {
                    return true
                }
            }
            false
        } catch (ioe: IOException) {
            throw IllegalStateException(ioe)
        }
    }

    /**
     * Adds a secondary owner for the specified VIN.
     *
     * @param txn
     * The [TransactionExecutor] for lambda execute.
     * @param vin
     * Unique VIN for a vehicle.
     * @param secondaryOwner
     * The secondary owner to add.
     * @throws IllegalStateException if failed to convert parameter into an [IonValue].
     */
    fun addSecondaryOwnerForVin(
        txn: TransactionExecutor, vin: String,
        secondaryOwner: String
    ) {
        try {
            log.info("Inserting secondary owner for vehicle with VIN: {}...", vin)
            val query = String.format(
                "FROM VehicleRegistration AS v WHERE v.VIN = '%s' " +
                        "INSERT INTO v.Owners.SecondaryOwners VALUE ?", vin
            )
            val newOwner = Constants.MAPPER.writeValueAsIonValue(
                Owner(
                    secondaryOwner
                )
            )
            val result = txn.execute(query, newOwner)
            log.info("VehicleRegistration Document IDs which had secondary owners added: ")
            ScanTable.printDocuments(result)
        } catch (ioe: IOException) {
            throw IllegalStateException(ioe)
        }
    }

    @JvmStatic
    fun main(args: Array<String>) {
        val vin = SampleData.VEHICLES[1].vin
        val govId = SampleData.PEOPLE[0].govId
        driver.execute { txn: TransactionExecutor ->
            val documentId = getDocumentIdByGovId(txn, govId)
            if (isSecondaryOwnerForVehicle(txn, vin, documentId)) {
                log.info("Person with ID {} has already been added as a secondary owner of this vehicle.", govId)
            } else {
                addSecondaryOwnerForVin(txn, vin, documentId)
            }
        }
        log.info("Secondary owners successfully updated.")
    }
}