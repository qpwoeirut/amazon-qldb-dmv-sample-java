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
import com.amazon.ion.system.IonReaderBuilder
import org.slf4j.LoggerFactory
import software.amazon.qldb.TransactionExecutor
import software.amazon.qldb.tutorial.ConnectToLedger.driver
import software.amazon.qldb.tutorial.ScanTable.printDocuments
import software.amazon.qldb.tutorial.ScanTable.toIonStructs
import software.amazon.qldb.tutorial.model.Owner
import software.amazon.qldb.tutorial.model.Person
import software.amazon.qldb.tutorial.model.Person.Companion.getDocumentIdByGovId
import software.amazon.qldb.tutorial.model.SampleData
import java.io.IOException
import java.util.*

/**
 * Find primary owner for a particular vehicle's VIN.
 * Transfer to another primary owner for a particular vehicle's VIN.
 *
 * This code expects that you have AWS credentials setup per:
 * http://docs.aws.amazon.com/java-sdk/latest/developer-guide/setup-credentials.html
 */
object TransferVehicleOwnership {
    val log = LoggerFactory.getLogger(TransferVehicleOwnership::class.java)

    /**
     * Query a driver's information using the given ID.
     *
     * @param txn
     * The [TransactionExecutor] for lambda execute.
     * @param documentId
     * The unique ID of a document in the Person table.
     * @return a [Person] object.
     * @throws IllegalStateException if failed to convert parameter into [IonValue].
     */
    fun findPersonFromDocumentId(txn: TransactionExecutor, documentId: String): Person {
        return try {
            log.info("Finding person for documentId: {}...", documentId)
            val query = "SELECT p.* FROM Person AS p BY pid WHERE pid = ?"
            val result = txn.execute(query, Constants.MAPPER.writeValueAsIonValue(documentId))
            check(!result.isEmpty) { "Unable to find person with ID: $documentId" }
            Constants.MAPPER.readValue(result.iterator().next(), Person::class.java)
        } catch (ioe: IOException) {
            throw IllegalStateException(ioe)
        }
    }

    /**
     * Find the primary owner for the given VIN.
     *
     * @param txn
     * The [TransactionExecutor] for lambda execute.
     * @param vin
     * Unique VIN for a vehicle.
     * @return a [Person] object.
     * @throws IllegalStateException if failed to convert parameter into [IonValue].
     */
    fun findPrimaryOwnerForVehicle(txn: TransactionExecutor, vin: String): Person {
        return try {
            log.info("Finding primary owner for vehicle with Vin: {}...", vin)
            val query = "SELECT Owners.PrimaryOwner.PersonId FROM VehicleRegistration AS v WHERE v.VIN = ?"
            val parameters = listOf(Constants.MAPPER.writeValueAsIonValue(vin))
            val result = txn.execute(query, parameters)
            val documents = toIonStructs(result)
            printDocuments(documents)
            check(!documents.isEmpty()) { "Unable to find registrations with VIN: $vin" }
            val reader = IonReaderBuilder.standard().build(documents[0])
            val personId = Constants.MAPPER.readValue(reader, LinkedHashMap::class.java)["PersonId"].toString()
            findPersonFromDocumentId(txn, personId)
        } catch (ioe: IOException) {
            throw IllegalStateException(ioe)
        }
    }

    /**
     * Update the primary owner for a vehicle registration with the given documentId.
     *
     * @param txn
     * The [TransactionExecutor] for lambda execute.
     * @param vin
     * Unique VIN for a vehicle.
     * @param documentId
     * New PersonId for the primary owner.
     * @throws IllegalStateException if no vehicle registration was found using the given document ID and VIN, or if failed
     * to convert parameters into [IonValue].
     */
    private fun updateVehicleRegistration(txn: TransactionExecutor, vin: String, documentId: String) {
        try {
            log.info("Updating primary owner for vehicle with Vin: {}...", vin)
            val query = "UPDATE VehicleRegistration AS v SET v.Owners.PrimaryOwner = ? WHERE v.VIN = ?"
            val parameters: MutableList<IonValue> = ArrayList()
            parameters.add(
                Constants.MAPPER.writeValueAsIonValue(
                    Owner(
                        documentId
                    )
                )
            )
            parameters.add(Constants.MAPPER.writeValueAsIonValue(vin))
            val result = txn.execute(query, parameters)
            printDocuments(result)
            check(!result.isEmpty) { "Unable to transfer vehicle, could not find registration." }
            log.info("Successfully transferred vehicle with VIN '{}' to new owner.", vin)
        } catch (ioe: IOException) {
            throw IllegalStateException(ioe)
        }
    }

    @JvmStatic
    fun main(args: Array<String>) {
        val vin = SampleData.VEHICLES[0].vin
        val primaryOwnerGovId = SampleData.PEOPLE[0].govId
        val newPrimaryOwnerGovId = SampleData.PEOPLE[1].govId
        driver.execute { txn: TransactionExecutor ->
            val primaryOwner = findPrimaryOwnerForVehicle(txn, vin)
            check(primaryOwner.govId == primaryOwnerGovId) {
                // Verify the primary owner.
                "Incorrect primary owner identified for vehicle, unable to transfer."
            }
            val newOwner = getDocumentIdByGovId(txn, newPrimaryOwnerGovId)
            updateVehicleRegistration(txn, vin, newOwner)
        }
        log.info("Successfully transferred vehicle ownership!")
    }
}