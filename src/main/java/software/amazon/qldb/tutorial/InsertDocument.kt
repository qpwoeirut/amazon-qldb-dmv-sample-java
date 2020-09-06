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
import software.amazon.qldb.tutorial.model.DriversLicense
import software.amazon.qldb.tutorial.model.SampleData
import software.amazon.qldb.tutorial.model.SampleData.getDocumentIdsFromDmlResult
import software.amazon.qldb.tutorial.model.SampleData.updateOwnerVehicleRegistration
import software.amazon.qldb.tutorial.model.SampleData.updatePersonIdDriversLicense
import software.amazon.qldb.tutorial.model.VehicleRegistration
import java.io.IOException
import java.util.*

/**
 * Insert documents into a table in a QLDB ledger.
 *
 * This code expects that you have AWS credentials setup per:
 * http://docs.aws.amazon.com/java-sdk/latest/developer-guide/setup-credentials.html
 */
object InsertDocument {
    val log = LoggerFactory.getLogger(InsertDocument::class.java)

    /**
     * Insert the given list of documents into the specified table and return the document IDs of the inserted documents.
     *
     * @param txn
     * The [TransactionExecutor] for lambda execute.
     * @param tableName
     * Name of the table to insert documents into.
     * @param documents
     * List of documents to insert into the specified table.
     * @return a list of document IDs.
     * @throws IllegalStateException if failed to convert documents into an [IonValue].
     */
    @JvmStatic
    fun insertDocuments(
        txn: TransactionExecutor, tableName: String?,
        documents: List<*>?
    ): List<String?> {
        log.info("Inserting some documents in the {} table...", tableName)
        return try {
            val query = String.format("INSERT INTO %s ?", tableName)
            val ionDocuments = Constants.MAPPER.writeValueAsIonValue(documents)
            getDocumentIdsFromDmlResult(txn.execute(query, ionDocuments))
        } catch (ioe: IOException) {
            throw IllegalStateException(ioe)
        }
    }

    /**
     * Update PersonIds in driver's licenses and in vehicle registrations using document IDs.
     *
     * @param documentIds
     * List of document IDs representing the PersonIds in DriversLicense and PrimaryOwners in VehicleRegistration.
     * @param licenses
     * List of driver's licenses to update.
     * @param registrations
     * List of registrations to update.
     */
    fun updatePersonId(
        documentIds: List<String?>, licenses: MutableList<DriversLicense?>,
        registrations: MutableList<VehicleRegistration?>
    ) {
        for (i in documentIds.indices) {
            val license = SampleData.LICENSES[i]
            val registration = SampleData.REGISTRATIONS[i]
            licenses.add(updatePersonIdDriversLicense(license, documentIds[i]))
            registrations.add(updateOwnerVehicleRegistration(registration, documentIds[i]!!))
        }
    }

    @JvmStatic
    fun main(args: Array<String>) {
        main()
    }

    @JvmStatic
    fun main() {
        val newDriversLicenses: MutableList<DriversLicense?> = ArrayList()
        val newVehicleRegistrations: MutableList<VehicleRegistration?> = ArrayList()
        driver.execute { txn: TransactionExecutor ->
            val documentIds = insertDocuments(txn, Constants.PERSON_TABLE_NAME, SampleData.PEOPLE)
            updatePersonId(documentIds, newDriversLicenses, newVehicleRegistrations)
            insertDocuments(txn, Constants.VEHICLE_TABLE_NAME, SampleData.VEHICLES)
            insertDocuments(
                txn, Constants.VEHICLE_REGISTRATION_TABLE_NAME,
                Collections.unmodifiableList(newVehicleRegistrations)
            )
            insertDocuments(
                txn, Constants.DRIVERS_LICENSE_TABLE_NAME,
                Collections.unmodifiableList(newDriversLicenses)
            )
        }
        log.info("Documents inserted successfully!")
    }
}