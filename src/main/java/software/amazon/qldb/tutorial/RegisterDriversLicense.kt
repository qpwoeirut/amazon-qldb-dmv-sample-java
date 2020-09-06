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

import com.fasterxml.jackson.dataformat.ion.IonObjectMapper
import org.slf4j.LoggerFactory
import software.amazon.qldb.Result
import software.amazon.qldb.TransactionExecutor
import software.amazon.qldb.tutorial.ConnectToLedger.driver
import software.amazon.qldb.tutorial.InsertDocument.insertDocuments
import software.amazon.qldb.tutorial.model.DriversLicense
import software.amazon.qldb.tutorial.model.Person
import software.amazon.qldb.tutorial.model.Person.Companion.getDocumentIdByGovId
import software.amazon.qldb.tutorial.model.SampleData.convertToLocalDate
import java.io.IOException

/**
 * Register a new driver's license.
 *
 * This code expects that you have AWS credentials setup per:
 * http://docs.aws.amazon.com/java-sdk/latest/developer-guide/setup-credentials.html
 */
object RegisterDriversLicense {
    val log = LoggerFactory.getLogger(RegisterDriversLicense::class.java)

    /**
     * Verify whether a driver already exists in the database.
     *
     * @param txn
     * The [TransactionExecutor] for lambda execute.
     * @param govId
     * The government ID of the new owner.
     * @return `true` if the driver has already been registered; `false` otherwise.
     * @throws IOException if failed to convert parameter into an IonValue.
     */
    @Throws(IOException::class)
    fun personAlreadyExists(txn: TransactionExecutor, govId: String): Boolean {
        val query = "SELECT * FROM Person AS p WHERE p.GovId = ?"
        val result = txn.execute(query, Constants.MAPPER.writeValueAsIonValue(govId))
        return !result.isEmpty
    }

    /**
     * Verify whether a driver has a driver's license in the database.
     *
     * @param txn
     * The [TransactionExecutor] for lambda execute.
     * @param personId
     * The unique personId of the new owner.
     * @return `true` if driver has a driver's license; `false` otherwise.
     * @throws IOException if failed to convert parameter into an IonValue.
     */
    @Throws(IOException::class)
    fun personHadDriversLicense(txn: TransactionExecutor, personId: String): Boolean {
        val result = queryDriversLicenseByPersonId(txn, personId)
        return !result.isEmpty
    }

    /**
     * Find a driver's license using the given personId.
     *
     * @param txn
     * The [TransactionExecutor] for lambda execute.
     * @param personId
     * The unique personId of a driver.
     * @return the result set.
     * @throws IOException if failed to convert parameter into an IonValue.
     */
    @Throws(IOException::class)
    fun queryDriversLicenseByPersonId(txn: TransactionExecutor, personId: String): Result {
        val query = "SELECT * FROM DriversLicense AS d WHERE d.PersonId = ?"
        val parameters = listOf(Constants.MAPPER.writeValueAsIonValue(personId))
        return txn.execute(query, parameters)
    }

    /**
     * Register a new driver's license.
     *
     * @param txn
     * The [TransactionExecutor] for lambda execute.
     * @param govId
     * The government ID of the new owner.
     * @param license
     * The new license to register.
     * @param personId
     * The unique personId of the new owner.
     * @throws IllegalStateException if failed to convert document ID to an IonValue.
     */
    private fun registerNewDriversLicense(
        txn: TransactionExecutor, govId: String,
        license: DriversLicense, personId: String
    ) {
        try {
            if (personHadDriversLicense(txn, personId)) {
                log.info("Person with government ID '{}' already has a license! No new license added.", govId)
                return
            }
            val query = "INSERT INTO DriversLicense ?"
            log.info(IonObjectMapper().writeValueAsIonValue(license).toPrettyString())
            val parameters = listOf(Constants.MAPPER.writeValueAsIonValue(license))
            txn.execute(query, parameters)
            val result = queryDriversLicenseByPersonId(txn, govId)
            if (ScanTable.toIonStructs(result).size > 0) {
                log.info("Problem occurred while inserting new license, please review the results.")
            } else {
                log.info("Successfully registered new driver.")
            }
        } catch (ioe: IOException) {
            throw IllegalStateException(ioe)
        }
    }

    @JvmStatic
    fun main(args: Array<String>) {
        val newPerson = Person(
            "Kate",
            "Mulberry",
            convertToLocalDate("1995-02-09"),
            "AQQ17B2342",
            "Passport",
            "22 Commercial Drive, Blaine, WA, 97722"
        )
        driver.execute { txn: TransactionExecutor ->
            val documentId: String
            val documentIdList: List<String>
            try {
                if (personAlreadyExists(txn, newPerson.govId)) {
                    log.info("Person with this GovId already exists.")
                    documentId = getDocumentIdByGovId(txn, newPerson.govId)
                } else {
                    documentIdList = insertDocuments(txn, Constants.PERSON_TABLE_NAME, listOf(newPerson))
                    documentId = documentIdList[0]
                }
            } catch (ioe: IOException) {
                throw IllegalStateException(ioe)
            }
            val newLicense = DriversLicense(
                documentId,
                "112 360 PXJ",
                "Full",
                convertToLocalDate("2018-06-30"),
                convertToLocalDate("2022-10-30")
            )
            registerNewDriversLicense(txn, newPerson.govId, newLicense, documentId)
        }
    }
}