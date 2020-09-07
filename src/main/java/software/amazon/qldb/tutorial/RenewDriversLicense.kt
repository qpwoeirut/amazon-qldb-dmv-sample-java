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
import com.amazon.ion.Timestamp
import com.amazon.ion.system.IonSystemBuilder
import org.slf4j.LoggerFactory
import software.amazon.qldb.TransactionExecutor
import software.amazon.qldb.tutorial.ConnectToLedger.driver
import software.amazon.qldb.tutorial.model.Owner
import software.amazon.qldb.tutorial.model.SampleData
import software.amazon.qldb.tutorial.model.SampleData.convertToLocalDate
import software.amazon.qldb.tutorial.model.SampleData.getDocumentIdsFromDmlResult
import java.io.IOException
import java.time.LocalDate
import java.util.*

/**
 * Find the person associated with a license number.
 * Renew a driver's license.
 *
 * This code expects that you have AWS credentials setup per:
 * http://docs.aws.amazon.com/java-sdk/latest/developer-guide/setup-credentials.html
 */
object RenewDriversLicense {
    val log = LoggerFactory.getLogger(RegisterDriversLicense::class.java)
    private val SYSTEM = IonSystemBuilder.standard().build()

    /**
     * Get the PersonId of a driver's license using the given license number.
     *
     * @param txn
     * The [TransactionExecutor] for lambda execute.
     * @param licenseNumber
     * License number of the driver's license to query.
     * @return the PersonId.
     * @throws IllegalStateException if failed to convert parameter into an [IonValue], or
     * if no PersonId was found.
     */
    private fun getPersonIdFromLicenseNumber(txn: TransactionExecutor, licenseNumber: String): String {
        return try {
            log.info("Finding person ID with license number: {}...", licenseNumber)
            val query = "SELECT PersonId FROM DriversLicense WHERE LicenseNumber = ?"
            val result = txn.execute(query, Constants.MAPPER.writeValueAsIonValue(licenseNumber))
            if (result.isEmpty) {
                ScanTable.printDocuments(result)
                throw IllegalStateException("Unable to find person with license number: $licenseNumber")
            }
            Constants.MAPPER.readValue(result.iterator().next(), Owner::class.java).personId
        } catch (ioe: IOException) {
            throw IllegalStateException(ioe)
        }
    }

    /**
     * Find a driver using the given personId.
     *
     * @param txn
     * The [TransactionExecutor] for lambda execute.
     * @param personId
     * The unique personId of a driver.
     * @throws IllegalStateException if failed to convert parameter into an [IonValue], or
     * if no driver was found using the given license number.
     */
    private fun verifyDriverFromLicenseNumber(txn: TransactionExecutor, personId: String) {
        try {
            log.info("Finding person with person ID: {}...", personId)
            val query = "SELECT p.* FROM Person AS p BY pid WHERE pid = ?"
            val parameters = listOf(Constants.MAPPER.writeValueAsIonValue(personId))
            val result = txn.execute(query, parameters)
            if (result.isEmpty) {
                ScanTable.printDocuments(result)
                throw IllegalStateException("Unable to find person with ID: $personId")
            }
        } catch (ioe: IOException) {
            throw IllegalStateException(ioe)
        }
    }

    /**
     * Renew the ValidToDate and ValidFromDate of a driver's license.
     *
     * @param txn
     * The [TransactionExecutor] for lambda execute.
     * @param licenseNumber
     * License number of the driver's license to update.
     * @param validFromDate
     * The new ValidFromDate.
     * @param validToDate
     * The new ValidToDate.
     * @return the list of updated Document IDs.
     * @throws IllegalStateException if failed to convert parameter into an [IonValue].
     */
    private fun renewDriversLicense(
        txn: TransactionExecutor, licenseNumber: String,
        validFromDate: LocalDate, validToDate: LocalDate
    ): List<String> {
        return try {
            log.info("Renewing license with license number: {}...", licenseNumber)
            val query = ("UPDATE DriversLicense AS d SET d.ValidFromDate = ?, d.ValidToDate = ? "
                    + "WHERE d.LicenseNumber = ?")
            val parameters: MutableList<IonValue> = ArrayList()
            parameters.add(localDateToTimestamp(validFromDate))
            parameters.add(localDateToTimestamp(validToDate))
            parameters.add(Constants.MAPPER.writeValueAsIonValue(licenseNumber))
            val result = txn.execute(query, parameters)
            val list = getDocumentIdsFromDmlResult(result)
            log.info("DriversLicense Document IDs which had licenses renewed: ")
            list.forEach { msg -> log.info(msg) }
            list
        } catch (ioe: IOException) {
            throw IllegalStateException(ioe)
        }
    }

    @JvmStatic
    fun main(args: Array<String>) {
        val licenseNumber = SampleData.LICENSES[0].licenseNumber
        driver.execute { txn: TransactionExecutor ->
            val personId = getPersonIdFromLicenseNumber(txn, licenseNumber)
            verifyDriverFromLicenseNumber(txn, personId)
            renewDriversLicense(
                txn, licenseNumber,
                convertToLocalDate("2019-04-19"), convertToLocalDate("2023-04-19")
            )
        }
    }

    private fun localDateToTimestamp(date: LocalDate): IonValue {
        return SYSTEM.newTimestamp(Timestamp.forDay(date.year, date.monthValue, date.dayOfMonth))
    }
}