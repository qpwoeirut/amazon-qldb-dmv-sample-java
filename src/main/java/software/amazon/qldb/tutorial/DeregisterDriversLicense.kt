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
import java.io.IOException

/**
 * De-register a driver's license.
 *
 * This code expects that you have AWS credentials setup per:
 * http://docs.aws.amazon.com/java-sdk/latest/developer-guide/setup-credentials.html
 */
object DeregisterDriversLicense {
    val log = LoggerFactory.getLogger(DeregisterDriversLicense::class.java)

    /**
     * De-register a driver's license specified by the given license number.
     *
     * @param txn
     * The [TransactionExecutor] for lambda execute.
     * @param licenseNumber
     * License number of the driver's license to de-register.
     * @throws IllegalStateException if failed to convert parameter into an [IonValue].
     */
    fun deregisterDriversLicense(txn: TransactionExecutor, licenseNumber: String?) {
        try {
            log.info("De-registering license with license number: {}...", licenseNumber)
            val query = "DELETE FROM DriversLicense AS d WHERE d.LicenseNumber = ?"
            val result = txn.execute(query, Constants.MAPPER.writeValueAsIonValue(licenseNumber))
            if (!result.isEmpty) {
                log.info("Successfully de-registered license: {}.", licenseNumber)
            } else {
                log.error("Error de-registering license, license {} not found.", licenseNumber)
            }
        } catch (ioe: IOException) {
            throw IllegalStateException(ioe)
        }
    }

    @JvmStatic
    fun main(args: Array<String>) {
        val license = SampleData.LICENSES[1]
        driver.execute { txn: TransactionExecutor -> deregisterDriversLicense(txn, license.licenseNumber) }
    }
}