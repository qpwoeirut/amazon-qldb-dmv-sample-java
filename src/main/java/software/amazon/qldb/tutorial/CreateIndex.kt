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

/**
 * Create indexes on tables in a particular ledger.
 *
 * This code expects that you have AWS credentials setup per:
 * http://docs.aws.amazon.com/java-sdk/latest/developer-guide/setup-credentials.html
 */
object CreateIndex {
    val log = LoggerFactory.getLogger(CreateIndex::class.java)

    /**
     * In this example, create indexes for registrations and vehicles tables.
     *
     * @param txn
     * The [TransactionExecutor] for lambda execute.
     * @param tableName
     * Name of the table to be created.
     * @param indexAttribute
     * The index attribute to use.
     * @return the number of tables created.
     */
    fun createIndex(txn: TransactionExecutor, tableName: String?, indexAttribute: String?): Int {
        log.info("Creating an index on {}...", indexAttribute)
        val createIndex = String.format("CREATE INDEX ON %s (%s)", tableName, indexAttribute)
        val r = txn.execute(createIndex)
        return SampleData.toIonValues(r).size
    }

    @JvmStatic
    fun main(args: Array<String>) {
        driver.execute { txn: TransactionExecutor ->
            createIndex(txn, Constants.PERSON_TABLE_NAME, Constants.PERSON_GOV_ID_INDEX_NAME)
            createIndex(txn, Constants.VEHICLE_TABLE_NAME, Constants.VIN_INDEX_NAME)
            createIndex(txn, Constants.DRIVERS_LICENSE_TABLE_NAME, Constants.DRIVER_LICENSE_NUMBER_INDEX_NAME)
            createIndex(txn, Constants.DRIVERS_LICENSE_TABLE_NAME, Constants.DRIVER_LICENSE_PERSONID_INDEX_NAME)
            createIndex(txn, Constants.VEHICLE_REGISTRATION_TABLE_NAME, Constants.VIN_INDEX_NAME)
            createIndex(
                txn, Constants.VEHICLE_REGISTRATION_TABLE_NAME,
                Constants.VEHICLE_REGISTRATION_LICENSE_PLATE_NUMBER_INDEX_NAME
            )
        }
        log.info("Indexes created successfully!")
    }
}