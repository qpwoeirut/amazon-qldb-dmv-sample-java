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

import com.amazon.ion.IonStruct
import com.amazon.ion.IonValue
import org.slf4j.LoggerFactory
import software.amazon.qldb.Result
import software.amazon.qldb.TransactionExecutor
import software.amazon.qldb.tutorial.ConnectToLedger.driver
import java.util.*
import java.util.function.Consumer
import java.util.stream.Collectors

/**
 * Scan for all the documents in a table.
 *
 * This code expects that you have AWS credentials setup per:
 * http://docs.aws.amazon.com/java-sdk/latest/developer-guide/setup-credentials.html
 */
object ScanTable {
    private val log = LoggerFactory.getLogger(ScanTable::class.java)

    /**
     * Scan the table with the given `tableName` for all documents.
     *
     * @param txn
     * The [TransactionExecutor] for lambda execute.
     * @param tableName
     * Name of the table to scan.
     * @return a list of documents in [IonStruct] .
     */
    private fun scanTableForDocuments(txn: TransactionExecutor, tableName: String): List<IonStruct> {
        log.info("Scanning '{}'...", tableName)
        val scanTable = "SELECT * FROM $tableName"
        val documents = toIonStructs(txn.execute(scanTable))
        log.info("Scan successful!")
        printDocuments(documents)
        return documents
    }

    /**
     * Pretty print all elements in the provided [Result].
     *
     * @param result
     * [Result] from executing a query.
     */
    @JvmStatic
    fun printDocuments(result: Result) {
        result.iterator().forEachRemaining { row: IonValue -> log.info(row.toPrettyString()) }
    }

    /**
     * Pretty print all elements in the provided list of [IonStruct].
     *
     * @param documents
     * List of documents to print.
     */
    @JvmStatic
    fun printDocuments(documents: List<IonStruct>) {
        documents.forEach(Consumer { row: IonStruct -> log.info(row.toPrettyString()) })
    }

    /**
     * Convert the result set into a list of [IonStruct].
     *
     * @param result
     * [Result] from executing a query.
     * @return a list of documents in IonStruct.
     */
    @JvmStatic
    fun toIonStructs(result: Result): List<IonStruct> {
        val documentList: MutableList<IonStruct> = ArrayList()
        result.iterator().forEachRemaining { row: IonValue -> documentList.add(row as IonStruct) }
        return documentList
    }

    @JvmStatic
    fun main(args: Array<String>) {
        driver.execute { txn ->
            val tableNames = scanTableForDocuments(txn, Constants.USER_TABLES)
                .map { it["name"].toString() }
            for (tableName in tableNames) {
                scanTableForDocuments(txn, tableName)
            }
        }
    }
}