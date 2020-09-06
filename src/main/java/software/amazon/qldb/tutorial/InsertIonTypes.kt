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
import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.annotation.JsonProperty
import org.slf4j.LoggerFactory
import software.amazon.qldb.TransactionExecutor
import software.amazon.qldb.tutorial.ConnectToLedger.driver
import software.amazon.qldb.tutorial.CreateTable.createTable
import software.amazon.qldb.tutorial.InsertDocument.insertDocuments

/**
 * Insert all the supported Ion types into a ledger and verify that they are stored and can be retrieved properly, retaining
 * their original properties.
 *
 * This code expects that you have AWS credentials setup per:
 * http://docs.aws.amazon.com/java-sdk/latest/developer-guide/setup-credentials.html
 */
object InsertIonTypes {
    val log = LoggerFactory.getLogger(InsertIonTypes::class.java)
    const val TABLE_NAME = "IonTypes"

    /**
     * Update a document's Name value in the database. Then, query the value of the Name key and verify the expected Ion type was
     * saved.
     *
     * @param txn
     * The [TransactionExecutor] for statement execution.
     * @param ionValue
     * The [IonValue] to set the document's Name value to.
     *
     * @throws AssertionError when no value is returned for the Name key or if the value does not match the expected type.
     */
    fun updateRecordAndVerifyType(txn: TransactionExecutor, ionValue: IonValue) {
        val updateStatement = String.format("UPDATE %s SET Name = ?", TABLE_NAME)
        val parameters = listOf(ionValue)
        txn.execute(updateStatement, parameters)
        log.info("Updated document.")
        val searchQuery = String.format("SELECT VALUE Name FROM %s", TABLE_NAME)
        val result = txn.execute(searchQuery)
        if (result.isEmpty) {
            throw AssertionError("Did not find any values for the Name key.")
        }
        for (value in result) {
            if (!ionValue.javaClass.isInstance(value)) {
                throw AssertionError(
                    String.format(
                        "The queried value, %s, is not an instance of %s.",
                        value.javaClass.toString(), ionValue.javaClass.toString()
                    )
                )
            }
            if (value.type != ionValue.type) {
                throw AssertionError(
                    String.format(
                        "The queried value type, %s, does not match %s.",
                        value.type.toString(), ionValue.type.toString()
                    )
                )
            }
        }
        log.info(
            "Successfully verified value is instance of {} with type {}.", ionValue.javaClass.toString(),
            ionValue.type.toString()
        )
    }

    /**
     * Delete a table.
     *
     * @param txn
     * The [TransactionExecutor] for lambda execute.
     * @param tableName
     * The name of the table to delete.
     */
    fun deleteTable(txn: TransactionExecutor, tableName: String) {
        log.info("Deleting {} table...", tableName)
        val statement = String.format("DROP TABLE %s", tableName)
        txn.execute(statement)
        log.info("{} table successfully deleted.", tableName)
    }

    @JvmStatic
    fun main(args: Array<String>) {
        val ionBlob = Constants.SYSTEM.newBlob("hello".toByteArray())
        val ionBool = Constants.SYSTEM.newBool(true)
        val ionClob = Constants.SYSTEM.newClob("{{'This is a CLOB of text.'}}".toByteArray())
        val ionDecimal = Constants.SYSTEM.newDecimal(0.1)
        val ionFloat = Constants.SYSTEM.newFloat(0.2)
        val ionInt = Constants.SYSTEM.newInt(1)
        val ionList = Constants.SYSTEM.newList(intArrayOf(1, 2))
        val ionNull = Constants.SYSTEM.newNull()
        val ionSexp = Constants.SYSTEM.newSexp(intArrayOf(2, 3))
        val ionString = Constants.SYSTEM.newString("string")
        val ionStruct = Constants.SYSTEM.newEmptyStruct()
        ionStruct.put("brand", Constants.SYSTEM.newString("ford"))
        val ionSymbol = Constants.SYSTEM.newSymbol("abc")
        val ionTimestamp = Constants.SYSTEM.newTimestamp(Timestamp.now())
        val ionNullBlob = Constants.SYSTEM.newNullBlob()
        val ionNullBool = Constants.SYSTEM.newNullBool()
        val ionNullClob = Constants.SYSTEM.newNullClob()
        val ionNullDecimal = Constants.SYSTEM.newNullDecimal()
        val ionNullFloat = Constants.SYSTEM.newNullFloat()
        val ionNullInt = Constants.SYSTEM.newNullInt()
        val ionNullList = Constants.SYSTEM.newNullList()
        val ionNullSexp = Constants.SYSTEM.newNullSexp()
        val ionNullString = Constants.SYSTEM.newNullString()
        val ionNullStruct = Constants.SYSTEM.newNullStruct()
        val ionNullSymbol = Constants.SYSTEM.newNullSymbol()
        val ionNullTimestamp = Constants.SYSTEM.newNullTimestamp()
        driver.execute { txn: TransactionExecutor ->
            createTable(txn, TABLE_NAME)
            val document = Document(Constants.SYSTEM.newString("val"))
            insertDocuments(txn, TABLE_NAME, listOf(document))
            updateRecordAndVerifyType(txn, ionBlob)
            updateRecordAndVerifyType(txn, ionBool)
            updateRecordAndVerifyType(txn, ionClob)
            updateRecordAndVerifyType(txn, ionDecimal)
            updateRecordAndVerifyType(txn, ionFloat)
            updateRecordAndVerifyType(txn, ionInt)
            updateRecordAndVerifyType(txn, ionList)
            updateRecordAndVerifyType(txn, ionNull)
            updateRecordAndVerifyType(txn, ionSexp)
            updateRecordAndVerifyType(txn, ionString)
            updateRecordAndVerifyType(txn, ionStruct)
            updateRecordAndVerifyType(txn, ionSymbol)
            updateRecordAndVerifyType(txn, ionTimestamp)
            updateRecordAndVerifyType(txn, ionNullBlob)
            updateRecordAndVerifyType(txn, ionNullBool)
            updateRecordAndVerifyType(txn, ionNullClob)
            updateRecordAndVerifyType(txn, ionNullDecimal)
            updateRecordAndVerifyType(txn, ionNullFloat)
            updateRecordAndVerifyType(txn, ionNullInt)
            updateRecordAndVerifyType(txn, ionNullList)
            updateRecordAndVerifyType(txn, ionNullSexp)
            updateRecordAndVerifyType(txn, ionNullString)
            updateRecordAndVerifyType(txn, ionNullStruct)
            updateRecordAndVerifyType(txn, ionNullSymbol)
            updateRecordAndVerifyType(txn, ionNullTimestamp)
            deleteTable(txn, TABLE_NAME)
        }
    }

    /**
     * This class represents a simple document with a single key, Name, to use for the IonTypes table.
     */
    private class Document @JsonCreator constructor(
        @get:JsonProperty("Name")
        @param:JsonProperty("Name") private val name: IonValue
    )
}