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
package software.amazon.qldb.tutorial.model

import com.amazon.ion.IonString
import com.amazon.ion.IonStruct
import com.amazon.ion.IonValue
import software.amazon.qldb.Result
import software.amazon.qldb.TransactionExecutor
import software.amazon.qldb.tutorial.ConnectToLedger.driver
import software.amazon.qldb.tutorial.Constants
import software.amazon.qldb.tutorial.qldb.DmlResultDocument
import software.amazon.qldb.tutorial.qldb.QldbRevision
import java.io.IOException
import java.math.BigDecimal
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.util.*

const val nullPersonId = ""

/**
 * Sample domain objects for use throughout this tutorial.
 */
object SampleData {
    val DATE_TIME_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd")
    @JvmField
    val REGISTRATIONS = Collections.unmodifiableList(
        Arrays.asList(
            VehicleRegistration(
                "1N4AL11D75C109151", "LEWISR261LL", "WA", "Seattle",
                BigDecimal.valueOf(90.25), convertToLocalDate("2017-08-21"), convertToLocalDate("2020-05-11"),
                Owners(Owner(nullPersonId), emptyList())
            ),
            VehicleRegistration(
                "KM8SRDHF6EU074761", "CA762X", "WA", "Kent",
                BigDecimal.valueOf(130.75), convertToLocalDate("2017-09-14"), convertToLocalDate("2020-06-25"),
                Owners(Owner(nullPersonId), emptyList())
            ),
            VehicleRegistration(
                "3HGGK5G53FM761765", "CD820Z", "WA", "Everett",
                BigDecimal.valueOf(442.30), convertToLocalDate("2011-03-17"), convertToLocalDate("2021-03-24"),
                Owners(Owner(nullPersonId), emptyList())
            ),
            VehicleRegistration(
                "1HVBBAANXWH544237", "LS477D", "WA", "Tacoma",
                BigDecimal.valueOf(42.20), convertToLocalDate("2011-10-26"), convertToLocalDate("2023-09-25"),
                Owners(Owner(nullPersonId), emptyList())
            ),
            VehicleRegistration(
                "1C4RJFAG0FC625797", "TH393F", "WA", "Olympia",
                BigDecimal.valueOf(30.45), convertToLocalDate("2013-09-02"), convertToLocalDate("2024-03-19"),
                Owners(Owner(nullPersonId), emptyList())
            )
        )
    )
    @JvmField
    val VEHICLES = Collections.unmodifiableList(
        Arrays.asList(
            Vehicle("1N4AL11D75C109151", "Sedan", 2011, "Audi", "A5", "Silver"),
            Vehicle("KM8SRDHF6EU074761", "Sedan", 2015, "Tesla", "Model S", "Blue"),
            Vehicle("3HGGK5G53FM761765", "Motorcycle", 2011, "Ducati", "Monster 1200", "Yellow"),
            Vehicle("1HVBBAANXWH544237", "Semi", 2009, "Ford", "F 150", "Black"),
            Vehicle("1C4RJFAG0FC625797", "Sedan", 2019, "Mercedes", "CLK 350", "White")
        )
    )
    @JvmField
    val PEOPLE = Collections.unmodifiableList(
        Arrays.asList(
            Person(
                "Raul", "Lewis", convertToLocalDate("1963-08-19"),
                "LEWISR261LL", "Driver License", "1719 University Street, Seattle, WA, 98109"
            ),
            Person(
                "Brent", "Logan", convertToLocalDate("1967-07-03"),
                "LOGANB486CG", "Driver License", "43 Stockert Hollow Road, Everett, WA, 98203"
            ),
            Person(
                "Alexis", "Pena", convertToLocalDate("1974-02-10"),
                "744 849 301", "SSN", "4058 Melrose Street, Spokane Valley, WA, 99206"
            ),
            Person(
                "Melvin", "Parker", convertToLocalDate("1976-05-22"),
                "P626-168-229-765", "Passport", "4362 Ryder Avenue, Seattle, WA, 98101"
            ),
            Person(
                "Salvatore", "Spencer", convertToLocalDate("1997-11-15"),
                "S152-780-97-415-0", "Passport", "4450 Honeysuckle Lane, Seattle, WA, 98101"
            )
        )
    )
    @JvmField
    val LICENSES = Collections.unmodifiableList(
        Arrays.asList(
            DriversLicense(
                nullPersonId, "LEWISR261LL", "Learner",
                convertToLocalDate("2016-12-20"), convertToLocalDate("2020-11-15")
            ),
            DriversLicense(
                nullPersonId, "LOGANB486CG", "Probationary",
                convertToLocalDate("2016-04-06"), convertToLocalDate("2020-11-15")
            ),
            DriversLicense(
                nullPersonId, "744 849 301", "Full",
                convertToLocalDate("2017-12-06"), convertToLocalDate("2022-10-15")
            ),
            DriversLicense(
                nullPersonId, "P626-168-229-765", "Learner",
                convertToLocalDate("2017-08-16"), convertToLocalDate("2021-11-15")
            ),
            DriversLicense(
                nullPersonId, "S152-780-97-415-0", "Probationary",
                convertToLocalDate("2015-08-15"), convertToLocalDate("2021-08-21")
            )
        )
    )

    /**
     * Converts a date string with the format 'yyyy-MM-dd' into a [java.util.Date] object.
     *
     * @param date
     * The date string to convert.
     * @return [java.time.LocalDate] or null if there is a [ParseException]
     */
    @JvmStatic
    @Synchronized
    fun convertToLocalDate(date: String): LocalDate {
        return LocalDate.parse(date, DATE_TIME_FORMAT)
    }

    /**
     * Convert the result set into a list of IonValues.
     *
     * @param result
     * The result set to convert.
     * @return a list of IonValues.
     */
    fun toIonValues(result: Result): List<IonValue> {
        val valueList: MutableList<IonValue> = ArrayList()
        result.iterator().forEachRemaining { e: IonValue -> valueList.add(e) }
        return valueList
    }

    /**
     * Get the document ID of a particular document.
     *
     * @param txn
     * A transaction executor object.
     * @param tableName
     * Name of the table containing the document.
     * @param identifier
     * The identifier used to narrow down the search.
     * @param value
     * Value of the identifier.
     * @return the list of document IDs in the result set.
     */
    @JvmStatic
    fun getDocumentId(
        txn: TransactionExecutor, tableName: String,
        identifier: String, value: String
    ): String {
        return try {
            val parameters = listOf(Constants.MAPPER.writeValueAsIonValue(value))
            val query = String.format(
                "SELECT metadata.id FROM _ql_committed_%s AS p WHERE p.data.%s = ?",
                tableName, identifier
            )
            val result = txn.execute(query, parameters)
            check(!result.isEmpty) { "Unable to retrieve document ID using $value" }
            getStringValueOfStructField(result.iterator().next() as IonStruct, "id")
        } catch (ioe: IOException) {
            throw IllegalStateException(ioe)
        }
    }

    /**
     * Get the document by ID.
     *
     * @param tableName
     * Name of the table to insert documents into.
     * @param documentId
     * The unique ID of a document in the Person table.
     * @return a [QldbRevision] object.
     * @throws IllegalStateException if failed to convert parameter into [IonValue].
     */
    fun getDocumentById(tableName: String, documentId: String): QldbRevision {
        return try {
            val ionValue = Constants.MAPPER.writeValueAsIonValue(documentId)
            val result = driver.execute<Result> { txn: TransactionExecutor ->
                txn.execute(
                    "SELECT c.* FROM _ql_committed_" + tableName + " AS c BY docId "
                            + "WHERE docId = ?", ionValue
                )
            }
            check(!result.isEmpty) { "Unable to retrieve document by id $documentId in table $tableName" }
            Constants.MAPPER.readValue(result.iterator().next(), QldbRevision::class.java)
        } catch (ioe: IOException) {
            throw IllegalStateException(ioe)
        }
    }

    /**
     * Return a list of modified document IDs as strings from a DML [Result].
     *
     * @param result
     * The result set from a DML operation.
     * @return the list of document IDs modified by the operation.
     */
    @JvmStatic
    fun getDocumentIdsFromDmlResult(result: Result): List<String> {
        val strings: MutableList<String> = ArrayList()
        result.iterator().forEachRemaining { row: IonValue? -> strings.add(getDocumentIdFromDmlResultDocument(row)) }
        return strings
    }

    /**
     * Convert the given DML result row's document ID to string.
     *
     * @param dmlResultDocument
     * The [IonValue] representing the results of a DML operation.
     * @return a string of document ID.
     */
    fun getDocumentIdFromDmlResultDocument(dmlResultDocument: IonValue?): String {
        return try {
            val result = Constants.MAPPER.readValue(dmlResultDocument, DmlResultDocument::class.java)
            result.documentId
        } catch (ioe: IOException) {
            throw IllegalStateException(ioe)
        }
    }

    /**
     * Get the String value of a given [IonStruct] field name.
     * @param struct the [IonStruct] from which to get the value.
     * @param fieldName the name of the field from which to get the value.
     * @return the String value of the field within the given [IonStruct].
     */
    fun getStringValueOfStructField(struct: IonStruct, fieldName: String): String {
        return (struct[fieldName] as IonString).stringValue()
    }

    /**
     * Return a copy of the given driver's license with updated person Id.
     *
     * @param oldLicense
     * The old driver's license to update.
     * @param personId
     * The PersonId of the driver.
     * @return the updated [DriversLicense].
     */
    @JvmStatic
    fun updatePersonIdDriversLicense(oldLicense: DriversLicense, personId: String): DriversLicense {
        return DriversLicense(
            personId, oldLicense.licenseNumber, oldLicense.licenseType,
            oldLicense.validFromDate, oldLicense.validToDate
        )
    }

    /**
     * Return a copy of the given vehicle registration with updated person Id.
     *
     * @param oldRegistration
     * The old vehicle registration to update.
     * @param personId
     * The PersonId of the driver.
     * @return the updated [VehicleRegistration].
     */
    @JvmStatic
    fun updateOwnerVehicleRegistration(
        oldRegistration: VehicleRegistration,
        personId: String
    ): VehicleRegistration {
        return VehicleRegistration(
            oldRegistration.vin, oldRegistration.licensePlateNumber,
            oldRegistration.state, oldRegistration.city, oldRegistration.pendingPenaltyTicketAmount,
            oldRegistration.validFromDate, oldRegistration.validToDate,
            Owners(Owner(personId), emptyList())
        )
    }
}