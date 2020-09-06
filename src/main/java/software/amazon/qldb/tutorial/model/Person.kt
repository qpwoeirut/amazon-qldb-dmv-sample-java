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

import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import com.fasterxml.jackson.databind.annotation.JsonSerialize
import software.amazon.qldb.TransactionExecutor
import software.amazon.qldb.tutorial.Constants
import software.amazon.qldb.tutorial.model.streams.RevisionData
import java.time.LocalDate

/**
 * Represents a person, serializable to (and from) Ion.
 */
class Person @JsonCreator constructor(
    @get:JsonProperty("FirstName")
    @param:JsonProperty("FirstName") val firstName: String,

    @get:JsonProperty("LastName")
    @param:JsonProperty("LastName") val lastName: String,

    @field:JsonDeserialize(using = IonLocalDateDeserializer::class)
    @field:JsonSerialize(using = IonLocalDateSerializer::class)
    @get:JsonProperty("DOB")
    @param:JsonProperty("DOB") val dob: LocalDate,

    @get:JsonProperty("GovId")
    @param:JsonProperty("GovId") val govId: String,

    @get:JsonProperty("GovIdType")
    @param:JsonProperty("GovIdType") val govIdType: String,

    @get:JsonProperty("Address")
    @param:JsonProperty("Address") val address: String
) : RevisionData {

    override fun toString(): String {
        return "Person{firstName='$firstName', lastName='$lastName', dob=$dob, govId='$govId', govIdType='$govIdType', address='$address'}"
    }

    companion object {
        /**
         * This returns the unique document ID given a specific government ID.
         *
         * @param txn
         * A transaction executor object.
         * @param govId
         * The government ID of a driver.
         * @return the unique document ID.
         */
        @JvmStatic
        fun getDocumentIdByGovId(txn: TransactionExecutor, govId: String): String {
            return SampleData.getDocumentId(txn, Constants.PERSON_TABLE_NAME, "GovId", govId)
        }
    }
}