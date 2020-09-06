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
import software.amazon.qldb.tutorial.model.SampleData.getDocumentId
import software.amazon.qldb.tutorial.model.streams.RevisionData
import java.math.BigDecimal
import java.time.LocalDate

/**
 * Represents a vehicle registration, serializable to (and from) Ion.
 */
class VehicleRegistration @JsonCreator constructor(
    @get:JsonProperty("VIN")
    @param:JsonProperty("VIN") val vin: String,
    @get:JsonProperty("LicensePlateNumber")
    @param:JsonProperty("LicensePlateNumber") val licensePlateNumber: String,
    @get:JsonProperty("State")
    @param:JsonProperty("State") val state: String,
    @get:JsonProperty("City")
    @param:JsonProperty("City") val city: String,
    @get:JsonProperty("PendingPenaltyTicketAmount")
    @param:JsonProperty("PendingPenaltyTicketAmount") val pendingPenaltyTicketAmount: BigDecimal,
    @get:JsonProperty("ValidFromDate")
    @get:JsonSerialize(using = IonLocalDateSerializer::class)
    @get:JsonDeserialize(using = IonLocalDateDeserializer::class)
    @param:JsonProperty("ValidFromDate") val validFromDate: LocalDate,
    @get:JsonProperty("ValidToDate")
    @get:JsonSerialize(using = IonLocalDateSerializer::class)
    @get:JsonDeserialize(using = IonLocalDateDeserializer::class)
    @param:JsonProperty("ValidToDate") val validToDate: LocalDate,
    @get:JsonProperty("Owners")
    @param:JsonProperty("Owners") val owners: Owners
) : RevisionData {

    override fun toString(): String {
        return """VehicleRegistration{
                |vin='$vin', 
                |licensePlateNumber='$licensePlateNumber', 
                |state='$state', 
                |city='$city', 
                |pendingPenaltyTicketAmount=$pendingPenaltyTicketAmount, 
                |validFromDate=$validFromDate, 
                |validToDate=$validToDate, 
                |owners=$owners
                |}""".trimMargin()
    }

    companion object {
        /**
         * Returns the unique document ID of a vehicle given a specific VIN.
         *
         * @param txn
         * A transaction executor object.
         * @param vin
         * The VIN of a vehicle.
         * @return the unique document ID of the specified vehicle.
         */
        @JvmStatic
        fun getDocumentIdByVin(txn: TransactionExecutor, vin: String): String {
            return getDocumentId(txn, Constants.VEHICLE_REGISTRATION_TABLE_NAME, "VIN", vin)
        }
    }
}