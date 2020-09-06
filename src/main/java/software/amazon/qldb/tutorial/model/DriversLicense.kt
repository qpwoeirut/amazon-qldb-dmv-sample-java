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
import software.amazon.qldb.tutorial.model.streams.RevisionData
import java.time.LocalDate

/**
 * Represents a driver's license, serializable to (and from) Ion.
 */
class DriversLicense @JsonCreator constructor(
    @get:JsonProperty("PersonId")
    @param:JsonProperty("PersonId") val personId: String,
    @get:JsonProperty("LicenseNumber")
    @param:JsonProperty("LicenseNumber") val licenseNumber: String,
    @get:JsonProperty("LicenseType")
    @param:JsonProperty("LicenseType") val licenseType: String,
    @field:JsonDeserialize(using = IonLocalDateDeserializer::class) @field:JsonSerialize(using = IonLocalDateSerializer::class) @get:JsonProperty(
        "ValidFromDate"
    )
    @param:JsonProperty("ValidFromDate") val validFromDate: LocalDate,
    @field:JsonDeserialize(using = IonLocalDateDeserializer::class) @field:JsonSerialize(using = IonLocalDateSerializer::class) @get:JsonProperty(
        "ValidToDate"
    )
    @param:JsonProperty("ValidToDate") val validToDate: LocalDate
) : RevisionData {

    override fun toString(): String {
        return "DriversLicense{" +
                "personId='" + personId + '\'' +
                ", licenseNumber='" + licenseNumber + '\'' +
                ", licenseType='" + licenseType + '\'' +
                ", validFromDate=" + validFromDate +
                ", validToDate=" + validToDate +
                '}'
    }
}