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
import software.amazon.qldb.tutorial.model.streams.RevisionData

/**
 * Represents a vehicle, serializable to (and from) Ion.
 */
class Vehicle @JsonCreator constructor(
    @get:JsonProperty("VIN")
    @param:JsonProperty("VIN") val vin: String,

    @get:JsonProperty("Type")
    @param:JsonProperty("Type") val type: String,

    @get:JsonProperty("Year")
    @param:JsonProperty("Year") val year: Int,

    @get:JsonProperty("Make")
    @param:JsonProperty("Make") val make: String,

    @get:JsonProperty("Model")
    @param:JsonProperty("Model") val model: String,

    @get:JsonProperty("Color")
    @param:JsonProperty("Color") val color: String
) : RevisionData {

    override fun toString(): String {
        return "Vehicle{vin='$vin', type='$type', year=$year, make='$make', model='$model', color='$color'}"
    }
}