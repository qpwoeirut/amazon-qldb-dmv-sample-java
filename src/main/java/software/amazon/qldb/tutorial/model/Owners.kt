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

import com.fasterxml.jackson.annotation.JsonProperty

/**
 * Represents a set of owners for a given vehicle, serializable to (and from) Ion.
 */
class Owners(
    @get:JsonProperty("PrimaryOwner")
    @param:JsonProperty("PrimaryOwner") val primaryOwner: Owner,
    @get:JsonProperty("SecondaryOwners")
    @param:JsonProperty("SecondaryOwners") val secondaryOwners: List<Owner>
) {

    override fun toString(): String {
        return "Owners{primaryOwner=$primaryOwner, secondaryOwners=$secondaryOwners}"
    }
}