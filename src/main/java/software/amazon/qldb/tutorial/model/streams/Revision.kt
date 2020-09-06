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
package software.amazon.qldb.tutorial.model.streams

import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.DeserializationContext
import com.fasterxml.jackson.databind.JsonDeserializer
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import software.amazon.qldb.tutorial.model.DriversLicense
import software.amazon.qldb.tutorial.model.Person
import software.amazon.qldb.tutorial.model.Vehicle
import software.amazon.qldb.tutorial.model.VehicleRegistration
import software.amazon.qldb.tutorial.qldb.BlockAddress
import software.amazon.qldb.tutorial.qldb.RevisionMetadata
import java.io.IOException

/**
 * Represents a Revision including both user data and metadata.
 */
class Revision @JsonCreator constructor(
    @param:JsonProperty("blockAddress") val blockAddress: BlockAddress,
    @param:JsonProperty("metadata") val metadata: RevisionMetadata,
    @param:JsonProperty("hash") val hash: ByteArray,
    @field:JsonDeserialize(using = RevisionDataDeserializer::class) @param:JsonProperty("data") val data: RevisionData
) {

    override fun equals(other: Any?): Boolean {
        if (this === other) {
            return true
        }
        if (other == null || javaClass != other.javaClass) {
            return false
        }
        val revision = other as Revision
        if (blockAddress != revision.blockAddress) {
            return false
        }
        if (metadata != revision.metadata) {
            return false
        }
        return if (!hash.contentEquals(revision.hash)) {
            false
        } else data == revision.data
    }

    override fun hashCode(): Int {
        var result = blockAddress.hashCode()
        result = 31 * result + (metadata.hashCode())
        result = 31 * result + hash.contentHashCode()
        result = 31 * result + (data.hashCode())
        return result
    }

    /**
     * Converts a [Revision] object to string.
     *
     * @return the string representation of the [Revision] object.
     */
    override fun toString(): String {
        return """Revision{
                |blockAddress=$blockAddress, 
                |metadata=$metadata, 
                |hash=${hash.contentToString()}, 
                |data=$data
                |}""".trimMargin()
    }

    class RevisionDataDeserializer : JsonDeserializer<RevisionData>() {
        @Throws(IOException::class)
        override fun deserialize(jp: JsonParser, dc: DeserializationContext): RevisionData {
            val tableInfo = jp.parsingContext.parent.currentValue as TableInfo
            val revisionData: RevisionData
            revisionData = when (tableInfo.tableName) {
                "VehicleRegistration" -> jp.readValueAs(
                    VehicleRegistration::class.java
                )
                "Person" -> jp.readValueAs(Person::class.java)
                "DriversLicense" -> jp.readValueAs(DriversLicense::class.java)
                "Vehicle" -> jp.readValueAs(Vehicle::class.java)
                else -> throw RuntimeException("Unsupported table " + tableInfo.tableName)
            }
            return revisionData
        }
    }
}