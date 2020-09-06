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
package software.amazon.qldb.tutorial.qldb

import com.amazon.ion.IonReader
import com.amazon.ion.system.IonSystemBuilder
import java.util.*

/**
 * A Java representation of the [Proof] object.
 * Returned from the [com.amazonaws.services.qldb.AmazonQLDB.getRevision] api.
 */
class Proof(val internalHashes: List<ByteArray>) {

    companion object {
        private val SYSTEM = IonSystemBuilder.standard().build()

        /**
         * Decodes a [Proof] from an ion text String. This ion text is returned in
         * a [GetRevisionResult.getProof]
         *
         * @param ionText
         * The ion text representing a [Proof] object.
         * @return [JournalBlock] parsed from the ion text.
         * @throws IllegalStateException if failed to parse the [Proof] object from the given ion text.
         */
        @JvmStatic
        fun fromBlob(ionText: String?): Proof {
            return try {
                val reader: IonReader = SYSTEM.newReader(ionText)
                val list: MutableList<ByteArray> = ArrayList()
                reader.next()
                reader.stepIn()
                while (reader.next() != null) {
                    list.add(reader.newBytes())
                }
                Proof(list)
            } catch (e: Exception) {
                throw IllegalStateException("Failed to parse a Proof from byte array")
            }
        }
    }
}