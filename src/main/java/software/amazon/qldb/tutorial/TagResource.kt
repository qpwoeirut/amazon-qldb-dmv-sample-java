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

import com.amazonaws.services.qldb.model.*
import org.slf4j.LoggerFactory
import software.amazon.qldb.tutorial.CreateLedger.waitForActive
import java.util.*

/**
 * Tagging and un-tagging resources, including tag on create.
 *
 * This code expects that you have AWS credentials setup per:
 * http://docs.aws.amazon.com/java-sdk/latest/developer-guide/setup-credentials.html
 */
object TagResource {
    val log = LoggerFactory.getLogger(TagResource::class.java)
    const val LEDGER_NAME = Constants.LEDGER_NAME_WITH_TAGS
    val client = CreateLedger.client

    private val CREATE_TAGS = mapOf(
        "IsTest" to "true",
        "Domain" to "Test"
    )
    private val ADD_TAGS = mapOf("Domain" to "Prod")
    private val REMOVE_TAGS = listOf("IsTest")

    /**
     * Create a ledger with the given tags.
     *
     * @param ledgerName
     * Name of the ledger to be created.
     * @param tags
     * The map of key-value pairs to create the ledger with.
     * @return [CreateLedgerResult].
     */
    private fun createWithTags(ledgerName: String, tags: Map<String, String>): CreateLedgerResult {
        log.info("Let's create the ledger with name: {}...", ledgerName)
        val request = CreateLedgerRequest()
            .withName(ledgerName)
            .withTags(tags)
            .withPermissionsMode(PermissionsMode.ALLOW_ALL)
        val result = client.createLedger(request)
        log.info("Success. Ledger state: {}", result.state)
        return result
    }

    /**
     * Add tags to a resource.
     *
     * @param resourceArn
     * The Amazon Resource Name (ARN) of the ledger to which to add the tags.
     * @param tags
     * The map of key-value pairs to add to a ledger.
     */
    private fun tagResource(resourceArn: String, tags: Map<String, String>) {
        log.info("Let's add tags {} for resource with arn: {}...", tags, resourceArn)
        val request = TagResourceRequest()
            .withResourceArn(resourceArn)
            .withTags(tags)
        client.tagResource(request)
        log.info("Successfully added tags.")
    }

    /**
     * Remove one or more tags from the specified QLDB resource.
     *
     * @param resourceArn
     * The Amazon Resource Name (ARN) of the ledger from which to remove the tags.
     * @param tagKeys
     * The list of tag keys to remove.
     */
    private fun untagResource(resourceArn: String, tagKeys: List<String>) {
        log.info("Let's remove tags {} for resource with arn: {}...", tagKeys, resourceArn)
        val request = UntagResourceRequest()
            .withResourceArn(resourceArn)
            .withTagKeys(tagKeys)
        client.untagResource(request)
        log.info("Successfully removed tags.")
    }

    /**
     * Returns all tags for a specified Amazon QLDB resource.
     *
     * @param resourceArn
     * The Amazon Resource Name (ARN) for which to list tags off.
     * @return [ListTagsForResourceResult].
     */
    private fun listTags(resourceArn: String): ListTagsForResourceResult {
        log.info("Let's list the tags for resource with arn: {}...", resourceArn)
        val request = ListTagsForResourceRequest()
            .withResourceArn(resourceArn)
        val result = client.listTagsForResource(request)
        log.info("Success. Tags: {}", result.tags)
        return result
    }

    @Throws(Exception::class)
    @JvmStatic
    fun main(args: Array<String>) {
        try {
            val resourceArn = createWithTags(LEDGER_NAME, CREATE_TAGS).arn
            waitForActive(LEDGER_NAME)
            listTags(resourceArn)
            untagResource(resourceArn, REMOVE_TAGS)
            listTags(resourceArn)
            tagResource(resourceArn, ADD_TAGS)
            listTags(resourceArn)
        } catch (e: Exception) {
            log.error("Unable to tag resources!", e)
            throw e
        }
    }
}